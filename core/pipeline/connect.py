"""WebSocket connect + subscribe, and the synchronized-start pre-flight gate.

This module owns the first network contact. Its job is to bring all five nodes
to the "connected and subscribed" state and hand back the live connections --
and *only* that. It does not read trades; the listeners (Stage 3) do, and they
do not begin until the runner opens the gate. This separation is what enforces
the fairness requirement from the design: no node starts being recorded until
every node is subscribed, so a fast node cannot bank an early advantage.

Pre-flight policy (decided earlier):
  * One attempt per node, no retries. If it doesn't work, the user re-runs.
  * Each attempt is bounded by a single ack timeout (connect + subscribe + ack).
  * All five attempt concurrently, so total pre-flight time is bounded by the
    slowest node, not the sum.
  * Strict: if ANY node fails, abort the whole run -- a five-column schema with
    a permanently-null column is worse than no run. But let every node finish
    and report its status first, so one launch diagnoses all failures at once.
  * Cleanup: on abort, the connections that DID succeed are closed, never leaked.

Only the subscription ack is checked here. Whether log notifications actually
flow is a Stage 3 concern. A node may begin sending logs the instant it acks;
any such pre-gate message is harmless and is discarded (here, by being skipped
while waiting for the ack; later, by the listener's not-yet-recording flag).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Awaitable, Callable, Protocol

from .config import Config, ConnectionConfig, FilterConfig, NodeConfig


# --- Connection abstraction (so tests can inject a fake transport) ---------


class _WebSocketLike(Protocol):
    """The minimal surface connect.py needs from a websocket connection."""

    async def send(self, data: str) -> None: ...
    async def recv(self) -> str | bytes: ...
    async def close(self) -> None: ...


# A connector opens one connection. Real use binds this to the websockets
# library; tests pass a fake. It returns an awaitable resolving to a connection.
Connector = Callable[..., Awaitable[_WebSocketLike]]


def _default_connector(url: str, *, ping_interval: float, ping_timeout: float):
    """Real connector, bound to the websockets asyncio client.

    The keepalive ping settings are applied here, at construction, because the
    connection object is created here. Pings are control frames and never
    surface from ``recv()``, so they never touch trade timestamping.
    """
    from websockets.asyncio.client import connect

    return connect(url, ping_interval=ping_interval, ping_timeout=ping_timeout)


# --- Result and error types ------------------------------------------------


@dataclass
class NodeConnection:
    """A live, subscribed connection, ready to be handed to a listener."""

    node: NodeConfig
    websocket: _WebSocketLike
    subscription_id: str


class ConnectError(Exception):
    """A single node failed to connect/subscribe within its ack timeout."""

    def __init__(self, node: NodeConfig, reason: str):
        self.node = node
        self.reason = reason
        super().__init__(f"node_{node.index} ({node.name}): {reason}")


class PreflightError(Exception):
    """One or more nodes failed pre-flight; the run is aborted."""

    def __init__(self, failures: list[ConnectError]):
        self.failures = failures
        summary = "; ".join(
            f"node_{f.node.index} {f.node.name}: {f.reason}" for f in failures
        )
        super().__init__(
            f"pre-flight failed: {len(failures)} node(s) did not subscribe ({summary})"
        )


# --- Pure helpers (no I/O; unit-tested directly) ---------------------------


def build_subscribe_request(filter_cfg: FilterConfig, request_id: int = 1) -> dict:
    """Build the eth_subscribe('logs', ...) JSON-RPC request.

    A single subscription covers both exchange contracts (address array) and the
    OrderFilled event (topics[0]); this keeps each node to one recv() stream,
    which the lean-listener design depends on.
    """
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "eth_subscribe",
        "params": [
            "logs",
            {
                "address": list(filter_cfg.contracts),
                "topics": [filter_cfg.order_filled_topic],
            },
        ],
    }


def classify_message(raw: str | bytes, request_id: int) -> tuple[str, str | None]:
    """Classify a message received while awaiting the subscription ack.

    Returns one of:
      ("ack", subscription_id)  -- the response to our request; success
      ("error", reason)         -- an RPC error response, or a malformed message
      ("other", None)           -- some other message (e.g. an early log
                                   notification); skip it and keep waiting

    Pure: no node context, so the caller turns ("error", reason) into a
    ConnectError. The ack is matched by request id, never by position, so an
    early notification arriving before the ack does not get mistaken for it.
    """
    try:
        msg = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return ("error", "non-JSON response to subscribe request")
    if not isinstance(msg, dict):
        return ("error", "unexpected subscribe response shape")

    if msg.get("id") == request_id:
        if "error" in msg:
            err = msg["error"]
            reason = err.get("message", str(err)) if isinstance(err, dict) else str(err)
            return ("error", f"subscribe rejected: {reason}")
        result = msg.get("result")
        if isinstance(result, str):
            return ("ack", result)
        return ("error", f"malformed subscribe ack (no string result): {msg!r}")

    # Not our response id -> a notification or unrelated control message.
    return ("other", None)


def format_result(node: NodeConfig, result: NodeConnection | ConnectError) -> str:
    """The one-line pre-flight status string for a node (printed in node order)."""
    if isinstance(result, NodeConnection):
        return f"[SUBSCRIBED] node_{node.index} ({node.name})"
    reason = result.reason if isinstance(result, ConnectError) else repr(result)
    return f"[ERROR] node_{node.index} ({node.name}) FAILED: {reason}"


# --- Per-node connect (I/O) ------------------------------------------------


async def _safe_close(ws: _WebSocketLike) -> None:
    try:
        await ws.close()
    except Exception:
        pass  # best-effort; we are already tearing down or aborting


async def _await_ack(ws: _WebSocketLike, node: NodeConfig, request_id: int) -> str:
    """Read messages until the subscription ack arrives; raise on RPC error.

    Bounded externally by the ack timeout (see connect_node). Skipping non-ack
    messages is safe: a pre-ack log notification is pre-gate data we discard
    anyway.
    """
    while True:
        raw = await ws.recv()
        kind, payload = classify_message(raw, request_id)
        if kind == "ack":
            assert payload is not None
            return payload
        if kind == "error":
            raise ConnectError(node, payload or "subscribe error")
        # kind == "other": skip and keep waiting


async def _connect_and_subscribe(
    node: NodeConfig,
    filter_cfg: FilterConfig,
    conn_cfg: ConnectionConfig,
    connector: Connector,
) -> NodeConnection:
    ws = await connector(
        node.url,
        ping_interval=conn_cfg.ping_interval_seconds,
        ping_timeout=conn_cfg.ping_timeout_seconds,
    )
    # From here on, any failure (including cancellation when the ack timeout
    # fires) must close the socket so we never leak a half-open connection.
    try:
        request = build_subscribe_request(filter_cfg)
        await ws.send(json.dumps(request))
        sub_id = await _await_ack(ws, node, request["id"])
        return NodeConnection(node=node, websocket=ws, subscription_id=sub_id)
    except BaseException:
        await _safe_close(ws)
        raise


async def connect_node(
    node: NodeConfig,
    filter_cfg: FilterConfig,
    conn_cfg: ConnectionConfig,
    ack_timeout: float,
    *,
    connector: Connector = _default_connector,
) -> NodeConnection:
    """Connect and subscribe one node within ``ack_timeout`` seconds.

    Always raises ConnectError on any failure (timeout, connection refused, bad
    key, rejected filter), with a short human-readable reason -- never a raw
    library exception -- so the pre-flight output is uniform.
    """
    try:
        return await asyncio.wait_for(
            _connect_and_subscribe(node, filter_cfg, conn_cfg, connector),
            timeout=ack_timeout,
        )
    except asyncio.TimeoutError:
        raise ConnectError(node, "timeout") from None
    except ConnectError:
        raise
    except Exception as exc:  # connection refused, TLS error, bad handshake, ...
        reason = f"{type(exc).__name__}: {exc}".strip().rstrip(":")
        raise ConnectError(node, reason) from exc


# --- The gate --------------------------------------------------------------


async def open_all(
    config: Config,
    *,
    connector: Connector = _default_connector,
) -> list[NodeConnection]:
    """Concurrently connect+subscribe all nodes; all-ack-or-abort.

    Prints one status line per node in node order. On success returns the live
    connections (caller owns closing them). On any failure, closes the
    successful connections and raises PreflightError.
    """
    results = await asyncio.gather(
        *(
            connect_node(
                node,
                config.filter,
                config.connection,
                config.preflight.ack_timeout_seconds,
                connector=connector,
            )
            for node in config.nodes
        ),
        return_exceptions=True,  # do NOT cancel siblings; let every node report
    )

    connections: list[NodeConnection] = []
    failures: list[ConnectError] = []
    for node, result in zip(config.nodes, results):
        if isinstance(result, NodeConnection):
            print(format_result(node, result))
            connections.append(result)
        else:
            err = result if isinstance(result, ConnectError) else ConnectError(
                node, f"{type(result).__name__}: {result}"
            )
            print(format_result(node, err))
            failures.append(err)

    if failures:
        await asyncio.gather(
            *(_safe_close(c.websocket) for c in connections),
            return_exceptions=True,
        )
        raise PreflightError(failures)

    return connections


async def close_all(connections: list[NodeConnection]) -> None:
    """Close every connection (used on shutdown and on post-success abort)."""
    await asyncio.gather(
        *(_safe_close(c.websocket) for c in connections),
        return_exceptions=True,
    )