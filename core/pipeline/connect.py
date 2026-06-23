from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

from websockets.asyncio.client import ClientConnection, connect

from ..config import Config, ConnectionConfig, FilterConfig, NodeConfig


# --- Connection/Exception Definitions ---


@dataclass
class NodeConnection:
    """A successful connection to a node."""

    node: NodeConfig
    websocket: ClientConnection
    subscription_id: str


class ConnectError(Exception):
    """A node failed to connect."""

    def __init__(self, node: NodeConfig, reason: str):
        self.node = node
        self.reason = reason
        super().__init__(f"node_{node.index} ({node.name}): {reason}")


class PreflightError(Exception):
    """One or more nodes failed before data collection could begin; the run was aborted."""

    def __init__(self, failures: list[ConnectError]):
        self.failures = failures
        summary = "; ".join(
            f"node_{f.node.index} {f.node.name}: {f.reason}" for f in failures
        )
        super().__init__(
            f"Pre-flight failed: {len(failures)} node(s) did not subscribe ({summary})"
        )


# --- Helpers ---


def _build_sub_request(filter_cfg: FilterConfig, request_id: int = 1) -> dict:
    """Builds the eth_subscribe() JSON-RPC request.

    Returns: A dictionary representing the JSON-RPC request.
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


def _classify_message(raw: str | bytes, request_id: int) -> tuple[str, str | None]:
    """Classifies a message received when waiting for the subscription ack.

    Returns one of the following:
        ("ack", subscription_id)     the response to our request; success
        ("error", reason)            an RPC error response, or a malformed message
        ("other", None)              some other message
    """
    try:
        msg = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return ("error", "raw isn't a valid JSON")
    if not isinstance(msg, dict):
        return ("error", "dict wasn't received")
    if msg.get("id") == request_id:
        if "error" in msg:
            err = msg["error"]
            reason = err.get("message", str(err)) if isinstance(err, dict) else str(err)
            return ("error", f"subscription rejected: {reason}")
        result = msg.get("result")
        if isinstance(result, str):
            return ("ack", result)
        return ("error", f"missing 'result' string: {msg!r}")
    return ("other", None)


def _format_result(node: NodeConfig, result: NodeConnection | ConnectError) -> str:
    """Formats the result of a node connection attempt, and returns a string to be printed into the console."""
    if isinstance(result, NodeConnection):
        return f"[SUBSCRIBED] node_{node.index} ({node.name})"
    reason = result.reason if isinstance(result, ConnectError) else repr(result)
    return f"[ERROR] node_{node.index} ({node.name}) FAILED: {reason}"


async def _safe_close(ws: ClientConnection) -> None:
    try:
        await ws.close()
    except Exception:
        pass


async def _await_ack(
        ws: ClientConnection, 
        node: NodeConfig, 
        request_id: int,
    ) -> str:
    """Reads messages until the subscription ack arrives. Raises a ConnectError if an error message is received.

    Externally bounded by the ack timeout (see connect_node()).
    """
    while True:
        raw = await ws.recv()
        kind, payload = _classify_message(raw, request_id)
        if kind == "ack":
            assert payload is not None
            return payload
        if kind == "error":
            raise ConnectError(node, payload or "subscription error")


async def _connect_and_subscribe(
    node: NodeConfig,
    filter_cfg: FilterConfig,
    conn_cfg: ConnectionConfig,
) -> NodeConnection:
    """Connects to a node and subscribes to a filter.
    
    Returns a NodeConnection object on success, or raises a ConnectError (in _await_ack()) on failure.
    Also ensures that the connection is closed on ANY exception.
    """
    ws = await connect(
        node.url,
        ping_interval=conn_cfg.ping_interval_seconds,
        ping_timeout=conn_cfg.ping_timeout_seconds,
    )
    try:
        request = _build_sub_request(filter_cfg)
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
) -> NodeConnection:
    """Connect and subscribe one node within ``ack_timeout`` seconds.

    Always raises ConnectError on any failure (timeout, connection refused, bad
    key, rejected filter), with a short human-readable reason -- never a raw
    library exception -- so the pre-flight output is uniform.
    """
    try:
        return await asyncio.wait_for(
            _connect_and_subscribe(node, filter_cfg, conn_cfg),
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


async def open_all(config: Config) -> list[NodeConnection]:
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
            )
            for node in config.nodes
        ),
        return_exceptions=True,  # do NOT cancel siblings; let every node report
    )

    connections: list[NodeConnection] = []
    failures: list[ConnectError] = []
    for node, result in zip(config.nodes, results):
        if isinstance(result, NodeConnection):
            print(_format_result(node, result))
            connections.append(result)
        else:
            err = result if isinstance(result, ConnectError) else ConnectError(
                node, f"{type(result).__name__}: {result}"
            )
            print(_format_result(node, err))
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