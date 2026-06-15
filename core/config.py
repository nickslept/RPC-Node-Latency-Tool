"""Configuration loading and validation for the RPC Node Latency Comparison Tool.

This module is the single source of truth for every tunable parameter and -- the
load-bearing part -- for the node -> column mapping. The ordered ``[[nodes]]``
list in ``config.toml`` assigns node_1, node_2, ... strictly by position. Both
the pipeline (which writes those columns) and the analysis stage (which labels
charts) read that same mapping from here, which is what keeps a statement like
"node_2 is always PublicNode" true across the entire tool and across runs.

Secrets never live in ``config.toml``. URL templates reference environment
variables as ``${VAR}``; the real values come from a ``.env`` file (loaded here)
or the ambient environment, and are interpolated at load time. ``config.toml``
is therefore safe to commit; ``.env`` is not. A template with no ``${...}`` (for
a keyless provider such as PublicNode) passes through unchanged.

Loading fails fast and loud: a missing secret, the wrong number of nodes, or an
out-of-range parameter raises :class:`ConfigError` with a message that says
exactly what to fix, rather than producing a subtly-wrong run.
"""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass

from dotenv import dotenv_values

from .schema import NUM_NODES, arrival_column


class ConfigError(Exception):
    """Raised for any invalid or incomplete configuration."""


# --- Typed config objects --------------------------------------------------
# Frozen so a loaded config is immutable for the life of a run.


@dataclass(frozen=True)
class NodeConfig:
    """One RPC provider, bound to a positional column slot.

    ``index`` is 1-based and ``column`` is the schema column it owns; together
    they are the canonical provider -> column mapping. ``url`` is the fully
    resolved websocket endpoint (secrets already interpolated).
    """

    index: int
    name: str
    column: str
    url: str


@dataclass(frozen=True)
class CompletionConfig:
    min_nodes_required: int
    timeout_seconds: float
    scanner_interval_seconds: float


@dataclass(frozen=True)
class WriterConfig:
    batch_size: int


@dataclass(frozen=True)
class PreflightConfig:
    ack_timeout_seconds: float


@dataclass(frozen=True)
class ConnectionConfig:
    ping_interval_seconds: float
    ping_timeout_seconds: float
    stop_on_disconnect: bool


@dataclass(frozen=True)
class FilterConfig:
    contracts: tuple[str, ...]
    order_filled_topic: str


@dataclass(frozen=True)
class Config:
    nodes: tuple[NodeConfig, ...]
    completion: CompletionConfig
    writer: WriterConfig
    preflight: PreflightConfig
    connection: ConnectionConfig
    filter: FilterConfig


# --- Defaults for the numeric tunables -------------------------------------
# These have safe, agreed defaults, so their config sections are optional. The
# things with no safe default -- the node list and the contract filter -- are
# required and raise if absent.

_DEFAULTS: dict[str, dict[str, object]] = {
    "completion": {
        "min_nodes_required": 5,
        "timeout_seconds": 10.0,
        "scanner_interval_seconds": 10.0,
    },
    "writer": {"batch_size": 1000},
    "preflight": {"ack_timeout_seconds": 10.0},
    "connection": {
        "ping_interval_seconds": 5.0,
        "ping_timeout_seconds": 5.0,
        "stop_on_disconnect": True,
    },
}


# --- Secret interpolation --------------------------------------------------

_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _interpolate(template: str, env: dict[str, str], *, node_name: str) -> str:
    """Substitute ``${VAR}`` references in a URL template from ``env``.

    A template with no references is returned unchanged (the keyless-provider
    case). A reference to an unset variable raises :class:`ConfigError` naming
    both the node and the variable, since that is invariably a missing secret.
    """

    def repl(match: re.Match[str]) -> str:
        var = match.group(1)
        if var not in env or env[var] in (None, ""):
            raise ConfigError(
                f"node '{node_name}': environment variable '{var}' referenced in "
                f"its url_template is not set. Add it to your .env (or the "
                f"environment) -- see .env.example."
            )
        return env[var]

    return _VAR_PATTERN.sub(repl, template)


# --- Small validation helpers ----------------------------------------------


def _require(table: dict, key: str, *, where: str) -> object:
    if key not in table:
        raise ConfigError(f"missing required key '{key}' in [{where}]")
    return table[key]


def _positive(value: object, *, name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        raise ConfigError(f"'{name}' must be a positive number, got {value!r}")
    return value


# --- Section parsers -------------------------------------------------------


def _parse_completion(raw: dict) -> CompletionConfig:
    merged = {**_DEFAULTS["completion"], **raw}
    min_nodes = merged["min_nodes_required"]
    if not isinstance(min_nodes, int) or isinstance(min_nodes, bool) or not (
        1 <= min_nodes <= NUM_NODES
    ):
        raise ConfigError(
            f"completion.min_nodes_required must be an integer in 1..{NUM_NODES}, "
            f"got {min_nodes!r}"
        )
    return CompletionConfig(
        min_nodes_required=min_nodes,
        timeout_seconds=_positive(
            merged["timeout_seconds"], name="completion.timeout_seconds"
        ),
        scanner_interval_seconds=_positive(
            merged["scanner_interval_seconds"],
            name="completion.scanner_interval_seconds",
        ),
    )


def _parse_writer(raw: dict) -> WriterConfig:
    merged = {**_DEFAULTS["writer"], **raw}
    batch = merged["batch_size"]
    if not isinstance(batch, int) or isinstance(batch, bool) or batch <= 0:
        raise ConfigError(
            f"writer.batch_size must be a positive integer, got {batch!r}"
        )
    return WriterConfig(batch_size=batch)


def _parse_preflight(raw: dict) -> PreflightConfig:
    merged = {**_DEFAULTS["preflight"], **raw}
    return PreflightConfig(
        ack_timeout_seconds=_positive(
            merged["ack_timeout_seconds"], name="preflight.ack_timeout_seconds"
        )
    )


def _parse_connection(raw: dict) -> ConnectionConfig:
    merged = {**_DEFAULTS["connection"], **raw}
    stop = merged["stop_on_disconnect"]
    if not isinstance(stop, bool):
        raise ConfigError(
            f"connection.stop_on_disconnect must be true/false, got {stop!r}"
        )
    return ConnectionConfig(
        ping_interval_seconds=_positive(
            merged["ping_interval_seconds"], name="connection.ping_interval_seconds"
        ),
        ping_timeout_seconds=_positive(
            merged["ping_timeout_seconds"], name="connection.ping_timeout_seconds"
        ),
        stop_on_disconnect=stop,
    )


def _parse_filter(raw: dict) -> FilterConfig:
    contracts = _require(raw, "contracts", where="filter")
    topic = _require(raw, "order_filled_topic", where="filter")
    if not isinstance(contracts, list) or not contracts:
        raise ConfigError("filter.contracts must be a non-empty list of addresses")
    if not all(isinstance(c, str) and c for c in contracts):
        raise ConfigError("filter.contracts must contain only non-empty strings")
    if not isinstance(topic, str) or not topic:
        raise ConfigError("filter.order_filled_topic must be a non-empty string")
    return FilterConfig(contracts=tuple(contracts), order_filled_topic=topic)


def _parse_nodes(raw_nodes: object, env: dict[str, str]) -> tuple[NodeConfig, ...]:
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise ConfigError(
            "config must define a [[nodes]] array (the ordered provider list)"
        )
    if len(raw_nodes) != NUM_NODES:
        raise ConfigError(
            f"expected exactly {NUM_NODES} [[nodes]] entries (the schema is fixed "
            f"at {NUM_NODES} node columns), found {len(raw_nodes)}"
        )

    nodes: list[NodeConfig] = []
    seen_names: set[str] = set()
    for position, entry in enumerate(raw_nodes, start=1):
        if not isinstance(entry, dict):
            raise ConfigError(f"[[nodes]] entry #{position} is malformed")
        name = _require(entry, "name", where=f"nodes #{position}")
        template = _require(entry, "url_template", where=f"nodes #{position}")
        if not isinstance(name, str) or not name:
            raise ConfigError(f"[[nodes]] entry #{position}: 'name' must be a string")
        if name in seen_names:
            raise ConfigError(f"duplicate node name '{name}' in [[nodes]]")
        seen_names.add(name)
        if not isinstance(template, str) or not template:
            raise ConfigError(
                f"node '{name}': 'url_template' must be a non-empty string"
            )
        url = _interpolate(template, env, node_name=name)
        nodes.append(
            NodeConfig(
                index=position,
                name=name,
                column=arrival_column(position),  # position IS the column mapping
                url=url,
            )
        )
    return tuple(nodes)


# --- Public entry point ----------------------------------------------------


def load_config(config_path: str, *, env_path: str | None = ".env") -> Config:
    """Load and fully validate configuration.

    ``config_path`` (TOML) is required and must exist. ``env_path`` is the
    secrets file; if it is None or simply absent, loading proceeds using only
    the ambient environment (useful in CI or when secrets are exported
    directly). Ambient environment variables take precedence over the .env file,
    so an exported value can override the file for a single run.

    Raises :class:`ConfigError` on any problem, with a message naming the fix.
    """
    if not os.path.exists(config_path):
        raise ConfigError(f"config file not found: {config_path}")

    file_env = (
        dotenv_values(env_path) if env_path and os.path.exists(env_path) else {}
    )
    # Ambient env overrides the .env file (lets you override a single secret
    # inline without editing the file). None values from dotenv are dropped.
    env: dict[str, str] = {
        k: v for k, v in {**file_env, **os.environ}.items() if v is not None
    }

    try:
        with open(config_path, "rb") as fh:
            data = tomllib.load(fh)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{config_path} is not valid TOML: {exc}") from exc

    return Config(
        nodes=_parse_nodes(data.get("nodes"), env),
        completion=_parse_completion(data.get("completion", {})),
        writer=_parse_writer(data.get("writer", {})),
        preflight=_parse_preflight(data.get("preflight", {})),
        connection=_parse_connection(data.get("connection", {})),
        filter=_parse_filter(data.get("filter", {})),
    )