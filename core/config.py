from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass

from dotenv import dotenv_values

from .schema import NUM_NODES, arrival_column


class ConfigError(Exception):
    """Raised for any invalid or incomplete configuration."""


# --- Immutable Config Dataclasses ---


@dataclass(frozen=True)
class NodeConfig:
    """A single RPC node provider's configuration.

    ``index`` goes from 1 to NUM_NODES (inclusive).
    ``name`` is the name of the node provider.
    ``column`` is the name of the column in the schema (e.g. "node_1_arrival_ns"). 
    ``url`` is the full websocket url (WITH the API key if the provider requires one).
    """

    index: int
    name: str
    column: str
    url: str


@dataclass(frozen=True)
class CompletionConfig:
    """The configuration for when a single row is considered complete.
    
    ``min_nodes_required`` is the minimum number of nodes that must have reported an arrival time for the transaction hash.
    ``timeout_seconds`` is the maximum time to wait for a row to be complete.
    ``scanner_interval_seconds`` is the interval at which the scanner checks for the conditions above.
    """
    min_nodes_required: int
    timeout_seconds: float
    scanner_interval_seconds: float


@dataclass(frozen=True)
class WriterConfig:
    """
    The configuration for the Writer.

    ``batch_size`` is the number of finalized rows the writer accumulates before writing a batch to the output parquet file.
    """
    batch_size: int


@dataclass(frozen=True)
class PreflightConfig:
    """
    The configuration for the checks done before data collection truly begins.

    ``ack_timeout_seconds`` is the maximum time a single node can take to send a subscription acknowledgment before the run is aborted.
    """
    ack_timeout_seconds: float


@dataclass(frozen=True)
class ConnectionConfig:
    """
    The configuration for maintaining and closing connections to the RPC nodes.

    ``ping_interval_seconds`` is the interval at which pings are sent to the nodes.
    ``ping_timeout_seconds`` is the maximum time to wait for a ping response before considering the connection dead.
    ``stop_on_disconnect`` is a boolean indicating whether to stop the run if any node disconnects.
    """
    ping_interval_seconds: float
    ping_timeout_seconds: float
    stop_on_disconnect: bool


@dataclass(frozen=True)
class FilterConfig:
    """
    The configuration for on-chain event filtering.

    ``contracts`` is a tuple of contract addresses (Binary and NegRisk).
    ``order_filled_topic`` is the topic for OrderFilled events.
    """
    contracts: tuple[str, ...]
    order_filled_topic: str


@dataclass(frozen=True)
class Config:
    """
    The master configuration.

    ``nodes`` is a tuple containing the configurations for each node.
    ``completion`` is the config for when a single row is considered complete.
    ``writer`` is the config for the writer.
    ``preflight`` is the config for the checks done before data collection truly begins.
    ``connection`` is the config for maintaining and closing connections to the RPC nodes.
    ``filter`` is the config for on-chain event filtering.
    """
    nodes: tuple[NodeConfig, ...]
    completion: CompletionConfig
    writer: WriterConfig
    preflight: PreflightConfig
    connection: ConnectionConfig
    filter: FilterConfig


# --- Parser Helpers ---


_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")

def _interpolate(template: str, env: dict[str, str], *, node_name: str) -> str:
    """Substitutes ``${VAR}`` references in a URL template from ``env``. 
    
    ``template`` is the URL string containing ``${VAR}`` placeholders.
    ``env`` is the mapping of environment variable names to their values.
    ``node_name`` is the node the template belongs to, used only in error messages.

    Returns: The interpolated URL string. In the case of no API keys needed, the template will be returned as-is.
    """

    def repl(match: re.Match[str]) -> str:
        var = match.group(1)
        if var not in env or env[var] in (None, ""):
            raise ConfigError(
                f"Node '{node_name}' references environment variable '{var}', which is not set. "
                f"Add it to your .env file. Note: other environment variables may be missing as well, but only the first missing variable will be reported."
            )
        return env[var]

    return _VAR_PATTERN.sub(repl, template)


def _require(table: dict, key: str, *, where: str) -> object:
    """
    Ensures that the given key is present in the table.
    """
    if key not in table:
        raise ConfigError(f"missing required key '{key}' in [{where}]")
    return table[key]


def _positive(value: object, *, name: str) -> float:
    """
    Ensures that the given value is a positive number.
    """
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        raise ConfigError(f"'{name}' must be a positive number, got {value!r}")
    return value


# --- config.toml Table Parsers ---


def _parse_completion(raw: dict) -> CompletionConfig:
    min_nodes = _require(raw, "min_nodes_required", where="completion")
    if not isinstance(min_nodes, int) or isinstance(min_nodes, bool) or not (
        1 <= min_nodes <= NUM_NODES
    ):
        raise ConfigError(
            f"completion.min_nodes_required must be an integer between 1 and {NUM_NODES}, "
            f"got {min_nodes!r}"
        )
    return CompletionConfig(
        min_nodes_required=min_nodes,
        timeout_seconds=_positive(
            _require(raw, "timeout_seconds", where="completion"),
            name="completion.timeout_seconds",
        ),
        scanner_interval_seconds=_positive(
            _require(raw, "scanner_interval_seconds", where="completion"),
            name="completion.scanner_interval_seconds",
        ),
    )


def _parse_writer(raw: dict) -> WriterConfig:
    batch = _require(raw, "batch_size", where="writer")
    if not isinstance(batch, int) or isinstance(batch, bool) or batch <= 0:
        raise ConfigError(
            f"writer.batch_size must be a positive integer, got {batch!r}"
        )
    return WriterConfig(batch_size=batch)


def _parse_preflight(raw: dict) -> PreflightConfig:
    return PreflightConfig(
        ack_timeout_seconds=_positive(
            _require(raw, "ack_timeout_seconds", where="preflight"),
            name="preflight.ack_timeout_seconds",
        )
    )


def _parse_connection(raw: dict) -> ConnectionConfig:
    stop = _require(raw, "stop_on_disconnect", where="connection")
    if not isinstance(stop, bool):
        raise ConfigError(
            f"connection.stop_on_disconnect must be true/false, got {stop!r}"
        )
    return ConnectionConfig(
        ping_interval_seconds=_positive(
            _require(raw, "ping_interval_seconds", where="connection"),
            name="connection.ping_interval_seconds",
        ),
        ping_timeout_seconds=_positive(
            _require(raw, "ping_timeout_seconds", where="connection"),
            name="connection.ping_timeout_seconds",
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
            "config.toml must define a [[nodes]] array (an ordered list of RPC node providers)"
        )
    if len(raw_nodes) != NUM_NODES:
        raise ConfigError(
            f"expected exactly {NUM_NODES} tables in [[nodes]] in config.toml (the schema requires one arrival-time column per node provider), "
            f"got {len(raw_nodes)}"
        )
    nodes: list[NodeConfig] = []
    seen_names: set[str] = set()
    for position, entry in enumerate(raw_nodes, start=1):
        if not isinstance(entry, dict):
            raise ConfigError(f"entry #{position} in [[nodes]] in config.toml must be a table, got {type(entry)}")
        name = _require(entry, "name", where=f"nodes #{position}")
        template = _require(entry, "url_template", where=f"nodes #{position}")
        if not isinstance(name, str) or not name:
            raise ConfigError(f"'name' for node #{position} in [[nodes]] in config.toml must be a string, got {type(name)}")
        if name in seen_names:
            raise ConfigError(f"duplicate node name '{name}' in [[nodes]] in config.toml")
        seen_names.add(name)
        if not isinstance(template, str) or not template:
            raise ConfigError(
                f"'url_template' for node '{name}' in config.toml must be a non-empty string"
            )
        url = _interpolate(template, env, node_name=name)
        nodes.append(
            NodeConfig(
                index=position,
                name=name,
                column=arrival_column(position),
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