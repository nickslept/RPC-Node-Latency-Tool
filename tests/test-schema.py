"""Tests for core.schema.

The headline test is the null round-trip: a null in a node column must survive a
parquet write/read as a true Arrow null and never be coerced to 0 (or to a float
NaN). That coercion would silently turn "this node did not report" into
"reported at offset 0" -- the fastest possible time -- which would corrupt every
latency statistic downstream. This is exactly the kind of pyarrow gotcha the
build plan calls for catching with two fake rows rather than a 200k-row run.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from core import schema


def test_columns_are_fixed_six_in_order():
    assert schema.COLUMNS == [
        "tx_hash",
        "node_1_arrival_ns",
        "node_2_arrival_ns",
        "node_3_arrival_ns",
        "node_4_arrival_ns",
        "node_5_arrival_ns",
    ]
    assert len(schema.COLUMNS) == 6
    assert schema.SCHEMA.names == schema.COLUMNS


def test_arrival_columns_are_nullable_int64():
    for name in schema.ARRIVAL_COLUMNS:
        field = schema.SCHEMA.field(name)
        assert field.type == pa.int64()
        assert field.nullable is True


def test_arrival_column_naming_and_bounds():
    assert schema.arrival_column(1) == "node_1_arrival_ns"
    assert schema.arrival_column(5) == "node_5_arrival_ns"
    with pytest.raises(ValueError):
        schema.arrival_column(0)
    with pytest.raises(ValueError):
        schema.arrival_column(6)


def test_null_round_trips_through_parquet(tmp_path):
    # Two rows: one fully reported, one with a None in node_3 and node_5.
    table = pa.table(
        {
            "tx_hash": ["0xaaa", "0xbbb"],
            "node_1_arrival_ns": [100, 200],
            "node_2_arrival_ns": [150, 260],
            "node_3_arrival_ns": [120, None],   # did not report
            "node_4_arrival_ns": [130, 240],
            "node_5_arrival_ns": [None, 280],   # did not report
        },
        schema=schema.SCHEMA,
    )

    path = tmp_path / "roundtrip.parquet"
    pq.write_table(table, path)
    back = pq.read_table(path)

    assert back.schema.equals(schema.SCHEMA)

    n3 = back.column("node_3_arrival_ns").to_pylist()
    n5 = back.column("node_5_arrival_ns").to_pylist()

    # The crucial assertions: the missing values are genuine None, NOT 0/NaN.
    assert n3 == [120, None]
    assert n5 == [None, 280]
    assert back.column("node_3_arrival_ns").null_count == 1
    assert back.column("node_5_arrival_ns").null_count == 1


def test_empty_table_conforms():
    t = schema.empty_table()
    assert t.num_rows == 0
    assert t.schema.equals(schema.SCHEMA)


def test_run_metadata_round_trip():
    md = schema.build_run_metadata(start_ref_ns=123456789, run_started_utc="2026-06-15T14:32:00Z")
    schema_with_md = schema.SCHEMA.with_metadata(md)

    parsed = schema.read_run_metadata(schema_with_md)
    assert parsed["start_ref_ns"] == 123456789
    assert parsed["run_started_utc"] == "2026-06-15T14:32:00Z"
    assert parsed["schema_version"] == schema.SCHEMA_VERSION


def test_run_metadata_survives_parquet(tmp_path):
    md = schema.build_run_metadata(start_ref_ns=42, run_started_utc="2026-06-15T00:00:00Z")
    table = schema.empty_table().replace_schema_metadata(md)
    path = tmp_path / "meta.parquet"
    pq.write_table(table, path)

    parsed = schema.read_run_metadata(pq.read_table(path).schema)
    assert parsed["start_ref_ns"] == 42