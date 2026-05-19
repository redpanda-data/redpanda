#!/usr/bin/env python3
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
"""Generate an Avro container file with decimal cases for interop testing.

The C++ side (avro_decimal_interop_test.cc) reads the file and asserts that
our `decode_avro_decimal` recovers the int128 value the reference Avro
encoder packed into each record's `payload` field.

Case names are the contract with the C++ test: mismatches surface as a
test failure naming the missing or extra case.

Usage: avro_decimal_interop_gen.py <output_path>
"""

from __future__ import annotations

import decimal
import json
import sys
from pathlib import Path

import avro.datafile
import avro.io
import avro.schema

# Iceberg caps decimal precision at 38, so the largest in-spec magnitude is
# 10**38 - 1. That value needs 127 bits (ceil(log2(10**38)) == 127) and still
# encodes as a full 16-byte payload, so we use it to exercise the int128
# boundary instead of the unrestricted ±2**127 range.
PRECISION = 38
PRECISION_MAX = 10**PRECISION - 1

SCHEMA = {
    "type": "record",
    "namespace": "redpanda.test",
    "name": "DecimalCase",
    "fields": [
        {"name": "name", "type": "string"},
        {
            "name": "payload",
            "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": PRECISION,
                "scale": 0,
            },
        },
    ],
}

# Mirrors the expected_values() table in avro_decimal_interop_test.cc.
# Names exercise wire payload lengths from 1 byte up to the 16-byte int128
# boundary, spanning sign-change points where the avro encoder must add a
# leading 0x00 or 0xFF byte to preserve the two's-complement sign.
CASES: list[tuple[str, int]] = [
    ("zero", 0),
    ("one", 1),
    ("neg_one", -1),
    ("127", 127),
    ("128", 128),  # 2 bytes: 0x00 0x80
    ("neg_128", -128),  # 1 byte: 0x80
    ("neg_129", -129),  # 2 bytes: 0xFF 0x7F
    ("12345", 12345),
    ("neg_12345", -12345),
    ("65536", 65536),
    ("two_pow_40", 2**40),
    ("neg_two_pow_40", -(2**40)),
    ("int64_max", 2**63 - 1),
    ("int64_min", -(2**63)),
    ("two_pow_64", 2**64),  # crosses the int64/int128 high-half boundary
    ("precision_38_max", PRECISION_MAX),  # full 16-byte payload, positive
    ("neg_precision_38_max", -PRECISION_MAX),  # full 16-byte payload, negative
]


def main(output_path: str) -> None:
    schema = avro.schema.parse(json.dumps(SCHEMA))
    with Path(output_path).open("wb") as f:
        writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema)
        try:
            for name, value in CASES:
                writer.append({"name": name, "payload": decimal.Decimal(value)})
        finally:
            writer.close()


if __name__ == "__main__":
    main(sys.argv[1])
