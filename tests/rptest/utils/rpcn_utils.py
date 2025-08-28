# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.services.redpanda import RedpandaService


def counter_stream_config(
    redpanda: RedpandaService,
    topic: str,
    subject: str,
    field_to_bloblang: dict[str, str] = {},
    cnt: int = 3000,
    interval_ms: int | None = None,
) -> dict:
    """
    Creates a RPCN config where the input is a simple counter, and fields are
    mapped via the input mapping of bloblang functions.

    If no field-mapping functions are provided, the stream will not use Schema
    Registry.
    """
    processors: list[dict] = []
    if len(field_to_bloblang) > 0:
        mapping_str = "\n".join(
            [
                f"root.{field} = {bloblang_fn}"
                for field, bloblang_fn in field_to_bloblang.items()
            ]
        )
        processors.append({"mapping": mapping_str})
        processors.append(
            {
                "schema_registry_encode": {
                    "url": redpanda.schema_reg().split(",")[0],
                    "subject": subject,
                    "refresh_period": "10s",
                }
            }
        )
    return {
        "input": {
            "generate": {
                "mapping": "root = counter()",
                "interval": "" if interval_ms is None else f"{interval_ms}ms",
                "count": cnt,
                "batch_size": 1,
            }
        },
        "pipeline": {"processors": processors},
        "output": {
            "redpanda": {
                "seed_brokers": redpanda.brokers_list(),
                "topic": topic,
            }
        },
    }
