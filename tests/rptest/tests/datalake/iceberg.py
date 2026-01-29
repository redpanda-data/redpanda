# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Tuple


Identifier = Tuple[str, ...]
"""A tuple of strings representing a table identifier.

Each string in the tuple represents a part of the table's unique path. For example,
a table in a namespace might be identified as:

    ("namespace", "table_name")

Examples:
    >>> identifier: Identifier = ("namespace", "table_name")
"""
