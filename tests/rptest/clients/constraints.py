# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from enum import Enum


class ConstraintType(str, Enum):
    RESTRIKT = 'restrict'
    CLAMP = 'clamp'


# Match the cluster configuration defaults
DEFAULT_SEGMENT_SIZE_CONSTRAINT = {
    'name': 'log_segment_size',
    'type': 'clamp',
    'min': 1048576
}

DEFAULT_SEGMENT_MS_CONSTRAINT = {
    'name': 'log_segment_ms',
    'type': 'clamp',
    'min': 600000,
    'max': 31536000000
}

DEFAULT_CONSTRAINTS = [
    DEFAULT_SEGMENT_SIZE_CONSTRAINT, DEFAULT_SEGMENT_MS_CONSTRAINT
]
