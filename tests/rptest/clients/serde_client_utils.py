# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from enum import IntEnum


class SchemaType(IntEnum):
    AVRO = 1
    PROTOBUF = 2

    def __str__(self):
        return self.name


class SerdeClientType(IntEnum):
    Python = 1
    Java = 2
    Golang = 3

    def __str__(self):
        return self.name
