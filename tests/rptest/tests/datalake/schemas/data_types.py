# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import string
from enum import Enum


class ProducerType(str, Enum):
    AVRO = "avro"
    PROTO2 = "proto2"
    PROTO3 = "proto3"


class GenericDataType:
    @classmethod
    def to_producer_type(cls, producer_type: ProducerType):
        if producer_type == ProducerType.AVRO:
            return cls.to_avro()
        elif producer_type == ProducerType.PROTO2:
            return cls.to_proto()
        elif producer_type == ProducerType.PROTO3:
            return cls.to_proto()
        else:
            raise NotImplementedError(f"Unknown ProducerType {producer_type}")  # pyright: ignore[reportUnreachable]

    @staticmethod
    def name() -> str:
        raise NotImplementedError("Not implemented in GenericDataType base class")

    @staticmethod
    def to_avro() -> str:
        raise NotImplementedError("Not implemented in GenericDataType base class")

    @staticmethod
    def to_proto() -> str:
        raise NotImplementedError("Not implemented in GenericDataType base class")


class GenericRecord(GenericDataType):
    """
    A generic record type. In Avro, this is a 'record', in Protobuf, this is a 'message'.
    """

    @staticmethod
    def name() -> str:
        return "generic_record"

    @staticmethod
    def to_avro() -> str:
        return "record"

    @staticmethod
    def to_proto() -> str:
        return "message"


class GenericArray(GenericDataType):
    """
    A generic array type. In Avro, this is an 'array', in Protobuf, this is a 'repeated'.
    """

    @staticmethod
    def name() -> str:
        return "generic_array"

    @staticmethod
    def to_avro() -> str:
        return "array"

    @staticmethod
    def to_proto() -> str:
        return "repeated"


class GenericMap(GenericDataType):
    """
    A generic map type. In Avro, this is a 'map', in Protobuf, this is a 'map'.
    """

    @staticmethod
    def name() -> str:
        return "generic_map"

    @staticmethod
    def to_avro() -> str:
        return "map"

    @staticmethod
    def to_proto() -> str:
        return "map"


class GenericEnum(GenericDataType):
    """
    A generic enum type. In Avro, this is an 'enum', in Protobuf, this is an 'enum'.
    """

    @staticmethod
    def name() -> str:
        return "generic_enum"

    @staticmethod
    def to_avro() -> str:
        return "enum"

    @staticmethod
    def to_proto() -> str:
        return "enum"

    @staticmethod
    def random(enum_symbols) -> str:
        return str(random.choice(enum_symbols))

    @staticmethod
    def make_valid_enum_name(n: int, producer_type: ProducerType) -> str:
        def make_avro_enum(n: int):
            """
            start with [A-Za-z_], subsequently contain only [A-Za-z0-9_]
            https://avro.apache.org/docs/1.11.1/specification/#names
            """
            first_char = random.choice(string.ascii_letters + "_")
            rest_of_name = random.choices(
                string.ascii_letters + string.digits + "_", k=n - 1
            )
            return first_char + "".join(rest_of_name)

        if producer_type == ProducerType.AVRO:
            return make_avro_enum(n)


class GenericUnion(GenericDataType):
    """
    A generic union type. In Avro, this is a 'union', in Protobuf, this is a 'union'.
    """

    @staticmethod
    def name() -> str:
        return "generic_union"

    @staticmethod
    def to_avro() -> str:
        return "union"

    @staticmethod
    def to_proto() -> str:
        return "oneof"


class GenericOptional(GenericDataType):
    """
    A generic optional type. In Avro, this is a regular field type with the default value of 'null', in Protobuf, this is an 'optional'.
    """

    @staticmethod
    def name() -> str:
        return "generic_optional"

    @staticmethod
    def to_avro() -> str:
        return "value_with_null_default"

    @staticmethod
    def to_proto() -> str:
        return "optional"


class GenericPrimitive(GenericDataType):
    @staticmethod
    def name() -> str:
        return "generic_primitive"


# TODO(willem): implement all complex data types
ALL_COMPLEX_DATA_TYPES = [
    GenericRecord,
    GenericArray,
    GenericMap,
    GenericEnum,
    # GenericUnion,
    # GenericOptional,
    GenericPrimitive,
]


class GenericBool(GenericDataType):
    """
    A generic bool type.
    """

    @staticmethod
    def name() -> str:
        return "generic_bool"

    @staticmethod
    def to_avro() -> str:
        return "boolean"

    @staticmethod
    def to_proto() -> str:
        return "bool"

    @staticmethod
    def random() -> bool:
        return random.choice([False, True])


class GenericInt(GenericDataType):
    """
    A generic int type.
    """

    @staticmethod
    def name() -> str:
        return "generic_int"

    @staticmethod
    def to_avro() -> str:
        return "int"

    @staticmethod
    def to_proto() -> str:
        return "int32"

    @staticmethod
    def random(mn: int, mx: int) -> int:
        return random.randint(mn, mx)


class GenericLong(GenericDataType):
    """
    A generic long type.
    """

    @staticmethod
    def name() -> str:
        return "generic_long"

    @staticmethod
    def to_avro() -> str:
        return "long"

    @staticmethod
    def to_proto() -> str:
        return "int64"

    @staticmethod
    def random(mn: int, mx: int) -> int:
        return random.randint(mn, mx)


class GenericFloat(GenericDataType):
    """
    A generic float type.
    """

    @staticmethod
    def name() -> str:
        return "generic_float"

    @staticmethod
    def to_avro() -> str:
        return "float"

    @staticmethod
    def to_proto() -> str:
        return "float"

    @staticmethod
    def random(mn: int, mx: int) -> float:
        return random.uniform(mn, mx)


class GenericDouble(GenericDataType):
    """
    A generic double type.
    """

    @staticmethod
    def name() -> str:
        return "generic_double"

    @staticmethod
    def to_avro() -> str:
        return "double"

    @staticmethod
    def to_proto() -> str:
        return "double"

    @staticmethod
    def random(mn: int, mx: int) -> float:
        return random.uniform(mn, mx)


class GenericBytes(GenericDataType):
    """
    A generic bytes type.
    """

    @staticmethod
    def name() -> str:
        return "generic_bytes"

    @staticmethod
    def to_avro() -> str:
        return "bytes"

    @staticmethod
    def to_proto() -> str:
        return "bytes"

    @staticmethod
    def random(n: int = 10) -> str:
        return "".join(
            random.choices(
                string.ascii_lowercase + string.ascii_uppercase + string.digits, k=n
            )
        )


class GenericString(GenericDataType):
    """
    A generic string type.
    """

    @staticmethod
    def name() -> str:
        return "generic_string"

    @staticmethod
    def to_avro() -> str:
        return "string"

    @staticmethod
    def to_proto() -> str:
        return "string"

    @staticmethod
    def random(n: int = 10) -> str:
        # Prefix string with 's' to prevent inappropriate conversion to integer types
        # in schema template, see make_random_record() in schema_generator.py
        return "s" + "".join(
            random.choices(
                string.ascii_lowercase + string.ascii_uppercase + string.digits, k=n - 1
            )
        )


ALL_PRIMITIVE_DATA_TYPES = [
    GenericBool,
    GenericInt,
    GenericLong,
    GenericFloat,
    GenericDouble,
    GenericString,
]

ALL_PRIMITIVE_DATA_TYPES_AND_ENUM = ALL_PRIMITIVE_DATA_TYPES + [GenericEnum]
