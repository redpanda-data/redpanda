#!/usr/bin/python3
"""
This helper script was used to generate values for
transform_utils_test.cc
"""

import codecs
import struct
import uuid
from decimal import Decimal
from typing import Optional, Union

import mmh3


def bucket_transform(hash_func, num_buckets, v):
    hash = hash_func(v)
    bucket = (hash & 2147483647) % num_buckets
    print("hash: {} value: {} bucket: {}".format(hash, v, bucket))


def hash_int(v) -> int:
    print(mmh3.hash(struct.pack("<q", v)))
    return mmh3.hash(struct.pack("<q", v))


bucket_transform(hash_int, 16, 321123)
bucket_transform(hash_int, 128, 321123)
bucket_transform(hash_int, 2025, 321123)
bucket_transform(hash_int, 16, 321123321123)
bucket_transform(hash_int, 128, 321123321123)
bucket_transform(hash_int, 2025, 321123321123)

bucket_transform(hash_int, 16, 1000)
bucket_transform(hash_int, 128, 1000)
bucket_transform(hash_int, 2025, 1000)

bucket_transform(hash_int, 16, 1000000 * 60 * 60 * 10)
bucket_transform(hash_int, 128, 1000000 * 60 * 60 * 10)
bucket_transform(hash_int, 2025, 1000000 * 60 * 60 * 10)

bucket_transform(hash_int, 16, 1741177530000000)
bucket_transform(hash_int, 128, 1741177530000000)
bucket_transform(hash_int, 2025, 1741177530000000)


def decimal_to_unscaled(value: Decimal) -> int:
    """Get an unscaled value given a Decimal value.

    Args:
        value (Decimal): A Decimal instance.

    Returns:
        int: The unscaled value.
    """
    sign, digits, _ = value.as_tuple()
    return int(Decimal((sign, digits, 0)).to_integral_value())


def bytes_required(value: Union[int, Decimal]) -> int:
    """Return the minimum number of bytes needed to serialize a decimal or unscaled value.

    Args:
        value (int | Decimal): a Decimal value or unscaled int value.

    Returns:
        int: the minimum number of bytes needed to serialize the value.
    """
    if isinstance(value, int):
        return (value.bit_length() + 8) // 8
    elif isinstance(value, Decimal):
        return (decimal_to_unscaled(value).bit_length() + 8) // 8

    raise ValueError(f"Unsupported value: {value}")


def decimal_to_bytes(value: Decimal, byte_length: Optional[int] = None) -> bytes:
    """Return a byte representation of a decimal.

    Args:
        value (Decimal): a decimal value.
        byte_length (int): The number of bytes.
    Returns:
        bytes: the unscaled value of the Decimal as bytes.
    """
    unscaled_value = decimal_to_unscaled(value)
    if byte_length is None:
        byte_length = bytes_required(unscaled_value)
    return unscaled_value.to_bytes(byte_length, byteorder="big", signed=True)


def uuid_to_bytes(value: uuid.UUID) -> bytes:
    return value.bytes


def hash_decimal(v: Decimal) -> int:
    print(">>> {}".format(codecs.encode(decimal_to_bytes(v), "hex")))
    return mmh3.hash(decimal_to_bytes(v))


def hash_string(v: str) -> int:
    return mmh3.hash(v)


def hash_uuid(v: uuid.UUID) -> int:
    print(">>> {}".format(codecs.encode(uuid_to_bytes(v), "hex")))
    return mmh3.hash(uuid_to_bytes(v))


def hash_bytes(v: bytes) -> int:
    return mmh3.hash(v)


bucket_transform(hash_decimal, 16, Decimal(123))
bucket_transform(hash_decimal, 128, Decimal(123))
bucket_transform(hash_decimal, 2025, Decimal(123))
bucket_transform(hash_decimal, 16, Decimal(18446744073709551739))
bucket_transform(hash_decimal, 128, Decimal(18446744073709551739))
bucket_transform(hash_decimal, 2025, Decimal(18446744073709551739))
bucket_transform(hash_decimal, 16, Decimal(116609477114555699131776463578))
bucket_transform(hash_decimal, 128, Decimal(116609477114555699131776463578))
bucket_transform(hash_decimal, 2025, Decimal(116609477114555699131776463578))
bucket_transform(hash_decimal, 16, Decimal(-18446744073709551493))
bucket_transform(hash_decimal, 128, Decimal(-18446744073709551493))
bucket_transform(hash_decimal, 2025, Decimal(-18446744073709551493))
bucket_transform(hash_decimal, 16, Decimal(-1012))
bucket_transform(hash_decimal, 128, Decimal(-1012))
bucket_transform(hash_decimal, 2025, Decimal(-1012))
bucket_transform(hash_decimal, 16, Decimal(-5923679718586680004350836613))
bucket_transform(hash_decimal, 128, Decimal(-5923679718586680004350836613))
bucket_transform(hash_decimal, 2025, Decimal(-5923679718586680004350836613))
bucket_transform(hash_string, 16, "non-latin to test UTF-8: Алексей")
bucket_transform(hash_string, 128, "non-latin to test UTF-8: Алексей")
bucket_transform(hash_string, 2025, "non-latin to test UTF-8: Алексей")
bucket_transform(hash_uuid, 16, uuid.UUID("ab4ed576-b638-424f-89d8-4ea602393772"))
bucket_transform(hash_uuid, 128, uuid.UUID("ab4ed576-b638-424f-89d8-4ea602393772"))
bucket_transform(hash_uuid, 2025, uuid.UUID("ab4ed576-b638-424f-89d8-4ea602393772"))
bucket_transform(hash_bytes, 16, bytes.fromhex("deadbeef"))
bucket_transform(hash_bytes, 128, bytes.fromhex("deadbeef"))
bucket_transform(hash_bytes, 2025, bytes.fromhex("deadbeef"))
