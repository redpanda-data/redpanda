#!/usr/bin/env python3
# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

#
# pip install kafka-python
#
import random
import functools
import string
from kafka.protocol import api, types, group, fetch, metadata, produce
from kafka.record.default_records import DefaultRecordBatch, DefaultRecordBatchBuilder

request_types = \
        group.JoinGroupRequest + \
        group.SyncGroupRequest + \
        group.HeartbeatRequest + \
        group.LeaveGroupRequest + \
        [fetch.FetchRequest_v4] + \
        metadata.MetadataRequest + \
        [produce.ProduceRequest_v3,
            produce.ProduceRequest_v4,
            produce.ProduceRequest_v5]


def random_int16():
    return random.randint(-2**15, 2**15 - 1)


def random_int32():
    return random.randint(-2**31, 2**31 - 1)


def random_int64():
    return random.randint(-2**63, 2**63 - 1)


def random_string():
    return "".join(random.choice(string.printable) for _ in range(50))


def random_bytes(allow_none=False):
    if allow_none and random.choice((True, False)):
        return None
    return bytes(
        bytearray(
            random.getrandbits(8) for _ in range(random.randint(0, 256))))


def random_record_batch():
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=DefaultRecordBatch.CODEC_NONE,
        is_transactional=False,
        producer_id=-1,  #random_int64(), disable idempotent
        producer_epoch=random_int16(),
        base_sequence=random_int32(),
        batch_size=9999999999)

    builder.append(offset=random_int32(),
                   timestamp=random_int64(),
                   key=random_bytes(True),
                   value=random_bytes(True),
                   headers=())

    return builder.build()


def random_field_value(field, req_type):
    if field is types.Int8:
        return random.randint(-2**7, 2**7 - 1)

    elif field is types.Int16:
        return random_int16()

    elif field is types.Int32:
        return random_int32()

    elif field is types.Int64:
        return random_int64()

    elif isinstance(field, types.String):
        return random_string()

    elif field is types.Bytes:
        # generate a random record batch. this is a special case because record
        # batches show up in the schema as opaque bytes. in this instance the
        # only field of type Bytes in a produce request is the record batch.
        if any(map(lambda t: issubclass(req_type, t), produce.ProduceRequest)):
            # only generate a single batch per topic-partition since that is a
            # restriction baked into the protocol for versions >= 3
            return random_record_batch()

        return random_bytes()

    elif field is types.Boolean:
        return random.choice((True, False))

    elif isinstance(field, types.Array):
        if isinstance(field.array_of, types.Schema):
            generator = lambda: tuple(
                map(functools.partial(random_field_value, req_type=req_type),
                    field.array_of.fields))
        else:
            generator = lambda: random_field_value(field.array_of, req_type)
        return tuple(generator() for _ in range(random.randint(0, 10)))

    raise Exception("unhandled type: {}".format(field))


def random_request():
    req_type = random.choice(request_types)
    req_args = map(functools.partial(random_field_value, req_type=req_type),
                   req_type.SCHEMA.fields)
    request = req_type(*req_args)
    correlation_id = random_int32()
    client_id = random_string()
    header = api.RequestHeader(request, correlation_id, client_id)
    return header, request


if __name__ == "__main__":
    import sys

    msg_count = 1
    if len(sys.argv) > 1:
        msg_count = int(sys.argv[1])

    for _ in range(msg_count):
        header, request = random_request()
        message = b''.join([header.encode(), request.encode()])
        size = types.Int32.encode(len(message))
        payload = size + message

        sys.stdout.buffer.write(payload)
