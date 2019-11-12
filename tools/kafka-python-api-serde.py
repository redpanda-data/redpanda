#!/usr/bin/env python3
#
# pip install kafka-python
#
import random
import string
from kafka.protocol import api, types, group, fetch

request_types = group.JoinGroupRequest + group.SyncGroupRequest + \
        group.HeartbeatRequest + group.LeaveGroupRequest + fetch.FetchRequest

def random_int32():
    return random.randint(-2**31, 2**31-1)

def random_string():
    return "".join(random.choice(string.printable) for _ in range(50))

def random_field_value(field):
    if field is types.Int8:
        return random.randint(-2**7, 2**7-1)

    elif field is types.Int16:
        return random.randint(-2**15, 2**15-1)

    elif field is types.Int32:
        return random_int32()

    elif field is types.Int64:
        return random.randint(-2**63, 2**63-1)

    elif isinstance(field, types.String):
        return random_string()

    elif field is types.Bytes:
        return bytes(bytearray(random.getrandbits(8) for _ in
            range(random.randint(0, 256))))

    elif field is types.Boolean:
        return random.choice((True, False))

    elif isinstance(field, types.Array):
        if isinstance(field.array_of, types.Schema):
            generator = tuple(map(random_field_value, field.array_of.fields))
        else:
            generator = random_field_value(field.array_of)
        return tuple(generator
            for _ in range(random.randint(0, 10)))

    raise Exception("unhandled type: {}".format(field))

def random_request():
    req_type = random.choice(request_types)
    req_args = map(random_field_value, req_type.SCHEMA.fields)
    request = req_type(*req_args)
    correlation_id = random_int32()
    client_id = random_string()
    header = api.RequestHeader(request, correlation_id, client_id)
    return header, request

if __name__ == "__main__":
    import sys

    header, request = random_request()
    message = b''.join([header.encode(), request.encode()])
    size = types.Int32.encode(len(message))
    payload = size + message

    sys.stdout.buffer.write(payload)
