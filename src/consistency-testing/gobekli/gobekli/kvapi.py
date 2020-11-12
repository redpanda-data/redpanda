# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import asyncio
import aiohttp
import json
import sys
import traceback
from collections import namedtuple

import logging
from gobekli.logging import m

cmdlog = logging.getLogger("gobekli-cmd")


class RequestTimedout(Exception):
    pass


class RequestCanceled(Exception):
    pass


Record = namedtuple('Record', ['write_id', 'value'])
Response = namedtuple('Response', ['record', 'metrics'])


class KVNode:
    def __init__(self, name, address):
        timeout = aiohttp.ClientTimeout(total=10)
        self.session = aiohttp.ClientSession(timeout=timeout)
        self.address = address
        self.name = name

    async def get_aio(self, key, read_id):
        data = None
        try:
            resp = await self.session.get(
                f"http://{self.address}/read?key={key}&read_id={read_id}")
            if resp.status == 200:
                data = await resp.read()
            else:
                raise RequestTimedout()
        except aiohttp.client_exceptions.ServerDisconnectedError:
            raise RequestTimedout()
        except aiohttp.client_exceptions.ClientConnectorError:
            raise RequestTimedout()
        except aiohttp.client_exceptions.ClientOSError:
            raise RequestTimedout()
        except ConnectionResetError:
            raise RequestTimedout()
        except asyncio.TimeoutError:
            raise RequestTimedout()
        except:
            e, v = sys.exc_info()[:2]

            cmdlog.info(
                m("unexpected kv/get error",
                  type="error",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=traceback.format_exc()).with_time())

            raise RequestTimedout()
        data = json.loads(data)
        record = None
        if data["status"] == "ok":
            if data["hasData"]:
                record = Record(data["writeID"], data["value"])
        elif data["status"] == "unknown":
            raise RequestTimedout()
        elif data["status"] == "fail":
            raise RequestCanceled()
        else:
            raise Exception(f"Unknown status: {data['status']}")
        return Response(record, data["metrics"])

    async def put_aio(self, key, value, write_id):
        data = None
        try:
            resp = await self.session.post(f"http://{self.address}/write",
                                           data=json.dumps({
                                               "key": key,
                                               "value": value,
                                               "writeID": write_id
                                           }))
            if resp.status == 200:
                data = await resp.read()
            else:
                raise RequestTimedout()
        except aiohttp.client_exceptions.ServerDisconnectedError:
            raise RequestTimedout()
        except aiohttp.client_exceptions.ClientConnectorError:
            raise RequestTimedout()
        except aiohttp.client_exceptions.ClientOSError:
            raise RequestTimedout()
        except ConnectionResetError:
            raise RequestTimedout()
        except asyncio.TimeoutError:
            raise RequestTimedout()
        except:
            e, v = sys.exc_info()[:2]

            cmdlog.info(
                m("unexpected kv/put error",
                  type="error",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=traceback.format_exc()).with_time())

            raise RequestTimedout()
        data = json.loads(data)
        record = None
        if data["status"] == "ok":
            if data["hasData"]:
                record = Record(data["writeID"], data["value"])
        elif data["status"] == "unknown":
            raise RequestTimedout()
        elif data["status"] == "fail":
            raise RequestCanceled()
        else:
            raise Exception(f"Unknown status: {data['status']}")
        return Response(record, data["metrics"])

    async def cas_aio(self, key, prev_write_id, value, write_id):
        data = None
        try:
            resp = await self.session.post(f"http://{self.address}/cas",
                                           data=json.dumps({
                                               "key": key,
                                               "prevWriteID": prev_write_id,
                                               "value": value,
                                               "writeID": write_id
                                           }))
            if resp.status == 200:
                data = await resp.read()
            else:
                raise RequestTimedout()
        except aiohttp.client_exceptions.ServerDisconnectedError:
            raise RequestTimedout()
        except aiohttp.client_exceptions.ClientConnectorError:
            raise RequestTimedout()
        except aiohttp.client_exceptions.ClientOSError:
            raise RequestTimedout()
        except ConnectionResetError:
            raise RequestTimedout()
        except asyncio.TimeoutError:
            raise RequestTimedout()
        except:
            e, v = sys.exc_info()[:2]

            cmdlog.info(
                m("unexpected kv/cas error",
                  type="error",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=traceback.format_exc()).with_time())

            raise RequestTimedout()
        data = json.loads(data)
        record = None
        if data["status"] == "ok":
            if data["hasData"]:
                record = Record(data["writeID"], data["value"])
        elif data["status"] == "unknown":
            raise RequestTimedout()
        elif data["status"] == "fail":
            raise RequestCanceled()
        else:
            raise Exception(f"Unknown status: {data['status']}")
        return Response(record, data["metrics"])

    async def close_aio(self):
        await self.session.close()
