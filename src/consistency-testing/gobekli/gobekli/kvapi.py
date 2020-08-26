import asyncio
import aiohttp
import json
from collections import namedtuple


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

    async def get_aio(self, key):
        resp = None
        try:
            resp = await self.session.get(
                f"http://{self.address}/read?key={key}")
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
        if resp.status == 200:
            data = await resp.read()
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
        else:
            raise RequestTimedout()

    async def put_aio(self, key, value, write_id):
        data = json.dumps({"key": key, "value": value, "writeID": write_id})
        resp = None
        try:
            resp = await self.session.post(f"http://{self.address}/write",
                                           data=data)
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
        if resp.status == 200:
            data = await resp.read()
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
        else:
            raise RequestTimedout()

    async def cas_aio(self, key, prev_write_id, value, write_id):
        data = json.dumps({
            "key": key,
            "prevWriteID": prev_write_id,
            "value": value,
            "writeID": write_id
        })
        resp = None
        try:
            resp = await self.session.post(f"http://{self.address}/cas",
                                           data=data)
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
        if resp.status == 200:
            data = await resp.read()
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
        else:
            raise RequestTimedout()

    async def close_aio(self):
        await self.session.close()
