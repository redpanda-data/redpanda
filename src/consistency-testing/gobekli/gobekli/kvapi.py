import asyncio
import aiohttp
import json
from collections import namedtuple


class RequestTimedout(Exception):
    pass


class RequestCanceled(Exception):
    pass


Record = namedtuple('Record', ['write_id', 'value'])


class KVNode:
    def __init__(self, name, address):
        self.session = aiohttp.ClientSession()
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
        if resp.status == 200:
            data = await resp.read()
            data = json.loads(data)
            if data["status"] == "ok":
                if data["hasData"]:
                    return Record(data["writeID"], data["value"])
                else:
                    return None
            elif data["status"] == "unknown":
                raise RequestTimedout()
            elif data["status"] == "fail":
                raise RequestCanceled()
            else:
                raise Exception(f"Unknown status: {data['status']}")
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
        if resp.status == 200:
            data = await resp.read()
            data = json.loads(data)
            if data["status"] == "ok":
                if data["hasData"]:
                    return Record(data["writeID"], data["value"])
                else:
                    return None
            elif data["status"] == "unknown":
                raise RequestTimedout()
            elif data["status"] == "fail":
                raise RequestCanceled()
            else:
                raise Exception(f"Unknown status: {data['status']}")
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
        if resp.status == 200:
            data = await resp.read()
            data = json.loads(data)
            if data["status"] == "ok":
                if data["hasData"]:
                    return Record(data["writeID"], data["value"])
                else:
                    return None
            elif data["status"] == "unknown":
                raise RequestTimedout()
            elif data["status"] == "fail":
                raise RequestCanceled()
            else:
                raise Exception(f"Unknown status: {data['status']}")
        else:
            raise RequestTimedout()

    async def close_aio(self):
        await self.session.close()
