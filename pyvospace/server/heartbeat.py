import asyncio
import aiohttp
import configparser
import asyncpg

from threading import Event
from contextlib import suppress


class StorageHeartbeatSource(object):
    def __init__(self, dsn, storage_id):
        self.dsn = dsn
        self.storage_id = storage_id
        self.busy_wait = Event()
        self.conn = None
        self.busy_task = None

    async def _storage_lock_loop(self):
        loop = asyncio.get_event_loop()
        while not self.busy_wait.is_set():
            await loop.run_in_executor(None, self.busy_wait.wait, 1000)

    async def run(self):
        self.conn = await asyncpg.connect(self.dsn)
        await self.conn.fetchrow("select pg_advisory_lock($1) as lock", self.storage_id)
        await self.conn.fetchrow("update nodes set busy=False where busy=True and storage_id=$1", self.storage_id)
        await self.conn.fetchrow("update storage set online=True where id=$1", self.storage_id)
        self.busy_task = asyncio.ensure_future(self._storage_lock_loop())

    async def close(self):
        if not self.busy_task:
            return

        self.busy_wait.set()
        with suppress(Exception):
            await self.busy_task
        self.busy_task = None

        try:
            await self.conn.fetchrow("update storage set online=False where id=$1", self.storage_id)
        finally:
            await self.conn.close()


class StorageHeartbeatSink(object):
    def __init__(self, db_pool, space_name):
        self.db_pool = db_pool
        self.space_name = space_name
        self.busy_wait = Event()
        self.busy_task = None

    async def _check_storage_heartbeat(self, storage_id):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                lock = await conn.fetchrow("select pg_try_advisory_xact_lock($1) as lock", storage_id)
                online = await conn.fetchrow("select online from storage where id=$1", storage_id)
                if lock['lock'] and online['online']:
                    await conn.fetchrow("update nodes set busy=False where busy=True and storage_id=$1", storage_id)

    async def _check_storage_heartbeat_loop(self):
        loop = asyncio.get_event_loop()
        while not self.busy_wait.is_set():
            async with self.db_pool.acquire() as conn:
                results = await conn.fetch("select id from storage where name=$1", self.space_name)
            tasks = []
            for result in results:
                tasks.append(asyncio.ensure_future(self._check_storage_heartbeat(result['id'])))
            await asyncio.wait(tasks)
            await loop.run_in_executor(None, self.busy_wait.wait, 30)

    async def run(self):
        self.busy_task = asyncio.ensure_future(self._check_storage_heartbeat_loop())

    async def close(self):
        if self.busy_task:
            self.busy_wait.set()
            with suppress(Exception):
                await self.busy_task