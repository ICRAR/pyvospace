import asyncpg
import configparser

from .uws import *
from .database import *
from pyvospace.core.exception import *

from aiohttp import web


class SpaceStorage(object):
    def __init__(self, config, db_pool, space_id):
        self.config = config
        self.space_id = space_id
        self.db_pool = db_pool
        self.executor = StorageUWSJobPool(space_id, db_pool)

    @classmethod
    async def get(cls, cfg_file):
        config = configparser.ConfigParser()
        config.read(cfg_file)

        name = config.get('Space', 'name')
        db_pool = await asyncpg.create_pool(dsn=config.get('Space', 'dsn'))

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                space_result = await conn.fetchrow("select * from space where name=$1 for update", name)
                if not space_result:
                    raise VOSpaceError(404, f'Space not found. {name}')

        return SpaceStorage(config, db_pool, int(space_result['id']))

    async def close(self):
        await self.executor.close()
        await self.db_pool.close()

    async def _execute(self, job, func, request):
        lock = 'share'
        if isinstance(job.transfer, PushToSpace):
            lock = 'update'

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    result = await self.executor.node_db._lock_node(job, conn, lock)
                    job.transfer.target = self.executor.node_db._resultset_to_node([result], [])
                    return await func(job, request)
        except asyncpg.exceptions.LockNotAvailableError:
            raise NodeBusyError(f"{job.transfer.target.path} is busy")

    async def execute(self, request, job_id, func):
        try:
            response = await self.executor.execute(job_id, self._execute, func, request)
            await asyncio.shield(self.executor.set_completed(job_id))
            return response

        except asyncio.CancelledError:
            return web.Response(status=400, text="Cancelled")

        except (NodeBusyError, InvalidJobError, InvalidJobStateError) as v:
            return web.Response(status=v.code, text=v.error)

        except (NodeDoesNotExistError, VOSpaceError) as e:
            await asyncio.shield(self.executor.set_error(job_id, e.error))
            return web.Response(status=e.code, text=e.error)

        except Exception as f:
            await asyncio.shield(self.executor.set_error(job_id, str(f)))
            return web.Response(status=500, text=str(f))
