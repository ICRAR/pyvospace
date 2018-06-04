import asyncio
import asyncpg
import configparser

from aiohttp import web
from aiohttp_security import authorized_userid, permits
from aiohttp_security.api import AUTZ_KEY

from pyvospace.core.model import PushToSpace
from pyvospace.core.exception import VOSpaceError, PermissionDenied, NodeBusyError, InvalidJobError, \
    InvalidJobStateError, NodeDoesNotExistError
from .auth import SpacePermission
from .uws import StorageUWSJobPool
from .database import NodeDatabase


class SpaceStorageServer(web.Application, SpacePermission):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = configparser.ConfigParser()
        self.config.read(cfg_file)

        self.name = self.config.get('Space', 'name')
        self.space_id = None
        self.db_pool = None
        self.executor = None

    async def setup(self):
        self.db_pool = await asyncpg.create_pool(dsn=self.config.get('Space', 'dsn'))
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                space_result = await conn.fetchrow("select * from space "
                                                   "where name=$1 for update", self.name)
                if not space_result:
                    raise VOSpaceError(404, f'Space not found. {self.name}')
        self.executor = StorageUWSJobPool(space_result['id'], self.db_pool,
                                          self.config.get('Space', 'dsn'), self)
        await self.executor.setup()

    async def permits(self, identity, permission, context):
        autz_policy = self.get(AUTZ_KEY)
        if autz_policy is None:
            return True
        return await autz_policy.permits(identity, permission, context)

    async def shutdown(self):
        await self.executor.close()
        await self.db_pool.close()

    async def _execute(self, job, func, request):
        identity = await authorized_userid(request)
        if identity is None:
            raise PermissionDenied(f'Credentials not found.')

        lock = 'share'
        if isinstance(job.job_info, PushToSpace):
            lock = 'update'
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    node_result = await self.executor._get_executing_target(job.job_id, conn, lock)
                    target_node = NodeDatabase._resultset_to_node([node_result], [])
                    job.transfer.target = target_node

                    if not await request.app.permits(identity, 'dataTransfer', context=job):
                        raise PermissionDenied('data transfer denied.')

                    return await func(job, request)
        except asyncpg.exceptions.LockNotAvailableError:
            raise NodeBusyError('')

    async def execute(self, request, job_id, func):
        try:
            identity = await authorized_userid(request)
            if identity is None:
                raise PermissionDenied(f'Credentials not found.')

            response = await self.executor.execute(job_id, identity, self._execute, func, request)
            await asyncio.shield(self.executor.set_completed(job_id))
            return response

        except asyncio.CancelledError:
            await asyncio.shield(self.executor.set_error(job_id, 'Cancelled'))
            return web.Response(status=400, text="Cancelled")

        except (InvalidJobError, InvalidJobStateError) as v:
            return web.Response(status=v.code, text=v.error)

        except (NodeDoesNotExistError, PermissionDenied, NodeBusyError, VOSpaceError) as e:
            await asyncio.shield(self.executor.set_error(job_id, e.error))
            return web.Response(status=e.code, text=e.error)

        except Exception as f:
            await asyncio.shield(self.executor.set_error(job_id, str(f)))
            return web.Response(status=500, text=str(f))
