import json
import asyncio
import asyncpg
import configparser

from aiohttp import web
from aiohttp_security import authorized_userid, permits
from aiohttp_security.api import AUTZ_KEY
from abc import abstractmethod
from aiojobs.aiohttp import create_scheduler, spawn

from pyvospace.core.exception import VOSpaceError, PermissionDenied, NodeBusyError, InvalidJobError, \
    InvalidJobStateError, NodeDoesNotExistError
from .auth import SpacePermission
from .uws import StorageUWSJobPool
from .heartbeat import StorageHeartbeatSource


class HTTPSpaceStorageServer(web.Application, SpacePermission):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = configparser.ConfigParser()
        self.config.read(cfg_file)

        self.name = self.config.get('Space', 'name')
        self.name = self.config.get('Storage', 'name')
        self.host = self.config.get('Storage', 'host')
        self.https = self.config.getboolean('Storage', 'https', fallback=False)
        self.port = self.config.getint('Storage', 'port')
        self.parameters = json.loads(self.config.get('Storage', 'parameters'))
        self.space_id = None
        self.storage_id = None
        self.db_pool = None
        self.executor = None
        self.heartbeat = None

    async def setup(self):
        dsn = self.config.get('Space', 'dsn')
        self.db_pool = await asyncpg.create_pool(dsn=dsn)
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                space_result = await conn.fetchrow("select * from space where name=$1 for update",
                                                   self.name)
                if not space_result:
                    raise VOSpaceError(404, f'Space not found. {self.name}')
                self.space_id = space_result['id']
                result = await conn.fetchrow("insert into storage (name, host, port, parameters, https) "
                                             "values ($1, $2, $3, $4, $5) on conflict (name, host, port) "
                                             "do update set parameters=$4, https=$5 returning id",
                                             self.name, self.host, self.port,
                                             json.dumps(self.parameters), self.https)
                self.storage_id = result['id']

        self.executor = StorageUWSJobPool(self.space_id, self.storage_id, self.db_pool,
                                          self.config.get('Space', 'dsn'), self)
        await self.executor.setup()
        #self.heartbeat = StorageHeartbeatSource(dsn, self.storage_id)
        #await self.heartbeat.run()
        self.set_router()
        self['AIOJOBS_SCHEDULER'] = await create_scheduler()

    @abstractmethod
    async def download(self, job, request):
        raise NotImplementedError()

    @abstractmethod
    async def upload(self, job, request):
        raise NotImplementedError()

    def set_router(self):
        self.router.add_put('/vospace/{direction}/{job_id}', self.upload_request)
        self.router.add_get('/vospace/{direction}/{job_id}', self.download_request)

    async def upload_request(self, request):
        job_id = request.match_info.get('job_id', None)
        job = await spawn(request, self.execute_storage_job(request, job_id, self.upload))
        return await job.wait()

    async def download_request(self, request):
        job_id = request.match_info.get('job_id', None)
        job = await spawn(request, self.execute_storage_job(request, job_id, self.download))
        return await job.wait()

    async def permits(self, identity, permission, context):
        autz_policy = self.get(AUTZ_KEY)
        if autz_policy is None:
            return True
        return await autz_policy.permits(identity, permission, context)

    async def shutdown(self):
        #await self.heartbeat.close()
        await self['AIOJOBS_SCHEDULER'].close()
        await self.executor.close()
        await self.db_pool.close()

    async def execute_storage_job(self, request, job_id, func):
        try:
            identity = await authorized_userid(request)
            if identity is None:
                raise PermissionDenied(f'Credentials not found.')

            response = await self.executor.execute(job_id, identity, func, request)
            await asyncio.shield(self.executor.set_completed(job_id))
            return response

        except asyncio.CancelledError:
            await asyncio.shield(self.executor.set_error(job_id, 'Cancelled'))
            return web.Response(status=400, text="Cancelled")

        except (InvalidJobError, InvalidJobStateError, NodeBusyError) as v:
            return web.Response(status=v.code, text=v.error)

        except (NodeDoesNotExistError, PermissionDenied, VOSpaceError) as e:
            await asyncio.shield(self.executor.set_error(job_id, e.error))
            return web.Response(status=e.code, text=e.error)

        except BaseException as f:
            await asyncio.shield(self.executor.set_error(job_id, str(f)))
            return web.Response(status=500, text=str(f))