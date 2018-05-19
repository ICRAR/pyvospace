import json
import aiohttp
import asyncio
import asyncpg
import configparser

from abc import ABCMeta, abstractmethod
from aiohttp import web
from contextlib import suppress
from collections import namedtuple

from .uws import set_uws_phase_to_completed, get_uws_job_conn, \
    UWSPhase, set_uws_phase_to_error, UWSKey, UWSJobExecutor
from pyvospace.core.exception import VOSpaceError, NodeDoesNotExistError, InvalidJobError, \
    InvalidJobStateError, NodeBusyError


class AbstractServerStorage(metaclass=ABCMeta):
    def __init__(self, app: aiohttp.web.Application, config: configparser.ConfigParser):
        self.app = app
        self.config = config

        self.name = config['Storage']['name']
        self.host = config['Storage']['host']
        self.port = int(config['Storage']['port'])
        self.parameters = json.loads(config['Storage']['parameters'])
        self.storage_id = None
        self.space_id = None
        self.db_pool = None
        self.executor = UWSJobExecutor()

    @abstractmethod
    async def download(self, request):
        raise NotImplementedError()

    @abstractmethod
    async def upload(self, request):
        raise NotImplementedError()

    async def _upload_data(self, request):
        return await self._data_transfer_request(request, self.upload)

    async def _download_data(self, request):
        return await self._data_transfer_request(request, self.download)

    async def shutdown(self):
        await self.executor.close()
        await self.db_pool.close()

    async def setup(self):
        self.db_pool = await asyncpg.create_pool(dsn=self.config['Space']['dsn'])
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                space_result = await conn.fetchrow("select * from space where name=$1 for update",
                                                   self.name)
                if not space_result:
                    raise VOSpaceError(404, f'Space not found. Name: {self.name}')

                storage_result = await conn.fetchrow("insert into storage (name, host, port, parameters) "
                                                     "values ($1, $2, $3, $4) on conflict (name, host, port) "
                                                     "do update set parameters=$4 returning id",
                                                     self.name, self.host,
                                                     self.port, json.dumps(self.parameters))

        self.storage_id = storage_result['id']
        self.space_id = space_result['id']
        self.app.router.add_put('/vospace/{direction}/{job_id}', self._upload_data)
        self.app.router.add_get('/vospace/{direction}/{job_id}', self._download_data)

    async def _data_transfer_request(self, request, func):
        space_id = self.space_id
        job_id = request.match_info.get('job_id', None)
        direction = request.match_info.get('direction', None)
        key = UWSKey(space_id, job_id)

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    job = await get_uws_job_conn(conn=conn, space_id=space_id, job_id=job_id, for_update=True)

                    # Can only start a EXECUTING Job
                    if job['phase'] != UWSPhase.Executing:
                        raise InvalidJobStateError('Invalid Job State. Job not EXECUTING.')

                    if job['direction'] != direction:
                        raise InvalidJobError('Direction does not match request.')

                    fut = self.executor.execute(self._run_transfer_job, key, request, job, func)
            return await fut

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)
        except asyncio.CancelledError:
            return web.Response(status=500, text="Cancelled")
        except Exception as g:
            return web.Response(status=500, text=str(g))

    async def _run_transfer_job(self, space_job_id, request, job, func):
        db_pool = self.db_pool

        try:
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    results = await self._lock_transfer_node(conn, space_job_id.space_id,
                                                             space_job_id.job_id, job['direction'])
                    if results.direction == 'pushToVoSpace':
                        setattr(request, 'transaction', conn)
                    setattr(request, 'job', results)
                    response = await func(request)

            with suppress(asyncio.CancelledError):
                await asyncio.shield(set_uws_phase_to_completed(db_pool, space_job_id.space_id,
                                                                space_job_id.job_id))
            return response
        # Ignore these errors i.e. don't set the job into error
        except NodeBusyError:
            raise
        # If the node has been deleted at some point then set the job into error
        except NodeDoesNotExistError as e:
            with suppress(asyncio.CancelledError):
                await asyncio.shield(set_uws_phase_to_error(db_pool, space_job_id.space_id,
                                                            space_job_id.job_id, str(e)))
            raise e

        except asyncio.CancelledError:
            with suppress(asyncio.CancelledError):
                await asyncio.shield(set_uws_phase_to_error(db_pool, space_job_id.space_id,
                                                            space_job_id.job_id, "Job Cancelled"))
            raise

        except Exception as f:
            with suppress(asyncio.CancelledError):
                await asyncio.shield(set_uws_phase_to_error(db_pool, space_job_id.space_id,
                                                            space_job_id.job_id, str(f)))
            raise VOSpaceError(500, str(f))

    async def _lock_transfer_node(self, conn, space_id, job_id, direction):
        # Row lock the node so updates/deletes can not occur for duration of upload/download
        try:
            if direction == 'pushToVoSpace':
                node_result = await conn.fetchrow("select nodes.type, nodes.name, nodes.path, uws_jobs.job_info, "
                                                  "uws_jobs.direction from nodes left join uws_jobs on "
                                                  "uws_jobs.target = nodes.path and target_id = nodes.space_id "
                                                  "where uws_jobs.id=$1 and uws_jobs.space_id=$2 "
                                                  "for update of nodes nowait",
                                                  job_id, space_id)
            else:
                node_result = await conn.fetchrow("select nodes.type, nodes.name, nodes.path, uws_jobs.job_info, "
                                                  "uws_jobs.direction from nodes left join uws_jobs "
                                                  "on uws_jobs.target = nodes.path and target_id = nodes.space_id "
                                                  "where uws_jobs.id=$1 and uws_jobs.space_id=$2 "
                                                  "for share of nodes nowait",
                                                  job_id, space_id)

            if not node_result:
                raise NodeDoesNotExistError(f"Target node for job does not exist.")

            node_result = dict(node_result)
            node_result['path'] = f"/{node_result['path'].replace('.', '/')}"
            node = namedtuple('node', node_result.keys())(**node_result)
            return node

        except asyncpg.exceptions.LockNotAvailableError:
            raise NodeBusyError("Node Busy.")