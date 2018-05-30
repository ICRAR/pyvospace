import datetime
import asyncio
import functools

from contextlib import suppress

from pyvospace.core.model import *
from pyvospace.core.exception import *
from .database import NodeDatabase


class UWSJobPool(object):
    def __init__(self, space_id, db_pool):
        self.db_pool = db_pool
        self.space_id = space_id
        self.executor = UWSJobExecutor(space_id)

    async def close(self):
        await self.executor.close()

    async def get_uws_job_phase(self, job_id):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.fetchrow("select phase, owner from uws_jobs "
                                             "where id=$1 and space_id=$2",
                                             job_id, self.space_id)
                if not result:
                    raise JobDoesNotExistError("Job does not exist")
                return result

    async def get_uws_job(self, job_id):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction(isolation='read_committed'):
                return await self._get_uws_job_conn(conn, job_id)

    async def _get_uws_job_conn(self, conn, job_id, for_update=False):
        try:
            if for_update:
                result = await conn.fetchrow("select * from uws_jobs "
                                             "where id=$1 and space_id=$2 for update",
                                             job_id, self.space_id)
            else:
                result = await conn.fetchrow("select * from uws_jobs "
                                             "where id=$1 and space_id=$2",
                                             job_id, self.space_id)
            if not result:
                raise VOSpaceError(404, f"Invalid Request. UWS job {job_id} does not exist.")

            return result
        except ValueError as e:
            raise InvalidJobError(f"Invalid JobId: {str(e)}")

    async def _update_uws_job(self, job):
        transfer_string = job.transfer.tostring()
        results_string = job.results_tostring()
        target_tree = NodeDatabase.path_to_ltree(job.job_info.target.path)

        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.fetchrow("update uws_jobs set phase=$1, results=$2, "
                                             "transfer=$3, target=$4, target_space_id=$7 "
                                             "where phase<=$5 and id=$6 and uws_jobs.space_id=$7 "
                                             "returning id",
                                             job.phase, results_string, transfer_string,
                                             target_tree, UWSPhase.Executing,
                                             job.job_id, self.space_id)
        if not result:
            raise InvalidJobStateError('Job not found or (ABORTED, ERROR)')

    def _resultset_to_job(self, result):
        job_info = Transfer.fromstring(result['job_info'])
        results = None
        if result['results']:
            results = UWSResult.fromstring(result['results'])
        #transfer = None
        #if result['transfer']:
        #    transfer = Transfer.fromstring(result['transfer'])
        job = UWSJob(result['id'], result['phase'], result['destruction'],
                     job_info, results, result['error'])
        job.owner = result['owner']
        return job

    async def get(self, job_id):
        async with self.db_pool.acquire() as conn:
            result = await self._get_uws_job_conn(conn=conn, job_id=job_id)
        return self._resultset_to_job(result)

    async def create(self, job_info, identity, phase=UWSPhase.Pending):
        job_info_string = job_info.tostring()
        destruction = datetime.datetime.utcnow() + datetime.timedelta(seconds=3000)
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.fetchrow("insert into uws_jobs (phase, destruction, job_info, owner, space_id) "
                                             "values ($1, $2, $3, $4, $5) returning *",
                                             phase, destruction, job_info_string, identity, self.space_id)
        return self._resultset_to_job(result)

    async def execute(self, job_id, identity, func, *args):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                result = await self._get_uws_job_conn(conn=conn, job_id=job_id, for_update=True)
                if identity != result['owner']:
                    raise PermissionDenied(f'{identity} is not the owner of the job.')
                # Can only start a PENDING Job
                if result['phase'] != UWSPhase.Pending:
                    raise InvalidJobStateError('Invalid Job State')
                job = self._resultset_to_job(result)
                fut = self.executor.execute(job, func, *args)
        return await fut

    async def set_executing(self, job_id):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                return await conn.fetchrow("update uws_jobs set phase=$2 "
                                           "where id=$1 and phase=$3 and space_id=$4 returning id",
                                           job_id, UWSPhase.Executing,
                                           UWSPhase.Pending, self.space_id)

    async def set_completed(self, job_id):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                return await conn.fetchrow("update uws_jobs set phase=$2 "
                                           "where id=$1 and phase=$3 and space_id=$4 returning id",
                                           job_id, UWSPhase.Completed,
                                           UWSPhase.Executing, self.space_id)

    async def set_error(self, job_id, error):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                return await conn.fetchrow("update uws_jobs set phase=$3, error=$2 "
                                           "where id=$1 and phase!=$4 and space_id=$5 returning id",
                                           job_id, error, UWSPhase.Error,
                                           UWSPhase.Aborted, self.space_id)

    async def abort(self, job_id, conn):
        result = await conn.fetchrow("update uws_jobs set phase=$2 "
                                     "where id=$1 and space_id=$4 returning id",
                                     job_id, UWSPhase.Aborted, self.space_id)
        return result


class StorageUWSJob(UWSJob):
    def __init__(self, job_id, phase, destruction, job_info, transfer):
        super().__init__(job_id, phase, destruction, job_info, None, None)
        self.transfer = transfer


class StorageUWSJobPool(UWSJobPool):
    def __init__(self, space_id, db_pool):
        super().__init__(space_id, db_pool)

    def _resultset_to_job(self, result):
        job_info = Transfer.fromstring(result['job_info'])
        transfer = Transfer.fromstring(result['transfer'])
        return StorageUWSJob(result['id'], result['phase'], result['destruction'], job_info, transfer)

    async def _get_executing_target(self, job_id, conn, lock='update'):
        result = await conn.fetchrow("select nodes.* from nodes left join uws_jobs on "
                                     "nodes.space_id = uws_jobs.space_id and nodes.path = uws_jobs.target "
                                     "where uws_jobs.id=$1 and uws_jobs.space_id=$2 "
                                     f"and uws_jobs.phase=2 for {lock} of nodes nowait",
                                     job_id, self.space_id)
        if not result:
            raise NodeDoesNotExistError('')
        return result

    async def execute(self, job_id, func, *args):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                result = await self._get_uws_job_conn(conn=conn, job_id=job_id, for_update=True)
                # Can only start an EXECUTING Job if its a protocol transfer
                if result['phase'] != UWSPhase.Executing:
                    raise InvalidJobStateError('Invalid Job State')
                job = self._resultset_to_job(result)
                if not isinstance(job.job_info, ProtocolTransfer):
                    raise InvalidJobStateError('Invalid Job Type')
                fut = self.executor.execute(job, func, *args)
        return await fut


class UWSJobExecutor(object):
    def __init__(self, space_id):
        self.job_tasks = {}
        self.space_id = space_id
        self._closing = False

    @property
    def closing(self):
        return self._closing

    def execute(self, job, func, *args):
        if self._closing:
            return ClosingError()

        key = (job.job_id, self.space_id)
        task = self.job_tasks.get(key, None)
        if task:
            raise InvalidJobStateError("Job already running")
        task = asyncio.ensure_future(func(job, *args))
        self.job_tasks[key] = (task, *args)
        task.add_done_callback(functools.partial(self._done, job))
        return task

    def _done(self, job, task):
        with suppress(Exception):
            task.exception()
        key = (job.job_id, self.space_id)
        del self.job_tasks[key]

    async def abort(self, job_id):
        key = (job_id, self.space_id)
        job_tuple = self.job_tasks.get(key, None)
        if job_tuple:
            job_tuple[0].cancel()
            with suppress(Exception):
                await job_tuple[0]

    async def close(self):
        if self._closing:
            return
        self._closing = True

        # wait for all tasks to gracefully end
        for _, job_tuple in dict(self.job_tasks).items():
            with suppress(Exception):
                await job_tuple[0]

        assert len(self.job_tasks) == 0

