import datetime
import asyncio
import functools
import asyncpg
import json

from contextlib import suppress

from pyvospace.core.model import UWSPhase, UWSJob, UWSResult, Transfer, \
    ProtocolTransfer, Copy, Move, Node, ContainerNode
from pyvospace.core.exception import VOSpaceError, JobDoesNotExistError, InvalidJobError, \
    InvalidJobStateError, PermissionDenied, NodeDoesNotExistError, ClosingError, NodeBusyError
from .database import NodeDatabase
from pyvospace.server import busy_fuzz


class UWSJobPool(object):
    def __init__(self, space_id, db_pool, permission):
        self.db_pool = db_pool
        self.space_id = space_id
        self.executor = UWSJobExecutor(space_id)
        self.permission = permission

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
        except asyncpg.exceptions.DataError as e:
            raise InvalidJobError(f"Invalid JobId: {str(e)}")

    async def _update_uws_job(self, job, conn):
        transfer_string = job.transfer.tostring()
        results_string = job.results_tostring()
        target_tree = NodeDatabase.path_to_ltree(job.job_info.target.path)
        result = await conn.fetchrow("with cte as "
                                     "(select id, space_id, phase from uws_jobs "
                                     "where id=$7 and space_id=$8 for update) "
                                     "update uws_jobs set phase=$1, results=$2, "
                                     "transfer=$3, node_path=$4, node_path_modified=$5 "
                                     "from cte where cte.phase<=$6 and "
                                     "uws_jobs.id=cte.id and uws_jobs.space_id=cte.space_id "
                                     "returning cte.id",
                                     job.phase, results_string, transfer_string,
                                     target_tree, job.node_path_modified, UWSPhase.Executing,
                                     job.job_id, self.space_id)
        if not result:
            raise InvalidJobStateError('Job not found or (ABORTED, ERROR)')

    def _resultset_to_job(self, result):
        job_info = Transfer.fromstring(result['job_info'])
        results = None
        if result['results']:
            results = UWSResult.fromstring(result['results'])
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
                # Can only start a PENDING Job
                if result['phase'] != UWSPhase.Pending:
                    raise InvalidJobStateError('Invalid Job State')

                job = self._resultset_to_job(result)
                if not await self.permission.permits(identity, 'runJob', context=job):
                    raise PermissionDenied('runJob denied.')

                fut = self.executor.execute(job, func, *args)
        return await fut

    async def abort(self, job_id, identity):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                result = await self._get_uws_job_conn(conn=conn, job_id=job_id, for_update=True)
                if result['phase'] in (UWSPhase.Completed, UWSPhase.Error):
                    raise InvalidJobStateError("Can't cancel a job that is COMPLETED or in ERROR.")

                job = self._resultset_to_job(result)
                if isinstance(job, Copy) or isinstance(job, Move):
                    # aborting a file copy or move can produce weird results so ignore it
                    if job.phase >= UWSPhase.Executing:
                        raise InvalidJobStateError("Can't abort a move/copy that is EXECUTING.")

                if not await self.permission.permits(identity, 'abortJob', context=job):
                    raise PermissionDenied('abortJob denied.')

                with suppress(asyncio.CancelledError):
                    await asyncio.shield(self.set_aborted(job_id, conn))

        with suppress(asyncio.CancelledError):
            await asyncio.shield(self.executor.abort(job_id))

    async def set_executing(self, job_id):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                return await conn.fetchrow("with cte as (select id, space_id, phase from uws_jobs "
                                           "where id=$1 and space_id=$4 for update)"
                                           "update uws_jobs set phase=$2 "
                                           "from cte where cte.phase=$3 and "
                                           "uws_jobs.id=cte.id and uws_jobs.space_id=cte.space_id "
                                           "returning cte.id",
                                           job_id, UWSPhase.Executing,
                                           UWSPhase.Pending, self.space_id)

    async def set_completed(self, job_id):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                return await conn.fetchrow("with cte as (select id, space_id, phase from uws_jobs "
                                           "where id=$1 and space_id=$4 for update)"
                                           "update uws_jobs set phase=$2 "
                                           "from cte where cte.phase=$3 and "
                                           "uws_jobs.id=cte.id and uws_jobs.space_id=cte.space_id "
                                           "returning cte.id",
                                           job_id, UWSPhase.Completed,
                                           UWSPhase.Executing, self.space_id)

    async def set_error(self, job_id, error):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                return await conn.fetchrow("with cte as (select id, space_id, phase from uws_jobs "
                                           "where id=$1 and space_id=$5 for update)"
                                           "update uws_jobs set phase=$3, error=$2 "
                                           "from cte where cte.phase!=$4 and "
                                           "uws_jobs.id=cte.id and uws_jobs.space_id=cte.space_id "
                                           "returning cte.id",
                                           job_id, error, UWSPhase.Error,
                                           UWSPhase.Aborted, self.space_id)

    async def set_aborted(self, job_id, conn):
        return await conn.fetchrow("with cte as (select id, space_id, phase from uws_jobs "
                                   "where id=$1 and space_id=$4 for update)"
                                   "update uws_jobs set phase=$2 "
                                   "from cte where cte.phase=any($3::integer[]) and "
                                   "uws_jobs.id=cte.id and uws_jobs.space_id=cte.space_id "
                                   "returning cte.id",
                                   job_id, UWSPhase.Aborted,
                                   [UWSPhase.Queued, UWSPhase.Pending, UWSPhase.Executing], self.space_id)


class NodeProxy:
    def __init__(self, proxied_object, tr):
        self._proxied = proxied_object
        self._tr = tr

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        try:
            getattr(self._proxied, key)
            self._proxied.__setattr__(key, value)
        except AttributeError:
            pass

    def __getattr__(self, attr):
        value = getattr(self._proxied, attr)
        return value

    async def save(self):
        with suppress(asyncio.CancelledError):
            await asyncio.shield(self._save())

    async def _save(self):
        if not self._tr._conn:
            raise InvalidJobStateError('transaction not established')

        if not self._tr._exclusive:
            raise InvalidJobStateError('transaction not exclusive')

        root = self._proxied
        node_db = self._tr._job._storage_pool.node_db
        await node_db.update_properties(root, self._tr._conn, root.owner, check_identity=False)
        if isinstance(root, ContainerNode):
            nodes = [node for node in Node.walk(root)]
            nodes.pop(0)
            await node_db.insert_tree(nodes, self._tr._conn)


class StorageUWSJob(UWSJob):
    def __init__(self, storage_pool, job_id, phase, destruction, job_info, transfer):
        super().__init__(job_id, phase, destruction, job_info, None, None)
        self._storage_pool = storage_pool
        self._transfer = transfer

    @property
    def transfer(self):
        return self._transfer

    @transfer.setter
    def transfer(self, value):
        self._transfer = value

    class StorageUWSJobTransaction(object):
        def __init__(self, job, exclusive):
            self._job = job
            self._conn = None
            self._tr = None
            self._exclusive = exclusive
            self._target = None

        @property
        def target(self):
            return self._target

        async def start(self):
            with suppress(asyncio.CancelledError):
                await asyncio.shield(self._start())

        async def rollback(self):
            with suppress(asyncio.CancelledError):
                await asyncio.shield(self._rollback())

        async def commit(self):
            with suppress(asyncio.CancelledError):
                await asyncio.shield(self._commit())

        async def _start(self):
            if self._conn is None:
                self._conn = await self._job._storage_pool.db_pool.acquire()
                self._tr = self._conn.transaction()
                await self._tr.start()

                job_result = await self._job._storage_pool._get_uws_job_conn(conn=self._conn,
                                                                             job_id=self._job.job_id,
                                                                             for_update=True)
                if job_result['phase'] != UWSPhase.Executing:
                    raise InvalidJobStateError('Invalid Job State')

                if self._exclusive:
                    node_results = await self._conn.fetch("select *, nlevel(path) from nodes "
                                                          "where path <@ $1 and space_id=$2 "
                                                          "order by nlevel(path) asc for update",
                                                          job_result['node_path'],
                                                          self._job._storage_pool.space_id)
                else:
                    node_results = await self._conn.fetch("select *, nlevel(path) from nodes "
                                                          "where path <@ $1 and space_id=$2 "
                                                          "order by nlevel(path) asc for share",
                                                          job_result['node_path'],
                                                          self._job._storage_pool.space_id)
                if not node_results:
                    raise NodeDoesNotExistError("target node does not exist.")

                # The first entry should always be the target path in the job
                if node_results[0]['path'] != job_result['node_path']:
                    raise InvalidJobError(f"{node_results[0]['path']} does not match target "
                                          f"{job_result['node_path']}")

                if node_results[0]['path_modified'] != job_result['node_path_modified']:
                    raise NodeDoesNotExistError('target has been modified.')

                node_properties = await self._conn.fetch("select * from properties "
                                                         "where node_path=any($1::ltree[]) and space_id=$2",
                                                         [node['path'] for node in node_results],
                                                         self._job._storage_pool.space_id)

                root_node = NodeDatabase.resultset_to_node_tree(node_results, node_properties)
                self._target = NodeProxy(root_node, self)

        async def _rollback(self):
            if self._conn:
                try:
                    await self._tr.rollback()
                finally:
                    await self._job._storage_pool.db_pool.release(self._conn)
                    self._conn = None

        async def _commit(self):
            if self._conn:
                try:
                    await self._tr.commit()
                finally:
                    await self._job._storage_pool.db_pool.release(self._conn)
                    self._conn = None

        async def __aenter__(self):
            with suppress(asyncio.CancelledError):
                await asyncio.shield(self._start())
                return self

        async def __aexit__(self, exc_type, exc, tb):
            if exc:
                with suppress(asyncio.CancelledError):
                    await asyncio.shield(self._rollback())
            else:
                with suppress(asyncio.CancelledError):
                    await asyncio.shield(self._commit())

    def transaction(self, exclusive=True):
        return StorageUWSJob.StorageUWSJobTransaction(self, exclusive)


class StorageUWSJobPool(UWSJobPool):
    def __init__(self, space_id, storage_id, db_pool, dsn, permission):
        super().__init__(space_id, db_pool, permission)
        self.storage_id = storage_id
        self.listener = None
        self.dsn = dsn
        self.node_db = NodeDatabase(space_id, db_pool, permission)

    async def setup(self):
        self.listener = await asyncpg.connect(dsn=self.dsn)
        await self.listener.add_listener('uws_jobs', self._jobs_callback)

    async def close(self):
        await self.listener.close()
        await super().close()

    def _jobs_callback(self, connection, pid, channel, payload):
        job = json.loads(payload)
        # check the job belongs to this space
        if int(job['row']['space_id']) != self.space_id:
            return
        if job['action'] != 'UPDATE':
            return
        phase = job['row']['phase']
        job_id = job['row']['id']
        if phase == UWSPhase.Aborted:
            loop = asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(self.executor.abort(job_id), loop)

    def _resultset_to_storage_job(self, result):
        job_info = Transfer.fromstring(result['job_info'])
        transfer = Transfer.fromstring(result['transfer'])
        job = StorageUWSJob(self, result['id'], result['phase'], result['destruction'], job_info, transfer)
        job.node_path_modified = result['node_path_modified']
        job.owner = result['owner']
        return job

    async def _set_storage_and_busy(self, path, conn):
        await conn.fetchrow("update nodes set storage_id=$1, busy=True "
                            "where space_id=$2 and path=$3",
                            self.storage_id, self.space_id, path)

    async def _set_not_busy(self, path):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.fetchrow("update nodes set busy=False "
                                    "where space_id=$1 and path=$2",
                                    self.space_id, path)

    async def _execute(self, job, func, *args):
        return await func(job, *args)

    async def execute(self, job_id, identity, func, *args):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                job_result = await self._get_uws_job_conn(conn=conn, job_id=job_id, for_update=True)
                # Can only start an EXECUTING Job if its a protocol transfer
                if job_result['phase'] != UWSPhase.Executing:
                    raise InvalidJobStateError('Invalid Job State')

                job = self._resultset_to_storage_job(job_result)
                if not isinstance(job.job_info, ProtocolTransfer):
                    raise InvalidJobStateError('Invalid Job Type')

                if not await self.permission.permits(identity, 'runJob', context=job):
                    raise PermissionDenied('runJob denied.')

                try:
                    node_results = await conn.fetch("select *, nlevel(path) from nodes "
                                                    "where path <@ $1 and space_id=$2 "
                                                    "order by nlevel(path) asc for update nowait;",
                                                    job_result['node_path'], self.space_id)
                except asyncpg.exceptions.LockNotAvailableError:
                    raise NodeBusyError(f"Path: {NodeDatabase.ltree_to_path(job_result['node_path'])}")

                await busy_fuzz()

                if not node_results:
                    raise NodeDoesNotExistError("target node does not exist.")

                # The first entry should always be the target path in the job
                if node_results[0]['path'] != job_result['node_path']:
                    raise InvalidJobError(f"{node_results[0]['path']} does not match target "
                                          f"{job_result['node_path']}")

                if node_results[0]['path_modified'] != job_result['node_path_modified']:
                    raise NodeDoesNotExistError('target has been modified.')

                node_properties = await conn.fetch("select * from properties "
                                                   "where node_path=any($1::ltree[]) and space_id=$2",
                                                   [node['path'] for node in node_results], self.space_id)

                root_node = NodeDatabase.resultset_to_node_tree(node_results, node_properties)
                job.transfer.target = root_node
                if not await self.permission.permits(identity, 'dataTransfer', context=job):
                    raise PermissionDenied('data transfer denied.')

                fut = self.executor.execute(job, self._execute, func, *args)

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

