import datetime
import asyncio
import functools

from contextlib import suppress
from collections import namedtuple

from .exception import VOSpaceError, ClosingError, InvalidJobStateError


UWS_Phase = namedtuple('NodeType', 'Pending '
                                   'Queued '
                                   'Executing '
                                   'Completed '
                                   'Error '
                                   'Aborted '
                                   'Unknown '
                                   'Held '
                                   'Suspended '
                                   'Archived')

UWSPhase = UWS_Phase(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

PhaseLookup = {0: 'PENDING',
               1: 'QUEUED',
               2: 'EXECUTING',
               3: 'COMPLETED',
               4: 'ERROR',
               5: 'ABORTED',
               6: 'UNKNOWN',
               7: 'HELD',
               8: 'SUSPENDED',
               9: 'ARCHIVED'}

UWSKey = namedtuple('UWSKey', 'space_id job_id')


def generate_uws_error(errors):
    if not errors:
        return ''

    return f"<uws:errorSummary><uws:message>{errors}</uws:message></uws:errorSummary>"


def generate_uws_job_xml(job_id,
                         phase,
                         destruction,
                         job_info,
                         results=None,
                         errors=None):
    xml = f'<?xml version="1.0" encoding="UTF-8"?>' \
          f'<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0" ' \
          f'xmlns:xs="http://www.w3.org/2001/XMLSchema-instance" ' \
          f'xmlns:xlink="http://www.w3.org/1999/xlink">' \
          f'<uws:jobId>{job_id}</uws:jobId><uws:ownerId xs:nill="true"/>' \
          f'<uws:phase>{PhaseLookup[phase]}</uws:phase><uws:quote></uws:quote>' \
          f'<uws:startTime xmlns:xs="http://www.w3.org/2001/XMLSchema-instance" xs:nil="true"/>' \
          f'<uws:endTime xmlns:xs="http://www.w3.org/2001/XMLSchema-instance" xs:nil="true"/>' \
          f'<uws:executionDuration>43200</uws:executionDuration>' \
          f'<uws:destruction>{destruction}</uws:destruction><uws:parameters/>' \
          f'{results if results else "<uws:results/>"}' \
          f'{generate_uws_error(errors)}' \
          f'<uws:jobInfo>{job_info}</uws:jobInfo></uws:job>'
    return xml


class UWSJobExecutor(object):
    def __init__(self):
        self.job_tasks = {}
        self._closing = False

    @property
    def closing(self):
        return self._closing

    def execute(self, func, key, *args):
        if self._closing:
            return ClosingError()

        # if task is already running then ignore
        if self.job_tasks.get(key, None):
            return

        task = asyncio.ensure_future(func(key, *args))
        self.job_tasks[key] = (task, *args)
        task.add_done_callback(functools.partial(self._done, key))
        return task

    def _done(self, key, task):
        with suppress(Exception):
            task.exception()
        del self.job_tasks[key]

    async def abort(self, key):
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


async def create_uws_job(app, conn, job_info, phase=UWSPhase.Pending):
    space_id = app['space_id']
    destruction = datetime.datetime.utcnow() + datetime.timedelta(seconds=3000)
    result = await conn.fetchrow("insert into uws_jobs (phase, destruction, job_info, space_id) "
                                 "values ($1, $2, $3, $4) returning id, space_id",
                                 phase, destruction, job_info, space_id)
    return UWSKey(result['space_id'], result['id'])


async def update_uws_job(conn, space_id, job_id, target, direction,
                         transfer, result, phase=UWSPhase.Pending):
    path_array = list(filter(None, target.split('/')))
    path_tree = '.'.join(path_array)
    # only update job if its pending
    result = await conn.fetchrow("update uws_jobs set target=$1, target_id=$8, "
                                 "direction=$2, phase=$3, transfer=$4, result=$5 where id=$6 "
                                 "and phase <= $7 and space_id=$8 returning id",
                                 path_tree, direction, phase,
                                 transfer, result, job_id,
                                 UWSPhase.Executing, space_id)
    if not result:
        return None
    return result['id']


async def get_uws_job_phase(db_pool, space_id, job_id):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            result = await conn.fetchrow("select phase, target from uws_jobs "
                                         "where id=$1 and space_id=$2", job_id, space_id)
            if not result:
                raise VOSpaceError(404, f"Invalid Request. UWS job {job_id} does not exist.")
            return result


async def get_uws_job(db_pool, space_id, job_id):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            return await get_uws_job_conn(conn, space_id, job_id)


async def get_uws_job_conn(conn, space_id, job_id, for_update=False):
    try:
        if for_update:
            result = await conn.fetchrow("select * from uws_jobs "
                                         "where id=$1 and space_id=$2 for update",
                                         job_id, space_id)
        else:
            result = await conn.fetchrow("select * from uws_jobs "
                                         "where id=$1 and space_id=$2", job_id, space_id)
        if not result:
            raise VOSpaceError(404, f"Invalid Request. UWS job {job_id} does not exist.")

        return result
    except ValueError as e:
        raise VOSpaceError(400, f"Invalid Request. Invalid JobId: {str(e)}")


async def set_uws_phase_to_executing(db_pool, space_id, job_id):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            return await conn.fetchrow("update uws_jobs set phase=$2 "
                                       "where id=$1 and phase=$3 and space_id=$4 returning id",
                                       job_id, UWSPhase.Executing, UWSPhase.Pending, space_id)


async def set_uws_phase_to_completed(db_pool, space_id, job_id):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            return await conn.fetchrow("update uws_jobs set phase=$2 "
                                       "where id=$1 and phase=$3 and space_id=$4 returning id",
                                       job_id, UWSPhase.Completed, UWSPhase.Executing, space_id)


async def set_uws_phase_to_error(db_pool, space_id, job_id, error):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            result = await conn.fetchrow("update uws_jobs set phase=$3, error=$2 "
                                         "where id=$1 and phase!=$4 and space_id=$5 returning id",
                                         job_id, error, UWSPhase.Error, UWSPhase.Aborted, space_id)
            return result


async def set_uws_phase_to_abort(db_pool, space_id, job_id):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            result = await conn.fetchrow("update uws_jobs set phase=$2 "
                                         "where id=$1 and phase!=$3 and space_id=$4 returning id",
                                         job_id, UWSPhase.Aborted, UWSPhase.Error, space_id)
            return result
