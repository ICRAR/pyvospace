import datetime
import asyncio

from collections import namedtuple

from .exception import VOSpaceError


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
               2: 'RUNNING', #VOSpace defines RUNNING not EXECUTING
               3: 'COMPLETED',
               4: 'ERROR',
               5: 'ABORTED',
               6: 'UNKNOWN',
               7: 'HELD',
               8: 'SUSPENDED',
               9: 'ARCHIVED'}


def generate_uws_job_info(job_id, phase, destruction, job_info):
    xml = f'<?xml version = "1.0" encoding = "UTF-8"?>' \
    f'<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink">' \
    f'<uws:jobId>{job_id}</uws:jobId>' \
    f'<uws:ownerId xs:nill="true"/>' \
    f'<uws:phase>{PhaseLookup[phase]}</uws:phase>' \
    f'<uws:quote></uws:quote>' \
    f'<uws:startTime xmlns:xs="http://www.w3.org/2001/XMLSchema-instance" xs:nil="true"/>' \
    f'<uws:endTime xmlns:xs="http://www.w3.org/2001/XMLSchema-instance" xs:nil="true"/>' \
    f'<uws:executionDuration>43200</uws:executionDuration>' \
    f'<uws:destruction>{destruction}</uws:destruction>' \
    f'<uws:parameters/>' \
    f'<uws:results/>' \
    f'<uws:jobInfo>{job_info}</uws:jobInfo>' \
    f'</uws:job>'
    return xml


class UWSJobExecutor(object):

    def __init__(self):
        self.job_tasks = {}

    async def execute(self, func, db_pool, job):
        task = asyncio.ensure_future(self._run(func, db_pool, job))
        self.job_tasks[task] = job
        task.add_done_callback(self._done)

    async def _run(self, func, db_pool, job):
        try:
            await set_uws_phase(db_pool, job['id'], UWSPhase.Executing)
            await func(db_pool, job)

        except VOSpaceError as e:
            await set_uws_error(db_pool, job['id'], e.error)

    def _done(self, task):
        del self.job_tasks[task]


async def create_uws_job(db_pool, job_info):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            destruction = datetime.datetime.utcnow() + datetime.timedelta(seconds=300)
            result = await conn.fetchrow("insert into uws_jobs (phase, destruction, job_info) "
                                         "values ($1, $2, $3) returning id",
                                         UWSPhase.Pending, destruction, job_info)

            xml = generate_uws_job_info(result['id'],
                                        UWSPhase.Pending,
                                        destruction,
                                        job_info)
            return xml


async def get_uws_job(db_pool, job_id):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            result = await conn.fetchrow("select * from uws_jobs where id=$1",
                                         job_id)
            if not result:
                raise VOSpaceError(400, f"Invalid Request. "
                                        f"UWS job {job_id} does not exist.")

            return result


async def set_uws_phase(db_pool, job_id, phase):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("update uws_jobs set phase=$1 where id=$2",
                                phase, job_id)


async def set_uws_error(db_pool, job_id, error):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"update uws_jobs set phase={UWSPhase.Error}, error=$1 where id=$2",
                                error, job_id)