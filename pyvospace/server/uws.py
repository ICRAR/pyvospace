import datetime
import asyncio

from collections import namedtuple

from .exception import VOSpaceError


class InvalidUWSState(Exception):
    pass


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


def results_dict_list_to_xml(results):
    if not results:
        return '<uws:results/>'

    attr_vals = []
    for attr in results:
        attr_vals.append(f'<uws:result id="{attr["id"]}" '
                         f'xlink:href="{attr["xlink:href"]}"/>')

    return f"<uws:results>{''.join(attr_vals)}</uws:results>"


def generate_uws_error(errors):
    if not errors:
        return ''

    return f"<uws:errorSummary><uws:message>{errors}" \
           f"</uws:message></uws:errorSummary>"


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
          f'<uws:jobId>{job_id}</uws:jobId>' \
          f'<uws:ownerId xs:nill="true"/>' \
          f'<uws:phase>{PhaseLookup[phase]}</uws:phase>' \
          f'<uws:quote></uws:quote>' \
          f'<uws:startTime xmlns:xs="http://www.w3.org/2001/XMLSchema-instance" xs:nil="true"/>' \
          f'<uws:endTime xmlns:xs="http://www.w3.org/2001/XMLSchema-instance" xs:nil="true"/>' \
          f'<uws:executionDuration>43200</uws:executionDuration>' \
          f'<uws:destruction>{destruction}</uws:destruction>' \
          f'<uws:parameters/>' \
          f'{results if results else "<uws:results/>"}' \
          f'{generate_uws_error(errors)}' \
          f'<uws:jobInfo>{job_info}</uws:jobInfo>' \
          f'</uws:job>'
    return xml


class UWSJobExecutor(object):

    def __init__(self):
        self.job_tasks = {}

    def execute(self, func, app, job):
        task = asyncio.ensure_future(self._run(func, app, job))
        self.job_tasks[task] = job
        task.add_done_callback(self._done)

    async def _run(self, func, app, job):
        try:
            await func(app, job['id'], job)

        except VOSpaceError as e:
            await set_uws_phase_to_error(app['db_pool'],
                                         job['id'],
                                         e.error)

    def _done(self, task):
        del self.job_tasks[task]

    async def close(self):
        while True:
            if len(self.job_tasks) == 0:
                return
            await asyncio.sleep(0.1)


async def create_uws_job(conn,
                         target,
                         direction,
                         job_info,
                         extras,
                         phase=UWSPhase.Pending):
    path_array = list(filter(None, target.split('/')))
    path_tree = '.'.join(path_array)

    destruction = datetime.datetime.utcnow() + datetime.timedelta(seconds=3000)
    result = await conn.fetchrow("insert into uws_jobs (target, direction, "
                                 "phase, destruction, job_info, extras) "
                                 "values ($1, $2, $3, $4, $5, $6) returning id",
                                 path_tree,
                                 direction,
                                 phase,
                                 destruction,
                                 job_info,
                                 extras)
    return result['id']


async def get_uws_job(db_pool, job_id):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            return await get_uws_job_conn(conn, job_id)


async def get_uws_job_conn(conn, job_id):
    try:
        result = await conn.fetchrow("select * from uws_jobs "
                                     "where id=$1 for share",
                                     job_id)
        if not result:
            raise VOSpaceError(400, f"Invalid Request. "
                                    f"UWS job {job_id} does not exist.")

        return result
    except ValueError as e:
        raise VOSpaceError(400, f"Invalid jobId: {str(e)}")


async def set_uws_phase_to_executing(db_pool, job_id, extras=None):
    async with db_pool.acquire() as conn:
        async with conn.transaction():

            if extras:
                return await conn.fetchrow("update uws_jobs set phase=$2, extras=$4 "
                                           "where id=$1 and phase=$3 returning id",
                                           job_id,
                                           UWSPhase.Executing,
                                           UWSPhase.Pending,
                                           extras)

            return await conn.fetchrow("update uws_jobs set phase=$2 "
                                       "where id=$1 and phase=$3 returning id",
                                       job_id,
                                       UWSPhase.Executing,
                                       UWSPhase.Pending)


async def set_uws_phase_to_completed(db_pool, job_id):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            return await conn.fetchrow("update uws_jobs set phase=$2 "
                                       "where id=$1 and phase=$3 returning id",
                                       job_id,
                                       UWSPhase.Completed,
                                       UWSPhase.Executing)


async def set_uws_phase_to_error(db_pool, job_id, error):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            return await conn.fetchrow("update uws_jobs set phase=$2, error=$3"
                                       "where id=$1 returning id",
                                       job_id,
                                       UWSPhase.Error,
                                       error)