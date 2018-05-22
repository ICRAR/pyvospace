import datetime
import asyncio
import functools
import copy

from contextlib import suppress
from collections import namedtuple
import lxml.etree as ET

from pyvospace.core.model import *
from pyvospace.core.exception import *
from .database import NodeDatabase

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


class UWSJobPool(object):
    def __init__(self, space_id, db_pool):
        self.db_pool = db_pool
        self.space_id = space_id
        self.executor = UWSJobExecutor(space_id)
        self.node_db = NodeDatabase(space_id, db_pool)

    async def close(self):
        await self.executor.close()

    async def get_uws_job_phase(self, job_id):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.fetchrow("select phase from uws_jobs "
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
        transfer = None
        if result['transfer']:
            transfer = Transfer.fromstring(result['transfer'])
        return UWSJob(result['id'], result['phase'], result['destruction'],
                      job_info, results, result['error'], transfer)

    async def get(self, job_id):
        async with self.db_pool.acquire() as conn:
            result = await self._get_uws_job_conn(conn=conn, job_id=job_id)
        return self._resultset_to_job(result)

    async def create(self, job_info, phase=UWSPhase.Pending):
        job_info_string = job_info.tostring()
        destruction = datetime.datetime.utcnow() + datetime.timedelta(seconds=3000)
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.fetchrow("insert into uws_jobs (phase, destruction, job_info, space_id) "
                                             "values ($1, $2, $3, $4) returning *",
                                             phase, destruction, job_info_string, self.space_id)
        return self._resultset_to_job(result)

    async def execute(self, job_id, func, *args):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                result = await self._get_uws_job_conn(conn=conn, job_id=job_id, for_update=True)
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


class StorageUWSJobPool(UWSJobPool):
    def __init__(self, space_id, db_pool):
        super().__init__(space_id, db_pool)

    async def execute(self, job_id, func, *args):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                result = await self._get_uws_job_conn(conn=conn, job_id=job_id, for_update=True)
                job = self._resultset_to_job(result)
                if not isinstance(job.job_info, ProtocolTransfer):
                    raise InvalidJobStateError('Invalid Job Type')
                # Can only start an EXECUTING Job if its a protocol transfer
                if job.phase != UWSPhase.Executing:
                    raise InvalidJobStateError('Invalid Job State')
                fut = self.executor.execute(job, func, *args)
        return await fut


class UWSResult(object):
    def __init__(self, id, attrs):
        self.id = id
        self.attrs = attrs

    def toxml(self, root):
        result = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}result')
        result.set('id', self.id)
        for key, value in self.attrs.items():
            result.set(key, value)

    @classmethod
    def fromstring(cls, xml):
        result_set = []
        if not xml:
            return result_set

        root = ET.fromstring(xml)
        for result in root.xpath('/uws:results/uws:result', namespaces=UWSJob.NS):
            id = result.attrib.get('id', None)
            assert id
            del result.attrib['id']
            result_set.append(UWSResult(id, result.attrib))
        return result_set


class UWSJob(object):

    NS = {'uws': 'http://www.ivoa.net/xml/UWS/v1.0',
          'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
          'xlink': 'http://www.w3.org/1999/xlink'}

    def __init__(self, job_id, phase, destruction, job_info, results=None, error=None, transfer=None):
        self.job_id = str(job_id)
        self.phase = phase
        self.destruction = destruction
        self.job_info = job_info
        self._results = None
        self.results = results
        self.error = error
        self.transfer = transfer

    @property
    def results(self):
        return self._results

    @results.setter
    def results(self, value):
        if value:
            assert isinstance(value, list) is True
            for result in value:
                assert isinstance(result, UWSResult) is True
                self._results = copy.deepcopy(value)

    def toxml(self):
        root = ET.Element("{http://www.ivoa.net/xml/UWS/v1.0}job", nsmap=UWSJob.NS)
        ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}jobId').text = self.job_id
        owner_element = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}ownerId')
        owner_element.set('{http://www.w3.org/2001/XMLSchema-instance}nil', 'true')
        ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}phase').text = PhaseLookup[self.phase]
        ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}quote').text = None
        starttime_element = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}startTime')
        starttime_element.set('{http://www.w3.org/2001/XMLSchema-instance}nil', 'true')
        endtime_element = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}endTime')
        endtime_element.set('{http://www.w3.org/2001/XMLSchema-instance}nil', 'true')
        ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}executionDuration').text = str(0)
        ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}destruction').text = str(self.destruction)
        if self.results:
            results = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}results')
            for result in results:
                result.toxml(results)
        if self.error:
            error_summary = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}errorSummary')
            ET.SubElement(error_summary, '{http://www.ivoa.net/xml/UWS/v1.0}message').text = self.error
        return root

    def results_tostring(self):
        if not self.results:
            return None
        results_elem = ET.Element('{http://www.ivoa.net/xml/UWS/v1.0}results', nsmap=UWSJob.NS)
        for result in self.results:
            result.toxml(results_elem)
        return ET.tostring(results_elem).decode("utf-8")

    def tostring(self):
        root = self.toxml()
        return ET.tostring(root).decode("utf-8")

    @classmethod
    def fromstring(cls, xml):
        root = ET.fromstring(xml)
        root_elem = root.xpath('/uws:job/uws:jobId', namespace=UWSJob.NS)
        assert root_elem
        assert root_elem.text
        phase_elem = root.xpath('/uws:job/uws:phase', namespace=UWSJob.NS)
        assert phase_elem
        assert phase_elem.text
        destruction_elem = root.xpath('/uws:job/uws:destruction', namespace=UWSJob.NS)
        assert destruction_elem
        assert destruction_elem.text
        for result in root.xpath('/uws:job/uws:results/uws:result', namespace=UWSJob.NS):
            pass


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

    async def abort(self, job):
        key = (job.job_id, self.space_id)
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

