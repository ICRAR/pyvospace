import unittest
import asyncio
import xml.etree.ElementTree as ET

from aiohttp import web

from pyvospace.core.model import *
from pyvospace.server import set_fuzz
from pyvospace.server.spaces.posix.storage.posix_storage import PosixStorageServer
from test.test_base import TestBase


class TestPushPull(TestBase):

    def setUp(self):
        super().setUp()
        self.loop.run_until_complete(self._setup())
        posix_server = self.loop.run_until_complete(PosixStorageServer.create(self.config_filename))

        self.posix_runner = web.AppRunner(posix_server)
        self.loop.run_until_complete(self.posix_runner.setup())
        self.posix_site = web.TCPSite(self.posix_runner, 'localhost', 8081)
        self.loop.run_until_complete(self.posix_site.start())

    async def _setup(self):
        await self.create_file('/tmp/datafile.dat')

    def tearDown(self):
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/datanode'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/syncdatanode'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/syncdatanode1'))
        self.loop.run_until_complete(self.posix_runner.shutdown())
        self.loop.run_until_complete(self.posix_runner.cleanup())
        super().tearDown()

    def test_push_to_space_sync_node_delete(self):
        async def run():
            node = Node('/syncdatanode')
            push = PushToSpace(node, [HTTPPut()])
            set_fuzz(True)

            async def defer_delete(node):
                await asyncio.sleep(0.5)
                await self.delete_node(node)

            tasks = [
                asyncio.ensure_future(self.sync_transfer_node(push, 404)),
                asyncio.ensure_future(defer_delete(node))
            ]

            await asyncio.gather(*tasks)
            set_fuzz(False)

        self.loop.run_until_complete(run())

    def test_push_to_space_sync(self):
        async def run():
            node = Node('/syncdatanode1')
            push = PushToSpace(node, [HTTPPut()],
                               params=[Parameter("ivo://ivoa.net/vospace/core#length", 1234)])

            transfer = await self.sync_transfer_node(push)
            put_end = transfer.protocols[0].endpoint.url
            await self.push_to_space(put_end, '/tmp/datafile.dat', expected_status=200)

            push = PullFromSpace(node, [HTTPGet()])
            transfer = await self.sync_transfer_node(push)
            pull_end = transfer.protocols[0].endpoint.url
            await self.pull_from_space(pull_end, '/tmp/download/')

        self.loop.run_until_complete(run())

    def test_push_to_space_sync_parameterised(self):
        async def run():
            node = Node('/syncdatanode1')
            push = PushToSpace(node, [HTTPPut()])
            transfer = await self.sync_transfer_node_parameterised(push)
            put_end = transfer.protocols[0].endpoint.url
            await self.push_to_space(put_end, '/tmp/datafile.dat', expected_status=200)

            pull = PullFromSpace(node, [HTTPGet()])
            await self.sync_pull_from_space_parameterised(pull, '/tmp/download/')

        self.loop.run_until_complete(run())

    def test_push_to_space_sync_failed(self):
        async def run():
            node = Node('/syncdatanode')
            push = PushToSpace(node, [HTTPPut()])
            transfer = await self.sync_transfer_node(push)
            put_end = transfer.protocols[0].endpoint.url

            try:
                await asyncio.wait_for(self.push_to_space(put_end,
                                                          '/tmp/datafile.dat',
                                                          expected_status=200), 0.1)
            except Exception as e:
                pass

            node = Node('/syncdatanode')
            push = PullFromSpace(node, [HTTPGet()])
            transfer = await self.sync_transfer_node(push)
            end_get = transfer.protocols[0].endpoint.url
            await self.pull_from_space(end_get, '/tmp/download/', expected_status=(500, 400))

        self.loop.run_until_complete(run())

    def test_push_to_space_async(self):
        async def run():
            node1 = ContainerNode('/datanode')
            await self.create_node(node1)

            node1 = ContainerNode('/datanode/datanode1')
            await self.create_node(node1)

            node = Node('/datanode/datanode1/datanode2')
            push = PushToSpace(node, [HTTPPut()])

            job = await self.transfer_node(push)
            # Job that is not in the correct phase
            # This means that the node is not yet associated with the job.
            # It gets associated when the job is run.
            await self.push_to_space(f'http://localhost:8081/vospace/pushToVoSpace/{job.job_id}',
                                     '/tmp/datafile.dat', expected_status=400)

            # Get transfer details, should be in invalid state because its not Executing
            await self.get_transfer_details(job.job_id, expected_status=400)
            await self.change_job_state(job.job_id)
            await self.poll_job(job.job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='EXECUTING')

            # Get transferDetails
            transfer = await self.get_transfer_details(job.job_id, expected_status=200)
            end = transfer.protocols[0].endpoint.url

            # badly formed job id
            await self.push_to_space('http://localhost:8081/vospace/pushToVoSpace/1234',
                                     '/tmp/datafile.dat', expected_status=400)

            # job that doesn't exist
            await self.push_to_space('http://localhost:8081/vospace/pushToVoSpace/1324a40b-4c6a-453b-a756-cd41ca4b7408',
                                     '/tmp/datafile.dat', expected_status=404)

            tasks = [
                asyncio.ensure_future(self.push_to_space_defer_error(end, '/tmp/datafile.dat')),
                asyncio.ensure_future(self.push_to_space_defer_error(end, '/tmp/datafile.dat'))
            ]

            result = []
            finished, unfinished = await asyncio.wait(tasks)
            self.assertEqual(len(finished), 2)
            for i in finished:
                result.append((await i)[0])

            self.assertIn(200, result)
            self.assertIn(400, result)

        self.loop.run_until_complete(run())

    def test_push_to_space_async_error(self):
        async def run():
            # can not push data to container node
            node1 = ContainerNode('/datanode')
            await self.create_node(node1)

            push = PushToSpace(node1, [HTTPPut()])
            job = await self.transfer_node(push)
            await self.change_job_state(job.job_id)
            await self.poll_job(job.job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='EXECUTING')

            transfer = await self.get_transfer_details(job.job_id, expected_status=200)
            end = transfer.protocols[0].endpoint.url
            await self.push_to_space(end, '/tmp/datafile.dat', expected_status=400)

            await self.delete('http://localhost:8080/vospace/nodes/datanode')

            # can not push data to linknode
            node1 = LinkNode('/datanode', 'http://google.com')
            await self.create_node(node1)

            push = PushToSpace(node1, [HTTPPut()])
            job = await self.transfer_node(push)
            await self.change_job_state(job.job_id)
            await self.poll_job(job.job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='ERROR')

            await self.delete('http://localhost:8080/vospace/nodes/datanode')

            # delete node before job execute
            node1 = Node('/datanode')
            await self.create_node(node1)

            push = PushToSpace(node1, [HTTPPut()])
            job = await self.transfer_node(push)
            await self.change_job_state(job.job_id)
            await self.poll_job(job.job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='EXECUTING')
            await self.delete('http://localhost:8080/vospace/nodes/datanode')

            transfer = await self.get_transfer_details(job.job_id, expected_status=200)
            end = transfer.protocols[0].endpoint.url
            await self.push_to_space(end, '/tmp/datafile.dat', expected_status=404)

        self.loop.run_until_complete(run())

    def test_push_to_space_concurrent(self):
        async def run():
            node1 = ContainerNode('/datanode')
            await self.create_node(node1)

            node1 = ContainerNode('/datanode/datanode1')
            await self.create_node(node1)

            node1 = Node('/datanode/datanode1/datanode2')
            await self.create_node(node1)

            node = Node('/datanode/datanode1/datanode2')
            push = PushToSpace(node, [HTTPPut()])

            job = await self.transfer_node(push)
            await self.change_job_state(job.job_id)
            await self.poll_job(job.job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='EXECUTING')

            transfer = await self.get_transfer_details(job.job_id, expected_status=200)
            end1 = transfer.protocols[0].endpoint.url

            # start job to upload to same node
            job = await self.transfer_node(push)
            await self.change_job_state(job.job_id)
            await self.poll_job(job.job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='EXECUTING')

            transfer = await self.get_transfer_details(job.job_id, expected_status=200)
            end2 = transfer.protocols[0].endpoint.url

            # concurrent upload
            tasks = [
                asyncio.ensure_future(self.push_to_space_defer_error(end1, '/tmp/datafile.dat')),
                asyncio.ensure_future(self.push_to_space_defer_error(end2, '/tmp/datafile.dat'))
            ]

            result = []
            finished, unfinished = await asyncio.wait(tasks)
            self.assertEqual(len(finished), 2)
            for i in finished:
                result.append((await i)[0])

            self.assertIn(200, result)
            self.assertIn(400, result)

        self.loop.run_until_complete(run())


if __name__ == '__main__':
    unittest.main()