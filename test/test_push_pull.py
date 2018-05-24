import unittest
import asyncio
import xml.etree.ElementTree as ET

import xml.dom.minidom as minidom

from aiohttp import web

from pyvospace.core.model import *
from pyvospace.server.spaces.posix.posix_storage import PosixStorageServer
from test.test_base import TestBase


def prettify(elem):
    """Return a pretty-printed XML string.
    """
    reparsed = minidom.parseString(elem)
    return reparsed.toprettyxml(indent="\t")


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
        self.loop.run_until_complete(self.posix_runner.shutdown())
        self.loop.run_until_complete(self.posix_runner.cleanup())

        super().tearDown()

    def test_push_to_space_sync_node_error(self):
        async def run():
            node = Node('/syncdatanode')
            push = PushToSpace(node, [HTTPPut()])
            status, response = await self.post('http://localhost:8080/vospace/synctrans',
                                               data=push.tostring())
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            put_prot = root.find('{http://www.ivoa.net/xml/VOSpace/v2.1}protocol')
            put_end = put_prot.find('{http://www.ivoa.net/xml/VOSpace/v2.1}endpoint')

            await self.delete('http://localhost:8080/vospace/nodes/syncdatanode')
            await self.push_to_space(put_end.text, '/tmp/datafile.dat', expected_status=404)
            #await self.push_to_space(put_end.text, '/tmp/datafile.dat', expected_status=200)

        self.loop.run_until_complete(run())

    def test_push_to_space_sync_failed(self):
        async def run():
            node = Node('/syncdatanode')
            push = PushToSpace(node, [HTTPPut()])
            status, response = await self.post('http://localhost:8080/vospace/synctrans',
                                               data=push.tostring())
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            put_prot = root.find('{http://www.ivoa.net/xml/VOSpace/v2.1}protocol')
            put_end = put_prot.find('{http://www.ivoa.net/xml/VOSpace/v2.1}endpoint')

            try:
                await asyncio.wait_for(self.push_to_space(put_end.text,
                                                          '/tmp/datafile.dat',
                                                          expected_status=200), 0.1)
            except Exception as e:
                pass

            node = Node('/syncdatanode')
            push = PullFromSpace(node, [HTTPGet()])
            status, response = await self.post('http://localhost:8080/vospace/synctrans',
                                               data=push.tostring())
            self.assertEqual(200, status, msg=response)
            root = ET.fromstring(response)
            prot_get = root.find('{http://www.ivoa.net/xml/VOSpace/v2.1}protocol')
            end_get = prot_get.find('{http://www.ivoa.net/xml/VOSpace/v2.1}endpoint')

            await self.pull_from_space(end_get.text, '/tmp/download/', expected_status=(500, 400))

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