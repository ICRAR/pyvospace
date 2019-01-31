#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2018
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA

import unittest
import asyncio

from aiohttp import web

from pyvospace.core.model import *
from pyvospace.server import set_fuzz, set_busy_fuzz
from pyvospace.server.spaces.ngas.storage.ngas_storage import NGASStorageServer
from test.test_base import TestBase


class TestPushPull(TestBase):

    def setUp(self):
        super().setUp()
        self.loop.run_until_complete(self._setup())
        ngas_server = self.loop.run_until_complete(NGASStorageServer.create(self.config_filename))

        self.ngas_runner = web.AppRunner(ngas_server)
        self.loop.run_until_complete(self.ngas_runner.setup())
        self.ngas_site = web.TCPSite(self.ngas_runner, 'localhost', 8081)
        self.loop.run_until_complete(self.ngas_site.start())

    async def _setup(self):
        if not os.path.exists('/tmp/download'):
            os.makedirs('/tmp/download')
        await self.create_file('/tmp/datafile.dat')
        await self.create_tar('/tmp/mytar.tar.gz')

    def tearDown(self):
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/root'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/datanode'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/syncdatanode'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/syncdatanode1.fits'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/root/mytar.tar.gz'))
        self.loop.run_until_complete(self.ngas_runner.shutdown())
        self.loop.run_until_complete(self.ngas_runner.cleanup())
        super().tearDown()

    def test_push_to_node(self):
        async def run():


        self.loop.run_until_complete(run())


    # def test_push_to_container(self):
    #     async def run():
    #         root_node = ContainerNode('/root')
    #         await self.create_node(root_node)
    #
    #         node = DataNode('/root/mytar.tar.gz',
    #                         properties=[Property('ivo://ivoa.net/vospace/core#title', "mytar.tar.gz", True)])
    #         await self.create_node(node)
    #
    #         security_method = SecurityMethod('ivo://ivoa.net/sso#cookie')
    #
    #         # push tar to node
    #         container_push = PushToSpace(node, [HTTPPut(security_method=security_method)],
    #                                      view=View('ivo://ivoa.net/vospace/core#tar'),
    #                                      params=[Parameter("ivo://ivoa.net/vospace/core#length", 1234)])
    #
    #         transfer = await self.sync_transfer_node(container_push)
    #         put_end = transfer.protocols[0].endpoint.url
    #         await self.push_to_space(put_end, '/tmp/mytar.tar.gz', expected_status=200)
    #
    #         # push to container node
    #         container_push = PushToSpace(root_node, [HTTPPut(security_method=security_method)],
    #                                      view=View('ivo://ivoa.net/vospace/core#tar'),
    #                                      params=[Parameter("ivo://ivoa.net/vospace/core#length", 1234)])
    #
    #         transfer = await self.sync_transfer_node(container_push)
    #         put_end = transfer.protocols[0].endpoint.url
    #         await self.push_to_space(put_end, '/tmp/mytar.tar.gz', expected_status=200)
    #
    #         pull = PullFromSpace(root_node, [HTTPGet()], view=View('ivo://ivoa.net/vospace/core#tar'))
    #         transfer = await self.sync_transfer_node(pull)
    #         pull_end = transfer.protocols[0].endpoint.url
    #         await self.pull_from_space(pull_end, '/tmp/download/')
    #
    #         pull = PullFromSpace(node, [HTTPGet()], view=View('ivo://ivoa.net/vospace/core#tar'))
    #         transfer = await self.sync_transfer_node(pull)
    #         pull_end = transfer.protocols[0].endpoint.url
    #         await self.pull_from_space(pull_end, '/tmp/download/')
    #
    #     self.loop.run_until_complete(run())

    # def test_push_to_space_sync_node_delete(self):
    #     async def run():
    #         node = Node('/syncdatanode')
    #         push = PushToSpace(node, [HTTPPut()])
    #         set_fuzz(True)
    #
    #         async def defer_delete(node):
    #             await asyncio.sleep(0.5)
    #             await self.delete_node(node)
    #
    #         tasks = [
    #             asyncio.ensure_future(self.sync_transfer_node(push, 200)),
    #             asyncio.ensure_future(defer_delete(node))
    #         ]
    #
    #         await asyncio.gather(*tasks)
    #         set_fuzz(False)
    #
    #     self.loop.run_until_complete(run())
    #
    # def test_push_to_space_sync(self):
    #     async def run():
    #         container_node = ContainerNode('/syncdatanode',
    #                                        properties=[Property('ivo://ivoa.net/vospace/core#title',
    #                                                             "syncdatanode", True)])
    #         await self.create_node(container_node)
    #
    #         node = DataNode('/syncdatanode/syncdatanode1.fits',
    #                         properties=[Property('ivo://ivoa.net/vospace/core#title', "syncdatanode1.fits", True),
    #                                     Property('ivo://ivoa.net/vospace/core#contributor', "dave", True)])
    #
    #         await self.create_node(node)
    #
    #         # push to container node
    #         container_push = PushToSpace(container_node, [HTTPPut()],
    #                                      params=[Parameter("ivo://ivoa.net/vospace/core#length", 1234)])
    #
    #         transfer = await self.sync_transfer_node(container_push)
    #         put_end = transfer.protocols[0].endpoint.url
    #         await self.push_to_space(put_end, '/tmp/datafile.dat', expected_status=400)
    #
    #         # push to leaf node
    #         push = PushToSpace(node, [HTTPPut()],
    #                            params=[Parameter("ivo://ivoa.net/vospace/core#length", 1234)])
    #
    #         transfer = await self.sync_transfer_node(push)
    #         put_end = transfer.protocols[0].endpoint.url
    #         await self.push_to_space(put_end, '/tmp/datafile.dat', expected_status=200)
    #
    #         # retrieve leaf data
    #         push = PullFromSpace(node, [HTTPGet()])
    #         transfer = await self.sync_transfer_node(push)
    #         pull_end = transfer.protocols[0].endpoint.url
    #         await self.pull_from_space(pull_end, '/tmp/download/')
    #
    #     self.loop.run_until_complete(run())
    #
    # def test_push_to_space_sync_parameterised(self):
    #     async def run():
    #         node = Node('/syncdatanode1.fits')
    #         push = PushToSpace(node, [HTTPPut()])
    #         transfer = await self.sync_transfer_node_parameterised(push)
    #         put_end = transfer.protocols[0].endpoint.url
    #         await self.push_to_space(put_end, '/tmp/datafile.dat', expected_status=200)
    #
    #         pull = PullFromSpace(node, [HTTPGet()])
    #         await self.sync_pull_from_space_parameterised(pull, '/tmp/download/')
    #
    #     self.loop.run_until_complete(run())
    #
    # def test_push_to_space_async(self):
    #     async def run():
    #         node1 = ContainerNode('/datanode')
    #         await self.create_node(node1)
    #
    #         node1 = ContainerNode('/datanode/datanode1')
    #         await self.create_node(node1)
    #
    #         node = Node('/datanode/datanode1/datanode2')
    #         push = PushToSpace(node, [HTTPPut()])
    #
    #         job = await self.transfer_node(push)
    #         # Job that is not in the correct phase
    #         # This means that the node is not yet associated with the job.
    #         # It gets associated when the job is run.
    #         await self.push_to_space(f'http://localhost:8081/vospace/pushToVoSpace/{job.job_id}',
    #                                  '/tmp/datafile.dat', expected_status=400)
    #
    #         # Get transfer details, should be in invalid state because its not Executing
    #         await self.get_transfer_details(job.job_id, expected_status=400)
    #         await self.change_job_state(job.job_id)
    #         await self.poll_job(job.job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='EXECUTING')
    #
    #         # Get transferDetails
    #         transfer = await self.get_transfer_details(job.job_id, expected_status=200)
    #         end = transfer.protocols[0].endpoint.url
    #
    #         # badly formed job id
    #         await self.push_to_space('http://localhost:8081/vospace/pushToVoSpace/1234',
    #                                  '/tmp/datafile.dat', expected_status=400)
    #
    #         # job that doesn't exist
    #         await self.push_to_space('http://localhost:8081/vospace/pushToVoSpace/1324a40b-4c6a-453b-a756-cd41ca4b7408',
    #                                  '/tmp/datafile.dat', expected_status=404)
    #
    #         tasks = [
    #             asyncio.ensure_future(self.push_to_space_defer_error(end, '/tmp/datafile.dat')),
    #             asyncio.ensure_future(self.push_to_space_defer_error(end, '/tmp/datafile.dat'))
    #         ]
    #
    #         result = []
    #         finished, unfinished = await asyncio.wait(tasks)
    #         self.assertEqual(len(finished), 2)
    #         for i in finished:
    #             result.append((await i)[0])
    #
    #         self.assertIn(200, result)
    #         self.assertIn(400, result)
    #
    #     self.loop.run_until_complete(run())
    #
    # def test_push_to_space_async_error(self):
    #     async def run():
    #         # can not push data to container node
    #         node1 = ContainerNode('/datanode',
    #                               properties=[Property('ivo://ivoa.net/vospace/core#title', "datanode", True)])
    #         await self.create_node(node1)
    #
    #         leaf = ContainerNode('/datanode/datanode1',
    #                              properties=[Property('ivo://ivoa.net/vospace/core#title', "datanode", True)])
    #         await self.create_node(leaf)
    #
    #         push = PushToSpace(node1, [HTTPPut()])
    #         job = await self.transfer_node(push)
    #         await self.change_job_state(job.job_id)
    #         await self.poll_job(job.job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='EXECUTING')
    #
    #         transfer = await self.get_transfer_details(job.job_id, expected_status=200)
    #         end = transfer.protocols[0].endpoint.url
    #         await self.push_to_space(end, '/tmp/datafile.dat', expected_status=400)
    #
    #         await self.delete('http://localhost:8080/vospace/nodes/datanode')
    #
    #         # can not push data to linknode
    #         node1 = LinkNode('/datanode', 'http://google.com')
    #         await self.create_node(node1)
    #
    #         push = PushToSpace(node1, [HTTPPut()])
    #         job = await self.transfer_node(push)
    #         await self.change_job_state(job.job_id)
    #         await self.poll_job(job.job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='ERROR')
    #
    #         await self.delete('http://localhost:8080/vospace/nodes/datanode')
    #
    #         # delete node before job execute
    #         node1 = Node('/datanode')
    #         await self.create_node(node1)
    #
    #         push = PushToSpace(node1, [HTTPPut()])
    #         job = await self.transfer_node(push)
    #         await self.change_job_state(job.job_id)
    #         await self.poll_job(job.job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='EXECUTING')
    #         await self.delete('http://localhost:8080/vospace/nodes/datanode')
    #
    #         transfer = await self.get_transfer_details(job.job_id, expected_status=200)
    #         end = transfer.protocols[0].endpoint.url
    #         await self.push_to_space(end, '/tmp/datafile.dat', expected_status=404)
    #
    #     self.loop.run_until_complete(run())
    #
    # def test_push_to_space_concurrent(self):
    #     async def run():
    #         node1 = ContainerNode('/datanode')
    #         await self.create_node(node1)
    #
    #         node1 = ContainerNode('/datanode/datanode1')
    #         await self.create_node(node1)
    #
    #         node1 = Node('/datanode/datanode1/datanode2.dat')
    #         await self.create_node(node1)
    #
    #         node = Node('/datanode/datanode1/datanode2.dat')
    #         push = PushToSpace(node, [HTTPPut()])
    #
    #         job = await self.transfer_node(push)
    #         await self.change_job_state(job.job_id)
    #         await self.poll_job(job.job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='EXECUTING')
    #
    #         transfer = await self.get_transfer_details(job.job_id, expected_status=200)
    #         end1 = transfer.protocols[0].endpoint.url
    #
    #         # start job to upload to same node
    #         job = await self.transfer_node(push)
    #         await self.change_job_state(job.job_id)
    #         await self.poll_job(job.job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='EXECUTING')
    #
    #         transfer = await self.get_transfer_details(job.job_id, expected_status=200)
    #         end2 = transfer.protocols[0].endpoint.url
    #
    #         set_fuzz(True)
    #
    #         # concurrent upload
    #         tasks = [
    #             asyncio.ensure_future(self.push_to_space_defer_error(end1, '/tmp/datafile.dat')),
    #             asyncio.ensure_future(self.push_to_space_defer_error(end2, '/tmp/datafile.dat'))
    #         ]
    #
    #         set_fuzz(False)
    #
    #         result = []
    #         finished, unfinished = await asyncio.wait(tasks)
    #         self.assertEqual(len(finished), 2)
    #         for i in finished:
    #             r = await i
    #             result.append(r[0])
    #
    #         self.assertIn(200, result)
    #         self.assertIn(400, result)
    #
    #     self.loop.run_until_complete(run())


if __name__ == '__main__':
    unittest.main()