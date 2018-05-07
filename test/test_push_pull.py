import unittest
import asyncio
import xml.etree.ElementTree as ET

import xml.dom.minidom as minidom

from aiohttp import web

from pyvospace.core.model import PushToSpace, PullFromSpace, Node, ContainerNode, HTTPPut, HTTPGet
from pyvospace.server.plugins.posix.posix_server import PosixFileServer
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

        posix_server = self.loop.run_until_complete(PosixFileServer.create(self.config_filename))
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

    def ttest_push_to_space_sync(self):
        async def run():
            node = Node('/syncdatanode')
            push = PushToSpace(node, [HTTPPut()])
            status, response = await self.post('http://localhost:8080/vospace/synctrans',
                                               data=push.tostring())
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            prot = root.find('{http://www.ivoa.net/xml/VOSpace/v2.1}protocol')
            end = prot.find('{http://www.ivoa.net/xml/VOSpace/v2.1}endpoint')

            await self.pull_from_space('http://localhost:8081/vospace/download/1234',
                                       '/tmp/download/', expected_status=400)

            try:
                await asyncio.wait_for(self.push_to_space(end.text,
                                                          '/tmp/datafile.dat',
                                                          expected_status=200), 0.2)
            except Exception as e:
                pass

            node = Node('/syncdatanode')
            push = PullFromSpace(node, [HTTPGet()])
            status, response = await self.post('http://localhost:8080/vospace/synctrans',
                                               data=push.tostring())
            self.assertEqual(200, status, msg=response)
            root = ET.fromstring(response)
            prot = root.find('{http://www.ivoa.net/xml/VOSpace/v2.1}protocol')
            end = prot.find('{http://www.ivoa.net/xml/VOSpace/v2.1}endpoint')

            await self.pull_from_space(end.text, '/tmp/download/', expected_status=500)

        self.loop.run_until_complete(run())

    def test_push_to_space(self):
        async def run():
            node1 = ContainerNode('/datanode')
            await self.create_node(node1)

            node1 = ContainerNode('/datanode/datanode1')
            await self.create_node(node1)

            node1 = Node('/datanode/datanode1/datanode2')
            await self.create_node(node1)

            node = Node('/datanode/datanode1/datanode2')
            push = PushToSpace(node, [HTTPPut()])

            response = await self.transfer_node(push)
            job_id = self.get_job_id(response)

            # Job that is not in the correct phase
            # This means that the node is not yet associated with the job.
            # It gets associated when the job is run.
            await self.push_to_space(f'http://localhost:8081/vospace/upload/{job_id}',
                                     '/tmp/datafile.dat', expected_status=400)

            await self.get_job_details(job_id)

            # Get transfer details, should be in invalid state because its not Executing
            await self.get_transfer_details(job_id, expected_status=400)

            await self.delete('http://localhost:8080/vospace/nodes/datanode')

            await self.change_job_state(job_id)

            await self.poll_job(job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='ERROR')

            # Check results
            response = await self.get_job_details(job_id)
            self.log.debug(prettify(response))
            return
            # Get transferDetails
            response = await self.get_transfer_details(job_id, expected_status=200)

            root = ET.fromstring(response)
            prot = root.find('{http://www.ivoa.net/xml/VOSpace/v2.1}protocol')
            end = prot.find('{http://www.ivoa.net/xml/VOSpace/v2.1}endpoint')

            # badly formed job id
            await self.push_to_space('http://localhost:8081/vospace/upload/1234',
                                     '/tmp/datafile.dat', expected_status=400)

            # job that doesn't exist
            await self.push_to_space('http://localhost:8081/vospace/upload/1324a40b-4c6a-453b-a756-cd41ca4b7408',
                                     '/tmp/datafile.dat', expected_status=404)

            #await self.delete('http://localhost:8080/vospace/nodes/datanode')

            # node should not be found
            await self.push_to_space(end.text, '/tmp/datafile.dat', expected_status=200)


        self.loop.run_until_complete(run())


if __name__ == '__main__':
    unittest.main()