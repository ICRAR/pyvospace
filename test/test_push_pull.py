import io
import asyncio
import aiofiles
import aiofiles.os
import unittest
import xml.etree.ElementTree as ET

import xml.dom.minidom as minidom

from aiohttp import web

from pyvospace.core.model import PushToSpace, Node, ContainerNode, HTTPPut
from pyvospace.server.plugins.posix.posix_server import PosixFileServer
from test.test_base import TestBase


def prettify(elem):
    """Return a pretty-printed XML string.
    """
    reparsed = minidom.parseString(elem)
    return reparsed.toprettyxml(indent="\t")


class TestCreate(TestBase):

    def setUp(self):

        super().setUp()

        self.loop.run_until_complete(self._setup())

        posix_server = self.loop.run_until_complete(PosixFileServer.create(self.config_filename))
        self.posix_runner = web.AppRunner(posix_server)
        self.loop.run_until_complete(self.posix_runner.setup())
        posix_site = web.TCPSite(self.posix_runner, 'localhost', 8081)
        self.loop.run_until_complete(posix_site.start())

    async def _setup(self):
        await self.create_file('/tmp/datafile.dat')

    def tearDown(self):
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/datanode'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/syncdatanode'))
        self.loop.run_until_complete(self.posix_runner.cleanup())

        super().tearDown()

    async def create_file(self, file_name):
        try:
            await aiofiles.os.stat(file_name)
        except FileNotFoundError:
            async with aiofiles.open(file_name, mode='wb') as f:
                await f.truncate(1024*io.DEFAULT_BUFFER_SIZE)

    async def file_sender(self, file_name=None):
        async with aiofiles.open(file_name, 'rb') as f:
            chunk = await f.read(64 * 1024)
            while chunk:
                yield chunk
                chunk = await f.read(64 * 1024)

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

            status, response = await self.put(end.text,
                                              data=self.file_sender(file_name='/tmp/datafile.dat'))
            self.assertEqual(200, status, msg=response)

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

            await self.get_job_details(job_id)

            # Get transfer details, should be in invalid state because its not Executing
            await self.get_transfer_details(job_id, expected_status=400)

            await self.change_job_state(job_id)
            await self.poll_job(job_id, poll_until=('EXECUTING', 'ERROR'), expected_status='EXECUTING')

            # Check results
            await self.get_job_details(job_id)

            # Get transferDetails
            response = await self.get_transfer_details(job_id, expected_status=200)

            root = ET.fromstring(response)
            prot = root.find('{http://www.ivoa.net/xml/VOSpace/v2.1}protocol')
            end = prot.find('{http://www.ivoa.net/xml/VOSpace/v2.1}endpoint')

            status, response = await self.put(end.text,
                                              data=self.file_sender(file_name='/tmp/datafile.dat'))
            self.assertEqual(200, status, msg=response)

        self.loop.run_until_complete(run())


if __name__ == '__main__':
    unittest.main()