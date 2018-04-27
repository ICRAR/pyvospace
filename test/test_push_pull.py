import io
import asyncio
import aiofiles
import aiofiles.os
import unittest
import xml.etree.ElementTree as ET

from aiohttp import web

from pyvospace.core.model import PushToSpace, Node, HTTPPut
from pyvospace.server.plugins.posix.posix_server import PosixFileServer
from test.test_base import TestBase


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
        #self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/datanode'))
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

    def test_push_to_space_sync(self):
        async def run():
            node = Node('vos://icrar.org!vospace/datanode/')
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
            node = Node('vos://icrar.org!vospace/datanode/')
            push = PushToSpace(node, [HTTPPut()])
            status, response = await self.post('http://localhost:8080/vospace/transfers',
                                               data=push.tostring())
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            state = 'PHASE=RUN'
            status, response = await self.post(f'http://localhost:8080/vospace/'
                                               f'transfers/{job_id.text}/phase',
                                               data=state)
            self.assertEqual(200, status, msg=response)

            while True:
                status, response = await self.get(f'http://localhost:8080/vospace/'
                                                  f'transfers/{job_id.text}/phase',
                                                  params=state)
                self.assertEqual(200, status, msg=response)
                if response == 'EXECUTING' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('EXECUTING', response)

            # Check results
            status, response = await self.get(f'http://localhost:8080/vospace/'
                                              f'transfers/{job_id.text}',
                                              params=None)
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            results = root.find('{http://www.ivoa.net/xml/UWS/v1.0}results')
            for result in results:
                if result.attrib['id'] == 'transferDetails':
                    break

            trans_url = f'http://localhost:8080{result.attrib["{http://www.w3.org/1999/xlink}href"]}'

            # Get transferDetails
            status, response = await self.get(trans_url, params=None)
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            prot = root.find('{http://www.ivoa.net/xml/VOSpace/v2.1}protocol')
            end = prot.find('{http://www.ivoa.net/xml/VOSpace/v2.1}endpoint')

            status, response = await self.put(end.text,
                                              data=self.file_sender(file_name='/tmp/datafile.dat'))
            self.assertEqual(200, status, msg=response)

        self.loop.run_until_complete(run())


if __name__ == '__main__':
    unittest.main()