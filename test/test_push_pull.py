import os
import io
import sys
import asyncio
import aiofiles
import aiofiles.os
import logging
import unittest
import aiohttp
import configparser
import xml.etree.ElementTree as ET

from aiohttp import web

from pyvospace.core.model import PushToSpace, Node, HTTPPut
from pyvospace.server.vospace import VOSpaceServer
from pyvospace.server.plugins.posix.posix_server import PosixFileServer

logging.basicConfig(stream=sys.stderr)
logging.getLogger('test').setLevel(logging.DEBUG)
log= logging.getLogger('test')


class TestCreate(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

        config_filename = 'test_vo.ini'
        config = configparser.ConfigParser()
        if not os.path.exists(config_filename):
            config['Database'] = {'dsn': 'postgres://test:test@localhost:5432/vos'}
            config['Plugin'] = {'path': '', 'name': 'posix'}
            config['PosixPlugin'] = {'host': 'localhost',
                                     'port': 8081,
                                     'root_dir': '/tmp/store/'}
            config.write(open(config_filename, 'w'))

        self.loop.run_until_complete(self._setup())

        posix_server = self.loop.run_until_complete(PosixFileServer.create(config_filename))
        self.posix_runner = web.AppRunner(posix_server)
        self.loop.run_until_complete(self.posix_runner.setup())
        posix_site = web.TCPSite(self.posix_runner, 'localhost', 8081)
        self.loop.run_until_complete(posix_site.start())

        self.app = self.loop.run_until_complete(VOSpaceServer.create(config_filename))
        self.runner = web.AppRunner(self.app)
        self.loop.run_until_complete(self.runner.setup())
        site = web.TCPSite(self.runner, 'localhost', 8080)
        self.loop.run_until_complete(site.start())

    async def _setup(self):
        await self.create_file('/tmp/datafile.dat')


    def tearDown(self):
        #self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/datanode'))
        self.loop.run_until_complete(self.posix_runner.cleanup())
        self.loop.run_until_complete(self.runner.cleanup())
        self.loop.close()

    async def delete(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.delete(url) as resp:
                return resp

    async def put(self, url, **kwargs):
        async with aiohttp.ClientSession() as session:
            async with session.put(url, **kwargs) as resp:
                return resp.status, await resp.text()

    async def post(self, url, xml):
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=xml) as resp:
                return resp.status, await resp.text()

    async def get(self, url, params):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                return resp.status, await resp.text()

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

    def test_push_to_space(self):
        async def run():
            node = Node('vos://icrar.org!vospace/datanode/')
            push = PushToSpace(node, [HTTPPut()])
            status, response = await self.post('http://localhost:8080/vospace/transfers',
                                               push.tostring())
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            state = 'PHASE=RUN'
            status, response = await self.post(f'http://localhost:8080/vospace/'
                                               f'transfers/{job_id.text}/phase', state)
            self.assertEqual(200, status, msg=response)

            while True:
                status, response = await self.get(f'http://localhost:8080/vospace/'
                                                  f'transfers/{job_id.text}/phase', state)
                self.assertEqual(200, status, msg=response)
                if response == 'EXECUTING' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('EXECUTING', response)

            # Check results
            status, response = await self.get(f'http://localhost:8080/vospace/'
                                              f'transfers/{job_id.text}', None)
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            results = root.find('{http://www.ivoa.net/xml/UWS/v1.0}results')
            for result in results:
                if result.attrib['id'] == 'transferDetails':
                    break

            trans_url = f'http://localhost:8080{result.attrib["{http://www.w3.org/1999/xlink}href"]}'

            # Get transferDetails
            status, response = await self.get(trans_url, None)
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