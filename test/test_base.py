import os
import io
import sys
import asyncio
import aiofiles
import aiofiles.os
import aiohttp
import logging
import unittest
import configparser
import xml.etree.ElementTree as ET

from aiohttp import web

from pyvospace.server.vospace import VOSpaceServer
from pyvospace.core.model import Node


class TestBase(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(stream=sys.stderr)
        logging.getLogger('test').setLevel(logging.DEBUG)
        self.log = logging.getLogger('test')

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

        self.config_filename = 'test_vo.ini'
        config = configparser.ConfigParser()
        if not os.path.exists(self.config_filename):
            config['Database'] = {'dsn': 'postgres://test:test@localhost:5432/vos'}
            config['Plugin'] = {'path': '',
                                'name': 'posix'}
            config['PosixPlugin'] = {'host': 'localhost',
                                     'port': 8081,
                                     'root_dir': '/tmp/store',
                                     'processing_dir': '/tmp/processing'}
            config.write(open(self.config_filename, 'w'))

        self.app = self.loop.run_until_complete(VOSpaceServer.create(self.config_filename))
        self.runner = web.AppRunner(self.app)
        self.loop.run_until_complete(self.runner.setup())
        site = web.TCPSite(self.runner, 'localhost', 8080,
                           reuse_address=True, reuse_port=True)
        self.loop.run_until_complete(site.start())

    def tearDown(self):
        self.loop.run_until_complete(self.runner.shutdown())
        self.loop.run_until_complete(self.runner.cleanup())
        self.loop.close()

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

    async def post(self, url, **kwargs):
        async with aiohttp.ClientSession() as session:
            async with session.post(url, **kwargs) as resp:
                return resp.status, await resp.text()

    async def delete(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.delete(url) as resp:
                return resp.status, await resp.text()

    async def put(self, url, **kwargs):
        async with aiohttp.ClientSession() as session:
            async with session.put(url, **kwargs) as resp:
                return resp.status, await resp.text()

    async def get(self, url, **kwargs):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, **kwargs) as resp:
                return resp.status, await resp.text()

    async def create_node(self, node, expected_status=201):
        status, response = await self.put(f'http://localhost:8080/vospace/nodes/{node.path}',
                                          data=node.tostring())
        self.assertEqual(status, expected_status, msg=response)
        return response

    async def get_node(self, path, params, expected_status=200):
        status, response = await self.get(f'http://localhost:8080/vospace/nodes/{path}', params=params)
        self.assertEqual(expected_status, status, msg=response)
        if status == 200:
            return Node.fromstring(response)
        return None

    async def set_node_properties(self, path, node, expected_status=200):
        status, response = await self.post(f'http://localhost:8080/vospace/nodes/{path}', data=node.tostring())
        self.assertEqual(expected_status, status, msg=response)

    async def transfer_node(self, transfer):
        status, response = await self.post('http://localhost:8080/vospace/transfers', data=transfer.tostring())
        self.assertEqual(200, status, msg=response)
        return response

    def get_job_id(self, response):
        root = ET.fromstring(response)
        job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
        self.assertIsNotNone(job_id.text)
        return job_id.text

    async def get_job_details(self, job_id):
        status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id}', params=None)
        return response

    def extract_transfer_details(self, response):
        root = ET.fromstring(response)
        results = root.find('{http://www.ivoa.net/xml/UWS/v1.0}results')
        for result in results:
            if result.attrib['id'] == 'transferDetails':
                return f'http://localhost:8080{result.attrib["{http://www.w3.org/1999/xlink}href"]}'
        return None

    async def get_transfer_details(self, job_id, expected_status=200):
        status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id}'
                                          f'/results/transferDetails', params=None)
        self.assertEqual(expected_status, status, msg=response)
        return response

    async def change_job_state(self, job_id, state='PHASE=RUN', expected_status=200):
        status, response = await self.post(f'http://localhost:8080/vospace/transfers/{job_id}/phase',
                                           data=state)
        self.assertEqual(status, expected_status, msg=response)
        return status, response

    async def poll_job(self, job_id, poll_until=('COMPLETED', 'ERROR'), expected_status='COMPLETED'):
        while True:
            status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id}/phase')
            self.assertEqual(200, status, msg=response)
            if response in poll_until:
                break
            await asyncio.sleep(0.1)
        self.assertEqual(response, expected_status, msg=response)

    async def get_error_summary(self, job_id, error_contains):
        status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id}/error',
                                          params=None)
        self.assertEqual(200, status, msg=response)
        root = ET.fromstring(response)
        error = root.find('{http://www.ivoa.net/xml/UWS/v1.0}errorSummary')
        self.assertIsNotNone(error)
        self.assertTrue(error_contains in error[0].text, msg=error[0].text)

    async def push_to_space(self, url, file_path, expected_status=200):
        status, response = await self.put(url, data=self.file_sender(file_name=file_path))
        self.assertEqual(status, expected_status, msg=response)

    async def pull_from_space(self, url, output_path, expected_status=200):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                self.assertEqual(resp.status, expected_status)
                if resp.status == 200:
                    hdr_length = int(resp.headers[aiohttp.hdrs.CONTENT_LENGTH])
                    path = f"{output_path}/{resp.content_disposition.filename}"
                    downloaded = 0
                    async with aiofiles.open(path, mode='wb') as out_file:
                        while True:
                            buff = await resp.content.read(65536)
                            downloaded += len(buff)
                            if not buff:
                                break
                            await out_file.write(buff)
                    self.assertEqual(hdr_length, downloaded, f"Header: {hdr_length} != Recv: {downloaded}")
