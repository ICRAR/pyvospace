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

import io
import sys
import asyncio
import json
import aiofiles
import aiofiles.os
import aiohttp
import logging
import unittest
import configparser
import xml.etree.ElementTree as ET
import requests
import socket
import tarfile

from aiohttp import web
from urllib.parse import urlencode
from passlib.hash import pbkdf2_sha256

from pyvospace.server.spaces.posix import PosixSpaceServer
from pyvospace.core.model import *


class TestBase(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(stream=sys.stderr)
        logging.getLogger('test').setLevel(logging.DEBUG)
        self.log = logging.getLogger('test')
        self.session = None
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

        self.config_filename = 'test_vo.ini'
        config = configparser.ConfigParser()
        if not os.path.exists(self.config_filename):
            storage_details = json.dumps(
                {'root_dir': '/tmp/posix/storage/',
                 'staging_dir': '/tmp/posix/staging/'})

            config['Space'] = {'host': 'localhost',
                               'port': 8080,
                               'name': 'posix',
                               'uri': 'icrar.org',
                               'dsn': 'postgres://vos_user:vos_user@localhost:5435/vospace',
                               'parameters': '{}',
                               'secret_key': 'ZlmNyXdQgRhhrC2Wwy-gLZj7Wv6ZtoKH',
                               'domain': '',
                               'use_ssl': 0
                               }

            config['Storage'] = {'name': 'posix',
                                 'host': 'localhost',
                                 'port': 8081,
                                 'parameters': storage_details,
                                 'use_ssl': 0
                                }

            with open(self.config_filename, 'w') as conf:
                config.write(conf)

        self.app = self.loop.run_until_complete(PosixSpaceServer.create(self.config_filename))
        self.runner = web.AppRunner(self.app)
        self.loop.run_until_complete(self.runner.setup())
        if not hasattr(socket, 'SO_REUSEPORT'):
            reuse_port = False
        else:
            reuse_port = True
        site = web.TCPSite(self.runner, 'localhost', 8080,
                           reuse_address=True, reuse_port=reuse_port)
        self.loop.run_until_complete(site.start())

        user = ['test', pbkdf2_sha256.hash('test'), [], [], 'posix', True]
        self.loop.run_until_complete(self.create_user(self.app['db_pool'], *user))
        self.session = self.loop.run_until_complete(self._login('test', 'test'))

    def tearDown(self):
        if self.session:
            self.loop.run_until_complete(self._logout())
            self.loop.run_until_complete(self.session.close())

        self.loop.run_until_complete(self.runner.shutdown())
        self.loop.run_until_complete(self.runner.cleanup())
        self.loop.close()

    async def create_user(self, db_pool, username, password, group_read, group_write, space_name, admin):
        async with db_pool.acquire() as conn:
            await conn.fetchrow("insert into users (username, password, groupread, "
                                "groupwrite, space_name, admin) values($1, $2, $3, $4, $5, $6) "
                                "on conflict (username, space_name) do nothing",
                                username, password, group_read, group_write, space_name, admin)


    async def create_tar(self, file_name):
        try:
            await aiofiles.os.stat(file_name)
        except FileNotFoundError:
            if not os.path.exists('/tmp/tar'):
                os.makedirs('/tmp/tar')
            if not os.path.exists('/tmp/tar/dir1/dir2'):
                os.makedirs('/tmp/tar/dir1/dir2')
            await self.create_file('/tmp/tar/test1', blocksize=2)
            await self.create_file('/tmp/tar/test2', blocksize=64)
            await self.create_file('/tmp/tar/test3', blocksize=1024)
            await self.create_file('/tmp/tar/dir1/test1', blocksize=2048)
            await self.create_file('/tmp/tar/dir1/dir2/test2', blocksize=128)
            with tarfile.open(file_name, "w") as tar:
                    tar.add('/tmp/tar')

    async def create_file(self, file_name, blocksize=1024):
        try:
            await aiofiles.os.stat(file_name)
        except FileNotFoundError:
            async with aiofiles.open(file_name, mode='wb') as f:
                await f.truncate(blocksize*io.DEFAULT_BUFFER_SIZE)

    async def file_sender(self, file_name=None):
        async with aiofiles.open(file_name, 'rb') as f:
            chunk = await f.read(64 * 1024)
            while chunk:
                yield chunk
                chunk = await f.read(64 * 1024)

    async def _login(self, username, password):
        session = aiohttp.ClientSession(auth=aiohttp.BasicAuth(username, password))
        async with session.post(f'http://localhost:8080/login') as resp:
            response = await resp.text()
            self.assertEqual(resp.status, 200, msg=response)
        return session

    async def _logout(self):
        async with self.session.post(f'http://localhost:8080/logout') as resp:
            response = await resp.text()
            self.assertEqual(resp.status, 200, msg=response)

    async def post(self, url, **kwargs):
        #async with aiohttp.ClientSession() as session:
        async with self.session.post(url, **kwargs) as resp:
            return resp.status, await resp.text()

    async def delete(self, url):
        #async with aiohttp.ClientSession() as session:
        async with self.session.delete(url) as resp:
            return resp.status, await resp.text()

    async def put(self, url, **kwargs):
        #async with aiohttp.ClientSession() as session:
        async with self.session.put(url, **kwargs) as resp:
            return resp.status, await resp.text()

    async def get(self, url, **kwargs):
        #async with aiohttp.ClientSession() as session:
        async with self.session.get(url, **kwargs) as resp:
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

    async def set_node_properties(self, node, expected_status=200):
        status, response = await self.post(f'http://localhost:8080/vospace/nodes/{node.path}', data=node.tostring())
        self.assertEqual(expected_status, status, msg=response)
        return response

    async def transfer_node(self, transfer):
        status, response = await self.post('http://localhost:8080/vospace/transfers', data=transfer.tostring())
        self.assertEqual(200, status, msg=response)
        return UWSJob.fromstring(response)

    async def sync_transfer_node(self, transfer, expected_status=200):
        status, response = await self.post('http://localhost:8080/vospace/synctrans', data=transfer.tostring())
        self.assertEqual(expected_status, status, msg=response)
        if status == 200:
            return Transfer.fromstring(response)
        return response

    async def sync_transfer_node_parameterised(self, transfer, expected_status=200):
        status, response = await self.post('http://localhost:8080/vospace/synctrans',
                                           params=transfer.tomap())
        self.assertEqual(expected_status, status, msg=response)
        if status == 200:
            return Transfer.fromstring(response)
        return response

    async def sync_pull_from_space_parameterised(self, transfer, output_path, expected_status=(200,)):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.download_file_product, transfer, output_path)

    async def delete_node(self, node):
        return await self.delete(f'http://localhost:8080/vospace/nodes/{node.path}')

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
        if status == 200:
            return Transfer.fromstring(response)
        return None

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
        job = UWSJob.fromstring(response)
        self.assertIsNotNone(job.error)
        self.assertTrue(error_contains in job.error, msg=job.error)

    async def push_to_space(self, url, file_path, expected_status=200):
        async with aiohttp.ClientSession(cookie_jar=self.session.cookie_jar) as session:
            async with session.put(url, data=self.file_sender(file_name=file_path)) as resp:
                response = await resp.text()
                self.assertEqual(resp.status, expected_status, msg=response)

    async def push_to_space_defer_error(self, url, file_path):
        async with aiohttp.ClientSession(cookie_jar=self.session.cookie_jar) as session:
            async with session.put(url, data=self.file_sender(file_name=file_path)) as resp:
                response = await resp.text()
                return resp.status, response

    async def pull_from_space(self, url, output_path, expected_status=(200,)):
        async with aiohttp.ClientSession(cookie_jar=self.session.cookie_jar) as session:
            async with session.get(url) as resp:
                self.assertIn(resp.status, expected_status)
                if resp.status == 200:
                    hdr_length = int(resp.headers[aiohttp.hdrs.CONTENT_LENGTH])
                    path = f"{output_path}/{resp.content_disposition.filename}"
                    downloaded = 0
                    try:
                        async with aiofiles.open(path, mode='wb') as out_file:
                            while downloaded < hdr_length:
                                buff = await resp.content.read(65536)
                                if not buff:
                                    break
                                await out_file.write(buff)
                                downloaded += len(buff)
                        self.assertEqual(hdr_length, downloaded, f"Header: {hdr_length} != Recv: {downloaded}")
                    except Exception as e:
                        raise IOError(str(e))

    async def pull_from_space_defer_error(self, url, output_path):
        async with aiohttp.ClientSession(cookie_jar=self.session.cookie_jar) as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    hdr_length = int(resp.headers[aiohttp.hdrs.CONTENT_LENGTH])
                    path = f"{output_path}/{resp.content_disposition.filename}"
                    downloaded = 0
                    try:
                        async with aiofiles.open(path, mode='wb') as out_file:
                            while downloaded < hdr_length:
                                buff = await resp.content.read(65536)
                                if not buff:
                                    break
                                await out_file.write(buff)
                                downloaded += len(buff)
                        self.assertEqual(hdr_length, downloaded, f"Header: {hdr_length} != Recv: {downloaded}")
                        return resp.status
                    except Exception as e:
                        raise IOError(str(e))

    def download_file_product(self, transfer, output_path):
        t = self.session.cookie_jar.filter_cookies('http://localhost:8080/')
        cookies = {}
        for _, value in t.items():
            cookies[value.key] = value.value
        with requests.post(url='http://localhost:8080/vospace/synctrans',
                           params=urlencode(transfer.tomap()),
                           cookies=cookies,
                           stream=True,
                           verify=False) as r:
            r.raise_for_status()
            path = f"{output_path}/test"
            with open(path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)

            file_size = os.path.getsize(path)
            content_length = int(r.headers['content-length'])
            if file_size != content_length:
                raise IOError('size mismatch')