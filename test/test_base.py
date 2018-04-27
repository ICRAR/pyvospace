import os
import sys
import asyncio
import aiohttp
import logging
import unittest
import configparser

from aiohttp import web

from pyvospace.server.vospace import VOSpaceServer


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
                                     'port': 8081}
            config.write(open(self.config_filename, 'w'))

        app = self.loop.run_until_complete(VOSpaceServer.create(self.config_filename))
        self.runner = web.AppRunner(app)
        self.loop.run_until_complete(self.runner.setup())
        site = web.TCPSite(self.runner, 'localhost', 8080,
                           reuse_address=True, reuse_port=True)
        self.loop.run_until_complete(site.start())

    def tearDown(self):
        self.loop.run_until_complete(self.runner.cleanup())
        self.loop.close()

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