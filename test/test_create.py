import os
import asyncio
import unittest
import aiohttp
import configparser
import xml.etree.ElementTree as ET

from xml.etree.ElementTree import tostring

from aiohttp import web

from pyvospace.client.model import Node, ContainerNode, LinkNode, Property
from pyvospace.server.vospace import VOSpaceServer


class TestCreate(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

        config_filename = 'test_vo.ini'
        config = configparser.ConfigParser()
        if not os.path.exists(config_filename):
            config['Database'] = {'dsn': 'postgres://test:test@localhost:5432/vos'}
            config.write(open(config_filename, 'w'))

        app = self.loop.run_until_complete(VOSpaceServer.create(config_filename))
        self.runner = web.AppRunner(app)
        self.loop.run_until_complete(self.runner.setup())
        site = web.TCPSite(self.runner, 'localhost', 8080)
        self.loop.run_until_complete(site.start())

    def tearDown(self):
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/test1'))

        self.loop.run_until_complete(self.runner.cleanup())

    async def delete(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.delete(url) as resp:
                return resp

    async def put(self, url, xml):
        async with aiohttp.ClientSession() as session:
            async with session.put(url, data=xml) as resp:
                return resp

    async def get(self, url, params):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                return resp

    def test_create_node_fail(self):
        async def run():
            # XML Parse Error
            resp = await self.put('http://localhost:8080/vospace/nodes/', '<test><test>')
            self.assertEqual(500, resp.status, msg=await resp.text())

            # Empty Path
            node = Node('vos://example.com!vospace/')
            resp = await self.put('http://localhost:8080/vospace/nodes/', str(node))
            self.assertEqual(400, resp.status, msg=await resp.text())

            # URI name in node does not match URL path
            node = Node('vos://example.com!vospace/data1')
            resp = await self.put('http://localhost:8080/vospace/nodes/data', str(node))
            self.assertEqual(400, resp.status, msg=await resp.text())

            # Invalid attribute
            node = Node('vos://example.com!vospace/data1')
            root = ET.fromstring(str(node))
            root.attrib.pop('uri')
            xml = tostring(root)
            resp = await self.put('http://localhost:8080/vospace/nodes/data1', xml)
            self.assertEqual(400, resp.status, msg=await resp.text())

            # Invalid node type
            node = Node('vos://example.com!vospace/data1')
            root = ET.fromstring(str(node))
            root.attrib['{http://www.w3.org/2001/XMLSchema}type'] = 'vos:unknown'
            xml = tostring(root)
            resp = await self.put('http://localhost:8080/vospace/nodes/data1', xml)
            self.assertEqual(400, resp.status, msg=await resp.text())

        self.loop.run_until_complete(run())

    def test_create_node(self):
        async def run():
            properties = [Property('ivo://ivoa.net/vospace/core#title', "Test", True),
                          Property('ivo://ivoa.net/vospace/core#description', "Hello", True)]
            node = ContainerNode('vos://example.com!vospace/test1', properties)
            resp = await self.put('http://localhost:8080/vospace/nodes/test1', str(node))
            self.assertEqual(201, resp.status, msg=await resp.text())

            node1 = ContainerNode('vos://example.com!vospace/test1/test2')
            resp = await self.put('http://localhost:8080/vospace/nodes/test1/test2', str(node1))
            self.assertEqual(201, resp.status, msg=await resp.text())

            # Duplicate Node
            resp = await self.put('http://localhost:8080/vospace/nodes/test1/test2', str(node1))
            self.assertEqual(409, resp.status, msg=await resp.text())

            # Link Node
            node2 = LinkNode('vos://example.com!vospace/test1/test2/test3', 'http://google.com')
            resp = await self.put('http://localhost:8080/vospace/nodes/test1/test2/test3', str(node2))
            self.assertEqual(201, resp.status, msg=await resp.text())

            # Check that Link Node is in Path
            node3 = Node('vos://example.com!vospace/test1/test2/test3/test4')
            resp = await self.put('http://localhost:8080/vospace/nodes/test1/test2/test3/test4', str(node3))
            self.assertEqual(400, resp.status, msg=await resp.text())

            params = {'uri': 'vos://example.com!vospace/mydir/file3401', 'detail': 'min'}
            resp = await self.get('http://localhost:8080/vospace/nodes/test1/', params)
            self.assertEqual(200, resp.status, msg=await resp.text())

        self.loop.run_until_complete(run())

if __name__ == '__main__':
    unittest.main()