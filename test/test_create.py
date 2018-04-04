import os
import sys
import asyncio
import logging
import unittest
import aiohttp
import configparser
import xml.etree.ElementTree as ET

from xml.etree.ElementTree import tostring

from aiohttp import web

from pyvospace.client.model import Node, ContainerNode, LinkNode, Property
from pyvospace.server.vospace import VOSpaceServer


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
            config.write(open(config_filename, 'w'))

        app = self.loop.run_until_complete(VOSpaceServer.create(config_filename))
        self.runner = web.AppRunner(app)
        self.loop.run_until_complete(self.runner.setup())
        site = web.TCPSite(self.runner, 'localhost', 8080)
        self.loop.run_until_complete(site.start())

    def tearDown(self):
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/test1'))
        self.loop.run_until_complete(self.runner.cleanup())
        self.loop.close()

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
            node = Node('vos://icrar.org!vospace/')
            resp = await self.put('http://localhost:8080/vospace/nodes/', node.tostring())
            self.assertEqual(400, resp.status, msg=await resp.text())

            # URI name in node does not match URL path
            node = Node('vos://icrar.org!vospace/data1')
            resp = await self.put('http://localhost:8080/vospace/nodes/data', node.tostring())
            self.assertEqual(400, resp.status, msg=await resp.text())

            # Invalid attribute
            node = Node('vos://icrar.org!vospace/data1')
            root = ET.fromstring(node.tostring())
            root.attrib.pop('uri')
            xml = tostring(root)
            resp = await self.put('http://localhost:8080/vospace/nodes/data1', xml)
            self.assertEqual(400, resp.status, msg=await resp.text())

            # Invalid node type
            node = Node('vos://icrar.org!vospace/data1')
            root = ET.fromstring(node.tostring())
            root.attrib['{http://www.w3.org/2001/XMLSchema-instance}type'] = 'vos:unknown'
            xml = tostring(root)
            resp = await self.put('http://localhost:8080/vospace/nodes/data1', xml)
            self.assertEqual(400, resp.status, msg=await resp.text())

        self.loop.run_until_complete(run())

    def test_create_node(self):
        async def run():
            # Invalid Path
            node1 = ContainerNode('vos://icrar.org!vospace/')
            resp = await self.put('http://localhost:8080/vospace/nodes/', node1.tostring())
            self.assertEqual(400, resp.status, msg=await resp.text())

            properties = [Property('ivo://ivoa.net/vospace/core#title', "Test", True),
                          Property('ivo://ivoa.net/vospace/core#description', "Hello", True)]
            node = ContainerNode('vos://icrar.org!vospace/test1', properties=properties)
            resp = await self.put('http://localhost:8080/vospace/nodes/test1', node.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            node1 = ContainerNode('vos://icrar.org!vospace/test1/test2')
            resp = await self.put('http://localhost:8080/vospace/nodes/test1/test2', node1.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            node2 = Node('vos://icrar.org!vospace/test1/data')
            resp = await self.put('http://localhost:8080/vospace/nodes/test1/data', node2.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            # Duplicate Node
            resp = await self.put('http://localhost:8080/vospace/nodes/test1/test2', node1.tostring())
            self.assertEqual(409, resp.status, msg=await resp.text())

            # Link Node
            node2 = LinkNode('vos://icrar.org!vospace/test1/test2/test3', 'http://google.com')
            resp = await self.put('http://localhost:8080/vospace/nodes/test1/test2/test3', node2.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            # Check that Link Node is in Path
            node3 = Node('vos://icrar.org!vospace/test1/test2/test3/test4')
            resp = await self.put('http://localhost:8080/vospace/nodes/test1/test2/test3/test4', node3.tostring())
            self.assertEqual(400, resp.status, msg=await resp.text())

            # Test failure cases of getNode
            params = {'detail': 'max'}
            resp = await self.get('http://localhost:8080/vospace/nodes/', params)
            response = await resp.text()
            self.assertEqual(400, resp.status, msg=response)

            # Node not found
            params = {'detail': 'max'}
            resp = await self.get('http://localhost:8080/vospace/nodes/mynode', params)
            response = await resp.text()
            self.assertEqual(404, resp.status, msg=response)

            # Invalid parameters
            params = {'detail': 'invalid'}
            resp = await self.get('http://localhost:8080/vospace/nodes/test1', params)
            response = await resp.text()
            self.assertEqual(400, resp.status, msg=response)

            params = {'limit': -1}
            resp = await self.get('http://localhost:8080/vospace/nodes/test1', params)
            response = await resp.text()
            self.assertEqual(400, resp.status, msg=response)

            params = {'limit': "string"}
            resp = await self.get('http://localhost:8080/vospace/nodes/test1', params)
            response = await resp.text()
            self.assertEqual(400, resp.status, msg=response)

            # get only one child of the container node, other should not be there.
            # Should be in alphabetical order
            params = {'detail': 'max', 'limit': 1}
            resp = await self.get('http://localhost:8080/vospace/nodes/test1', params)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            node = Node.fromstring(response)
            cmp_node = ContainerNode('vos://icrar.org!vospace/test1',
                                     properties=properties,
                                     nodes=[Node('vos://icrar.org!vospace/test1/data')])
            self.assertEqual(node, cmp_node)

            params = {'detail': 'min'}
            resp = await self.get('http://localhost:8080/vospace/nodes/test1', params)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            # detail = min should not have properties
            node = Node.fromstring(response)
            cmp_node = ContainerNode('vos://icrar.org!vospace/test1',
                                     properties=properties,
                                     nodes=[Node('vos://icrar.org!vospace/test1/data'),
                                            ContainerNode('vos://icrar.org!vospace/test1/test2')])
            self.assertNotEqual(node, cmp_node)

            cmp_node = ContainerNode('vos://icrar.org!vospace/test1',
                                     nodes=[Node('vos://icrar.org!vospace/test1/data'),
                                            ContainerNode('vos://icrar.org!vospace/test1/test2')])
            self.assertEqual(node, cmp_node)


        self.loop.run_until_complete(run())

if __name__ == '__main__':
    unittest.main()