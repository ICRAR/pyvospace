import os
import sys
import asyncio
import logging
import unittest
import aiohttp
import configparser
import xml.etree.ElementTree as ET

from aiohttp import web

from pyvospace.core.model import Node, ContainerNode, Property, DeleteProperty, Move, Copy
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
            config['StoragePlugin'] = {'path': '', 'name': 'posix'}
            config.write(open(config_filename, 'w'))

        app = self.loop.run_until_complete(VOSpaceServer.create(config_filename))
        self.runner = web.AppRunner(app)
        self.loop.run_until_complete(self.runner.setup())
        site = web.TCPSite(self.runner, 'localhost', 8080)
        self.loop.run_until_complete(site.start())

    def tearDown(self):
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/root1'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/root2'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/root3'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/root4'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/data0'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/data1'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/data2'))
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

    async def post(self, url, xml):
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=xml) as resp:
                return resp

    async def get(self, url, params):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                return resp

    def test_set_properties(self):
        async def run():
            properties = [Property('ivo://ivoa.net/vospace/core#title', "Hello1"),
                          Property('ivo://ivoa.net/vospace/core#description', "Hello2")]
            node1 = ContainerNode('vos://icrar.org!vospace/test1', properties=properties)
            resp = await self.put('http://localhost:8080/vospace/nodes/test1', node1.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            # Set properties
            properties = [Property('ivo://ivoa.net/vospace/core#title', "NewTitle"),
                          DeleteProperty('ivo://ivoa.net/vospace/core#description')]
            node1 = ContainerNode('vos://icrar.org!vospace/test1', properties=properties)
            resp = await self.post('http://localhost:8080/vospace/nodes/test1', node1.tostring())
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            params = {'detail': 'max'}
            resp = await self.get('http://localhost:8080/vospace/nodes/test1', params)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            node = Node.fromstring(response)
            prop = [Property('ivo://ivoa.net/vospace/core#title', "NewTitle")]
            orig_node = ContainerNode('vos://icrar.org!vospace/test1',
                                      properties=prop)

            self.assertEqual(node, orig_node)

        self.loop.run_until_complete(run())

    def test_move_node(self):
        async def run():
            root1 = ContainerNode('vos://icrar.org!vospace/root1')
            resp = await self.put('http://localhost:8080/vospace/nodes/root1', root1.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            root2 = ContainerNode('vos://icrar.org!vospace/root2')
            resp = await self.put('http://localhost:8080/vospace/nodes/root2', root2.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            properties = [Property('ivo://ivoa.net/vospace/core#title', "Test1", True),
                          Property('ivo://ivoa.net/vospace/core#description', "Test2", True)]
            node1 = ContainerNode('vos://icrar.org!vospace/root1/test2', properties=properties)
            resp = await self.put('http://localhost:8080/vospace/nodes/root1/test2', node1.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            node2 = ContainerNode('vos://icrar.org!vospace/root1/test2/test3')
            resp = await self.put('http://localhost:8080/vospace/nodes/root1/test2/test3', node2.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            node3 = ContainerNode('vos://icrar.org!vospace/root1/test2/test4')
            resp = await self.put('http://localhost:8080/vospace/nodes/root1/test2/test4', node3.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            # Check consistency of tree
            params = {'detail': 'max'}
            resp = await self.get('http://localhost:8080/vospace/nodes/root1/test2', params)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            node = Node.fromstring(response)
            orig_node = ContainerNode('vos://icrar.org!vospace/root1/test2',
                                      properties=properties,
                                      nodes=[ContainerNode('vos://icrar.org!vospace/root1/test2/test4'),
                                             ContainerNode('vos://icrar.org!vospace/root1/test2/test3')])
            self.assertEqual(node, orig_node)

            params = {'detail': 'max'}
            resp = await self.get('http://localhost:8080/vospace/nodes/root2', params)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            node = Node.fromstring(response)
            orig_node = ContainerNode('vos://icrar.org!vospace/root2')
            self.assertEqual(node, orig_node)

            # Move tree from node1 to root2
            mv = Move(node1, root2)
            resp = await self.post('http://localhost:8080/vospace/transfers', mv.tostring())
            uws_response = await resp.text()
            self.assertEqual(200, resp.status, msg=uws_response)

            root = ET.fromstring(uws_response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            state = 'PHASE=RUN'
            resp = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            while True:
                resp = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
                response = await resp.text()
                self.assertEqual(200, resp.status, msg=response)
                if response == 'COMPLETED' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('COMPLETED', response)

            # Check tree has been moved from node1 to root2
            params = {'detail': 'max'}
            resp = await self.get('http://localhost:8080/vospace/nodes/root2/test2', params)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            node = Node.fromstring(response)
            moved_node = ContainerNode('vos://icrar.org!vospace/root2/test2',
                                       properties=properties,
                                       nodes=[ContainerNode('vos://icrar.org!vospace/root2/test2/test4'),
                                              ContainerNode('vos://icrar.org!vospace/root2/test2/test3')])
            self.assertEqual(node, moved_node)

            params = {'detail': 'max'}
            resp = await self.get('http://localhost:8080/vospace/nodes/root1', params)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            node = Node.fromstring(response)
            orig_node = ContainerNode('vos://icrar.org!vospace/root1')
            self.assertEqual(node, orig_node)

        self.loop.run_until_complete(run())

    def test_invalid_copy_move(self):
        async def run():
            node0 = Node('vos://icrar.org!vospace/data0')
            node1 = ContainerNode('vos://icrar.org!vospace/data1')
            node2 = ContainerNode('vos://icrar.org!vospace/data2')
            node3 = ContainerNode('vos://icrar.org!vospace/data1/data4')
            node4 = Node('vos://icrar.org!vospace/data2/data4')

            mv = Move(node1, node2)
            resp = await self.post('http://localhost:8080/vospace/transfers', mv.tostring())
            uws_response = await resp.text()
            self.assertEqual(200, resp.status, msg=uws_response)

            # Invalid Jobid
            state = 'PHASE=RUN'
            resp = await self.post(f'http://localhost:8080/vospace/transfers/1234/phase', state)
            response = await resp.text()
            self.assertEqual(400, resp.status, msg=response)

            root = ET.fromstring(uws_response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            # Invalid Phase
            state = 'PHASE=STOP'
            resp = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
            response = await resp.text()
            self.assertEqual(400, resp.status, msg=response)

            state = 'PHASE=RUN'
            resp = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            while True:
                resp = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
                response = await resp.text()
                self.assertEqual(200, resp.status, msg=response)
                if response == 'COMPLETED' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('ERROR', response)

            # Check error, node should not exist
            resp = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/error', None)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            root = ET.fromstring(response)
            error = root.find('{http://www.ivoa.net/xml/UWS/v1.0}errorSummary')

            self.assertIsNotNone(error)
            self.assertTrue("Node Not Found" in error[0].text)

            # create nodes for invalid tests
            resp = await self.put('http://localhost:8080/vospace/nodes/data0', node0.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())
            resp = await self.put('http://localhost:8080/vospace/nodes/data1', node1.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())
            resp = await self.put('http://localhost:8080/vospace/nodes/data2', node2.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())
            resp = await self.put('http://localhost:8080/vospace/nodes/data1/data4', node3.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())
            resp = await self.put('http://localhost:8080/vospace/nodes/data2/data4', node4.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            # Invalid move to a non-container
            mv = Move(node1, node0)
            resp = await self.post('http://localhost:8080/vospace/transfers', mv.tostring())
            uws_response = await resp.text()
            self.assertEqual(200, resp.status, msg=uws_response)

            root = ET.fromstring(uws_response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            state = 'PHASE=RUN'
            resp = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            while True:
                resp = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
                response = await resp.text()
                self.assertEqual(200, resp.status, msg=response)
                if response == 'COMPLETED' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('ERROR', response)

            resp = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/error', None)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            root = ET.fromstring(response)
            error = root.find('{http://www.ivoa.net/xml/UWS/v1.0}errorSummary')

            self.assertIsNotNone(error)
            self.assertTrue("Duplicate Node" in error[0].text)

            # Invalid move if node already exists in destination tree
            mv = Move(node3, node2)
            resp = await self.post('http://localhost:8080/vospace/transfers', mv.tostring())
            uws_response = await resp.text()
            self.assertEqual(200, resp.status, msg=uws_response)

            root = ET.fromstring(uws_response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            state = 'PHASE=RUN'
            resp = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            while True:
                resp = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
                response = await resp.text()
                self.assertEqual(200, resp.status, msg=response)
                if response == 'COMPLETED' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('ERROR', response)

            resp = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/error', None)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            root = ET.fromstring(response)
            error = root.find('{http://www.ivoa.net/xml/UWS/v1.0}errorSummary')

            self.assertIsNotNone(error)
            self.assertTrue("Duplicate Node" in error[0].text)

            # Move parent to child
            mv = Move(node1, node3)
            resp = await self.post('http://localhost:8080/vospace/transfers', mv.tostring())
            uws_response = await resp.text()
            self.assertEqual(200, resp.status, msg=uws_response)

            root = ET.fromstring(uws_response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            state = 'PHASE=RUN'
            resp = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            while True:
                resp = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
                response = await resp.text()
                self.assertEqual(200, resp.status, msg=response)
                if response == 'COMPLETED' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('ERROR', response)

            resp = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/error', None)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            root = ET.fromstring(response)
            error = root.find('{http://www.ivoa.net/xml/UWS/v1.0}errorSummary')

            self.assertIsNotNone(error)
            self.assertTrue("Invalid URI." in error[0].text)

        self.loop.run_until_complete(run())

    def test_copy_node(self):
        async def run():
            root1 = ContainerNode('vos://icrar.org!vospace/root3')
            resp = await self.put('http://localhost:8080/vospace/nodes/root3', root1.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            root2 = ContainerNode('vos://icrar.org!vospace/root4')
            resp = await self.put('http://localhost:8080/vospace/nodes/root4', root2.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            properties = [Property('ivo://ivoa.net/vospace/core#title', "Test1", True),
                          Property('ivo://ivoa.net/vospace/core#description', "Test2", True)]
            node1 = ContainerNode('vos://icrar.org!vospace/root3/test1', properties=properties)
            resp = await self.put('http://localhost:8080/vospace/nodes/root3/test1', node1.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            properties1 = [Property('ivo://ivoa.net/vospace/core#title', "Hello", True),
                           Property('ivo://ivoa.net/vospace/core#description', "There", True)]
            node2 = Node('vos://icrar.org!vospace/root3/test1/test2', properties=properties1)
            resp = await self.put('http://localhost:8080/vospace/nodes/root3/test1/test2', node2.tostring())
            self.assertEqual(201, resp.status, msg=await resp.text())

            # Copy tree from node1 to root2
            mv = Copy(node1, root2)
            resp = await self.post('http://localhost:8080/vospace/transfers', mv.tostring())
            uws_response = await resp.text()
            self.assertEqual(200, resp.status, msg=uws_response)

            root = ET.fromstring(uws_response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            state = 'PHASE=RUN'
            resp = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            while True:
                resp = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase', state)
                response = await resp.text()
                self.assertEqual(200, resp.status, msg=response)
                if response == 'COMPLETED' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('COMPLETED', response)

            # Check tree has been moved from node1 to root2
            params = {'detail': 'max'}
            resp = await self.get('http://localhost:8080/vospace/nodes/root4/test1', params)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            node = Node.fromstring(response)
            copy_node = ContainerNode('vos://icrar.org!vospace/root4/test1',
                                      properties=properties,
                                      nodes=[Node('vos://icrar.org!vospace/root4/test1/test2')])

            self.assertEqual(node, copy_node)

            # Check descendant properties copied
            params = {'detail': 'max'}
            resp = await self.get('http://localhost:8080/vospace/nodes/root4/test1/test2', params)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            node = Node.fromstring(response)
            copy_node = Node('vos://icrar.org!vospace/root4/test1/test2',
                             properties=properties1)

            self.assertEqual(node, copy_node)

            # check original node is still there
            params = {'detail': 'max'}
            resp = await self.get('http://localhost:8080/vospace/nodes/root3/test1', params)
            response = await resp.text()
            self.assertEqual(200, resp.status, msg=response)

            node = Node.fromstring(response)
            orig_node = ContainerNode('vos://icrar.org!vospace/root3/test1',
                                      properties=properties,
                                      nodes=[Node('vos://icrar.org!vospace/root3/test1/test2')])
            self.assertEqual(node, orig_node)

        self.loop.run_until_complete(run())


if __name__ == '__main__':
    unittest.main()