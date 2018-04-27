import asyncio
import unittest
import xml.etree.ElementTree as ET

from pyvospace.core.model import Node, ContainerNode, Property, DeleteProperty, Move, Copy
from test.test_base import TestBase


class TestCreate(TestBase):

    def tearDown(self):
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/root1'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/root2'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/root3'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/root4'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/data0'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/data1'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/data2'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/test1'))

        super().tearDown()

    def test_set_properties(self):
        async def run():
            properties = [Property('ivo://ivoa.net/vospace/core#title', "Hello1"),
                          Property('ivo://ivoa.net/vospace/core#description', "Hello2")]
            node1 = ContainerNode('vos://icrar.org!vospace/test1', properties=properties)
            status, response = await self.put('http://localhost:8080/vospace/nodes/test1',
                                              data=node1.tostring())
            self.assertEqual(201, status, msg=response)

            # Set properties
            properties = [Property('ivo://ivoa.net/vospace/core#title', "NewTitle"),
                          DeleteProperty('ivo://ivoa.net/vospace/core#description')]
            node1 = ContainerNode('vos://icrar.org!vospace/test1', properties=properties)
            status, response = await self.post('http://localhost:8080/vospace/nodes/test1',
                                               data=node1.tostring())
            self.assertEqual(200, status, msg=response)

            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/test1',
                                              params=params)
            self.assertEqual(200, status, msg=response)

            node = Node.fromstring(response)
            prop = [Property('ivo://ivoa.net/vospace/core#title', "NewTitle")]
            orig_node = ContainerNode('vos://icrar.org!vospace/test1',
                                      properties=prop)

            self.assertEqual(node, orig_node)

        self.loop.run_until_complete(run())

    def test_move_node(self):
        async def run():
            root1 = ContainerNode('vos://icrar.org!vospace/root1')
            status, response = await self.put('http://localhost:8080/vospace/nodes/root1',
                                              data=root1.tostring())
            self.assertEqual(201, status, msg=response)

            root2 = ContainerNode('vos://icrar.org!vospace/root2')
            status, response = await self.put('http://localhost:8080/vospace/nodes/root2',
                                              data=root2.tostring())
            self.assertEqual(201, status, msg=response)

            properties = [Property('ivo://ivoa.net/vospace/core#title', "Test1", True),
                          Property('ivo://ivoa.net/vospace/core#description', "Test2", True)]
            node1 = ContainerNode('vos://icrar.org!vospace/root1/test2', properties=properties)
            status, response = await self.put('http://localhost:8080/vospace/nodes/root1/test2',
                                              data=node1.tostring())
            self.assertEqual(201, status, msg=response)

            node2 = ContainerNode('vos://icrar.org!vospace/root1/test2/test3')
            status, response = await self.put('http://localhost:8080/vospace/nodes/root1/test2/test3',
                                              data=node2.tostring())
            self.assertEqual(201, status, msg=response)

            node3 = ContainerNode('vos://icrar.org!vospace/root1/test2/test4')
            status, response = await self.put('http://localhost:8080/vospace/nodes/root1/test2/test4',
                                              data=node3.tostring())
            self.assertEqual(201, status, msg=response)

            # Check consistency of tree
            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/root1/test2',
                                              params=params)
            self.assertEqual(200, status, msg=response)

            node = Node.fromstring(response)
            orig_node = ContainerNode('vos://icrar.org!vospace/root1/test2',
                                      properties=properties,
                                      nodes=[ContainerNode('vos://icrar.org!vospace/root1/test2/test4'),
                                             ContainerNode('vos://icrar.org!vospace/root1/test2/test3')])
            self.assertEqual(node, orig_node)

            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/root2',
                                              params=params)
            self.assertEqual(200, status, msg=response)

            node = Node.fromstring(response)
            orig_node = ContainerNode('vos://icrar.org!vospace/root2')
            self.assertEqual(node, orig_node)

            # Move tree from node1 to root2
            mv = Move(node1, root2)
            status, response = await self.post('http://localhost:8080/vospace/transfers',
                                               data=mv.tostring())
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            state = 'PHASE=RUN'
            status, response = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                               data=state)
            self.assertEqual(200, status, msg=response)

            while True:
                status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                                  params=state)
                self.assertEqual(200, status, msg=response)
                if response == 'COMPLETED' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('COMPLETED', response)

            # Check tree has been moved from node1 to root2
            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/root2/test2',
                                              params=params)
            self.assertEqual(200, status, msg=response)

            node = Node.fromstring(response)
            moved_node = ContainerNode('vos://icrar.org!vospace/root2/test2',
                                       properties=properties,
                                       nodes=[ContainerNode('vos://icrar.org!vospace/root2/test2/test4'),
                                              ContainerNode('vos://icrar.org!vospace/root2/test2/test3')])
            self.assertEqual(node, moved_node)

            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/root1',
                                              params=params)
            self.assertEqual(200, status, msg=response)

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

            # create nodes for invalid tests
            status, response = await self.put('http://localhost:8080/vospace/nodes/data0',
                                              data=node0.tostring())
            self.assertEqual(201, status, msg=response)
            status, response = await self.put('http://localhost:8080/vospace/nodes/data1',
                                              data=node1.tostring())
            self.assertEqual(201, status, msg=response)
            status, response = await self.put('http://localhost:8080/vospace/nodes/data2',
                                              data=node2.tostring())
            self.assertEqual(201, status, msg=response)
            status, response = await self.put('http://localhost:8080/vospace/nodes/data1/data4',
                                              data=node3.tostring())
            self.assertEqual(201, status, msg=response)
            status, response = await self.put('http://localhost:8080/vospace/nodes/data2/data4',
                                              data=node4.tostring())
            self.assertEqual(201, status, msg=response)

            mv = Move(Node('vos://icrar.org!vospace/data11'), node2)
            status, response = await self.post('http://localhost:8080/vospace/transfers',
                                               data=mv.tostring())
            self.assertEqual(404, status, msg=response)

            mv = Move(node1, node2)
            status, response = await self.post('http://localhost:8080/vospace/transfers',
                                               data=mv.tostring())
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            # Invalid Jobid
            state = 'PHASE=RUN'
            status, response = await self.post(f'http://localhost:8080/vospace/transfers/1234/phase',
                                               data=state)
            self.assertEqual(400, status, msg=response)

            # Invalid Phase
            state = 'PHASE=STOP'
            status, response = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                               data=state)
            self.assertEqual(400, status, msg=response)

            # delete node before move
            await self.delete('http://localhost:8080/vospace/nodes/data1')

            state = 'PHASE=RUN'
            status, response = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                               data=state)
            self.assertEqual(200, status, msg=response)

            while True:
                status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                                  params=state)
                self.assertEqual(200, status, msg=response)
                if response == 'COMPLETED' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('ERROR', response)

            # Check error, node should not exist
            status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/error',
                                              params=None)
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            error = root.find('{http://www.ivoa.net/xml/UWS/v1.0}errorSummary')

            self.assertIsNotNone(error)
            self.assertTrue("Node Not Found" in error[0].text)

            # Invalid move to a non-container
            status, response = await self.put('http://localhost:8080/vospace/nodes/data1',
                                              data=node1.tostring())
            self.assertEqual(201, status, msg=response)

            mv = Move(node1, node0)
            status, response = await self.post('http://localhost:8080/vospace/transfers',
                                               data=mv.tostring())
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            state = 'PHASE=RUN'
            status, response = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                               data=state)
            self.assertEqual(200, status, msg=response)

            while True:
                status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                                  params=state)
                self.assertEqual(200, status, msg=response)
                if response == 'COMPLETED' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('ERROR', response)

            status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/error',
                                              params=None)
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            error = root.find('{http://www.ivoa.net/xml/UWS/v1.0}errorSummary')

            self.assertIsNotNone(error)
            self.assertTrue("Duplicate Node" in error[0].text)

            # Invalid move if node already exists in destination tree
            status, response = await self.put('http://localhost:8080/vospace/nodes/data1/data4',
                                              data=node3.tostring())
            self.assertEqual(201, status, msg=response)

            mv = Move(node3, node2)
            status, response = await self.post('http://localhost:8080/vospace/transfers',
                                               data=mv.tostring())
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            state = 'PHASE=RUN'
            status, response = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                               data=state)
            self.assertEqual(200, status, msg=response)

            while True:
                status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                                  params=state)
                self.assertEqual(200, status, msg=response)
                if response == 'COMPLETED' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('ERROR', response)

            status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/error',
                                              params=None)
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            error = root.find('{http://www.ivoa.net/xml/UWS/v1.0}errorSummary')

            self.assertIsNotNone(error)
            self.assertTrue("Duplicate Node" in error[0].text)

            # Move parent to child
            mv = Move(node1, node3)
            status, response = await self.post('http://localhost:8080/vospace/transfers',
                                               data=mv.tostring())
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            state = 'PHASE=RUN'
            status, response = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                               data=state)
            self.assertEqual(200, status, msg=response)

            while True:
                status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                                  params=state)
                self.assertEqual(200, status, msg=response)
                if response == 'COMPLETED' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('ERROR', response)

            status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/error',
                                              params=None)
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            error = root.find('{http://www.ivoa.net/xml/UWS/v1.0}errorSummary')

            self.assertIsNotNone(error)
            self.assertTrue("Invalid URI." in error[0].text)

        self.loop.run_until_complete(run())

    def test_copy_node(self):
        async def run():
            root1 = ContainerNode('vos://icrar.org!vospace/root3')
            status, response = await self.put('http://localhost:8080/vospace/nodes/root3',
                                              data=root1.tostring())
            self.assertEqual(201, status, msg=response)

            root2 = ContainerNode('vos://icrar.org!vospace/root4')
            status, response = await self.put('http://localhost:8080/vospace/nodes/root4',
                                              data=root2.tostring())
            self.assertEqual(201, status, msg=response)

            properties = [Property('ivo://ivoa.net/vospace/core#title', "Test1", True),
                          Property('ivo://ivoa.net/vospace/core#description', "Test2", True)]
            node1 = ContainerNode('vos://icrar.org!vospace/root3/test1', properties=properties)
            status, response = await self.put('http://localhost:8080/vospace/nodes/root3/test1',
                                              data=node1.tostring())
            self.assertEqual(201, status, msg=response)

            properties1 = [Property('ivo://ivoa.net/vospace/core#title', "Hello", True),
                           Property('ivo://ivoa.net/vospace/core#description', "There", True)]
            node2 = Node('vos://icrar.org!vospace/root3/test1/test2', properties=properties1)
            status, response = await self.put('http://localhost:8080/vospace/nodes/root3/test1/test2',
                                              data=node2.tostring())
            self.assertEqual(201, status, msg=response)

            # Copy tree from node1 to root2
            mv = Copy(node1, root2)
            status, response = await self.post('http://localhost:8080/vospace/transfers',
                                               data=mv.tostring())
            self.assertEqual(200, status, msg=response)

            root = ET.fromstring(response)
            job_id = root.find('{http://www.ivoa.net/xml/UWS/v1.0}jobId')
            self.assertIsNotNone(job_id)

            state = 'PHASE=RUN'
            status, response = await self.post(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                               data=state)
            self.assertEqual(200, status, msg=response)

            while True:
                status, response = await self.get(f'http://localhost:8080/vospace/transfers/{job_id.text}/phase',
                                                  params=state)
                self.assertEqual(200, status, msg=response)
                if response == 'COMPLETED' or response == 'ERROR':
                    break
                await asyncio.sleep(0.1)

            self.assertEqual('COMPLETED', response)

            # Check tree has been moved from node1 to root2
            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/root4/test1',
                                              params=params)
            self.assertEqual(200, status, msg=response)

            node = Node.fromstring(response)
            copy_node = ContainerNode('vos://icrar.org!vospace/root4/test1',
                                      properties=properties,
                                      nodes=[Node('vos://icrar.org!vospace/root4/test1/test2')])

            self.assertEqual(node, copy_node)

            # Check descendant properties copied
            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/root4/test1/test2',
                                              params=params)
            self.assertEqual(200, status, msg=response)

            node = Node.fromstring(response)
            copy_node = Node('vos://icrar.org!vospace/root4/test1/test2',
                             properties=properties1)

            self.assertEqual(node, copy_node)

            # check original node is still there
            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/root3/test1',
                                              params=params)
            self.assertEqual(200, status, msg=response)

            node = Node.fromstring(response)
            orig_node = ContainerNode('vos://icrar.org!vospace/root3/test1',
                                      properties=properties,
                                      nodes=[Node('vos://icrar.org!vospace/root3/test1/test2')])
            self.assertEqual(node, orig_node)

        self.loop.run_until_complete(run())


if __name__ == '__main__':
    unittest.main()