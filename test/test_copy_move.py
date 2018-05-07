import asyncio
import unittest
import xml.etree.ElementTree as ET

from pyvospace.core.model import Node, ContainerNode, Property, DeleteProperty, Move, Copy
from test.test_base import TestBase


class TestCopyMove(TestBase):

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
            node1 = ContainerNode('/test1', properties=properties)

            await self.create_node(node1)

            # Set properties
            properties = [Property('ivo://ivoa.net/vospace/core#title', "NewTitle"),
                          DeleteProperty('ivo://ivoa.net/vospace/core#description')]
            node1 = ContainerNode('/test1', properties=properties)

            # Node doesnt exist
            await self.set_node_properties('test2', Node('/test2'), expected_status=404)

            await self.set_node_properties('test1', node1)

            params = {'detail': 'max'}
            node = await self.get_node('test1', params)

            prop = [Property('ivo://ivoa.net/vospace/core#title', "NewTitle")]
            orig_node = ContainerNode('/test1',
                                      properties=prop)

            self.assertEqual(node, orig_node)

        self.loop.run_until_complete(run())

    def test_move_node(self):
        async def run():
            root1 = ContainerNode('root1')
            status, response = await self.put('http://localhost:8080/vospace/nodes/root1',
                                              data=root1.tostring())
            self.assertEqual(201, status, msg=response)

            root2 = ContainerNode('/root2')
            status, response = await self.put('http://localhost:8080/vospace/nodes/root2',
                                              data=root2.tostring())
            self.assertEqual(201, status, msg=response)

            properties = [Property('ivo://ivoa.net/vospace/core#title', "Test1", True),
                          Property('ivo://ivoa.net/vospace/core#description', "Test2", True)]
            node1 = ContainerNode('/root1/test2', properties=properties)
            status, response = await self.put('http://localhost:8080/vospace/nodes/root1/test2',
                                              data=node1.tostring())
            self.assertEqual(201, status, msg=response)

            node2 = ContainerNode('/root1/test2/test3')
            status, response = await self.put('http://localhost:8080/vospace/nodes/root1/test2/test3',
                                              data=node2.tostring())
            self.assertEqual(201, status, msg=response)

            node3 = ContainerNode('/root1/test2/test4')
            status, response = await self.put('http://localhost:8080/vospace/nodes/root1/test2/test4',
                                              data=node3.tostring())
            self.assertEqual(201, status, msg=response)

            # Check consistency of tree
            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/root1/test2',
                                              params=params)
            self.assertEqual(200, status, msg=response)

            node = Node.fromstring(response)
            orig_node = ContainerNode('/root1/test2',
                                      properties=properties,
                                      nodes=[ContainerNode('/root1/test2/test4'),
                                             ContainerNode('/root1/test2/test3')])
            self.assertEqual(node, orig_node)

            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/root2',
                                              params=params)
            self.assertEqual(200, status, msg=response)

            node = Node.fromstring(response)
            orig_node = ContainerNode('/root2')
            self.assertEqual(node, orig_node)

            # Move tree from node1 to root2
            mv = Move(node1, root2)
            response = await self.transfer_node(mv)
            job_id = self.get_job_id(response)
            await self.change_job_state(job_id, 'PHASE=RUN')
            await self.poll_job(job_id, expected_status='COMPLETED')

            # Check tree has been moved from node1 to root2
            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/root2/test2',
                                              params=params)
            self.assertEqual(200, status, msg=response)

            node = Node.fromstring(response)
            moved_node = ContainerNode('/root2/test2',
                                       properties=properties,
                                       nodes=[ContainerNode('/root2/test2/test4'),
                                              ContainerNode('/root2/test2/test3')])
            self.assertEqual(node, moved_node)

            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/root1',
                                              params=params)
            self.assertEqual(200, status, msg=response)

            node = Node.fromstring(response)
            orig_node = ContainerNode('/root1')
            self.assertEqual(node, orig_node)

        self.loop.run_until_complete(run())

    def test_invalid_copy_move(self):
        async def run():
            node0 = Node('/data0')
            node1 = ContainerNode('/data1')
            node2 = ContainerNode('/data2')
            node3 = ContainerNode('/data1/data4')
            node4 = Node('/data2/data4')

            # create nodes for invalid tests
            await self.create_node(node0)
            await self.create_node(node1)
            await self.create_node(node2)
            await self.create_node(node3)
            await self.create_node(node4)

            # Invalid Jobid
            await self.change_job_state(1234, 'PHASE=RUN', expected_status=400)

            # Node doesn't exist
            mv = Move(Node('/data11'), node2)
            response = await self.transfer_node(mv)
            job_id = self.get_job_id(response)
            await self.change_job_state(job_id, 'PHASE=RUN')
            await self.poll_job(job_id, expected_status='ERROR')

            # move node1 -> node2
            mv = Move(node1, node2)
            response = await self.transfer_node(mv)
            job_id = self.get_job_id(response)
            # Invalid Phase
            await self.change_job_state(job_id, 'PHASE=STOP', expected_status=400)

            # delete node before move
            await self.delete('http://localhost:8080/vospace/nodes/data1')

            # start move job
            await self.change_job_state(job_id, 'PHASE=RUN')
            await self.poll_job(job_id, expected_status='ERROR')
            # Check error, node should not exist
            await self.get_error_summary(job_id, "Node Not Found")

            # Create the deleted node from previous test
            await self.create_node(node1)

            # Invalid move to a non-container
            mv = Move(node1, node0)
            response = await self.transfer_node(mv)
            job_id = self.get_job_id(response)
            await self.change_job_state(job_id, 'PHASE=RUN')
            await self.poll_job(job_id, expected_status='ERROR')
            await self.get_error_summary(job_id, error_contains='Duplicate Node')

            # Invalid move if node already exists in destination tree
            await self.create_node(node3)
            mv = Move(node3, node2)
            await self.transfer_node(mv)
            job_id = self.get_job_id(response)
            await self.change_job_state(job_id, 'PHASE=RUN')
            await self.poll_job(job_id, expected_status='ERROR')
            await self.get_error_summary(job_id, error_contains='Duplicate Node')

            # Move parent to child which should be invalid because node1 is node3s parent
            mv = Move(node1, node3)
            response = await self.transfer_node(mv)
            job_id = self.get_job_id(response)
            await self.change_job_state(job_id, 'PHASE=RUN')
            await self.poll_job(job_id, expected_status='ERROR')
            await self.get_error_summary(job_id, error_contains='Invalid URI.')

        self.loop.run_until_complete(run())

    def test_copy_node(self):
        async def run():
            root1 = ContainerNode('/root3')
            await self.create_node(root1)

            root2 = ContainerNode('/root4')
            await self.create_node(root2)

            properties = [Property('ivo://ivoa.net/vospace/core#title', "Test1", True),
                          Property('ivo://ivoa.net/vospace/core#description', "Test2", True)]
            node1 = ContainerNode('/root3/test1', properties=properties)
            await self.create_node(node1)

            properties1 = [Property('ivo://ivoa.net/vospace/core#title', "Hello", True),
                           Property('ivo://ivoa.net/vospace/core#description', "There", True)]
            node2 = Node('/root3/test1/test2', properties=properties1)
            await self.create_node(node2)

            # Copy tree from node1 to root2
            mv = Copy(node1, root2)
            response = await self.transfer_node(mv)
            job_id = self.get_job_id(response)
            await self.change_job_state(job_id, 'PHASE=RUN')
            await self.poll_job(job_id, expected_status='COMPLETED')

            # Just chek there isn't any transfer details for a move or copy
            await self.get_transfer_details(job_id, expected_status=400)

            # Check tree has been moved from node1 to root2
            params = {'detail': 'max'}
            node = await self.get_node('root4/test1', params)

            copy_node = ContainerNode('/root4/test1',
                                      properties=properties,
                                      nodes=[Node('/root4/test1/test2')])
            self.assertEqual(node, copy_node)

            # check original node is still there
            params = {'detail': 'max'}
            node = await self.get_node('root3/test1', params)
            orig_node = ContainerNode('/root3/test1',
                                      properties=properties,
                                      nodes=[Node('/root3/test1/test2')])
            self.assertEqual(node, orig_node)

        self.loop.run_until_complete(run())


if __name__ == '__main__':
    unittest.main()