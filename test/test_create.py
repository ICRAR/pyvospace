import unittest
import xml.etree.ElementTree as ET

from xml.etree.ElementTree import tostring

from test.test_base import TestBase
from pyvospace.core.model import Node, ContainerNode, LinkNode, Property


class TestCreate(TestBase):

    def tearDown(self):
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/test1'))
        super().tearDown()

    def test_get_protocol(self):
        async def run():
            status, response = await self.get('http://localhost:8080/vospace/protocols',
                                              params=None)
            self.assertEqual(200, status, msg=response)

        self.loop.run_until_complete(run())

    def test_create_node_fail(self):
        async def run():
            # XML Parse Error
            status, response = await self.put('http://localhost:8080/vospace/nodes/',
                                              data='<test><test>')
            self.assertEqual(500, status, msg=response)

            # Empty Path
            node = Node('vos://icrar.org!vospace/')
            status, response = await self.put('http://localhost:8080/vospace/nodes/',
                                              data=node.tostring())
            self.assertEqual(400, status, msg=response)

            # URI name in node does not match URL path
            node = Node('vos://icrar.org!vospace/data1')
            status, response = await self.put('http://localhost:8080/vospace/nodes/data',
                                              data=node.tostring())
            self.assertEqual(400, status, msg=response)

            # Invalid attribute
            node = Node('vos://icrar.org!vospace/data1')
            root = ET.fromstring(node.tostring())
            root.attrib.pop('uri')
            xml = tostring(root)
            status, response = await self.put('http://localhost:8080/vospace/nodes/data1',
                                              data=xml)
            self.assertEqual(400, status, msg=response)

            # Invalid node type
            node = Node('vos://icrar.org!vospace/data1')
            root = ET.fromstring(node.tostring())
            root.attrib['{http://www.w3.org/2001/XMLSchema-instance}type'] = 'vos:unknown'
            xml = tostring(root)
            status, response = await self.put('http://localhost:8080/vospace/nodes/data1',
                                              data=xml)
            self.assertEqual(400, status, msg=response)

        self.loop.run_until_complete(run())

    def test_create_node(self):
        async def run():
            # Invalid Path
            node1 = ContainerNode('vos://icrar.org!vospace/')
            status, response = await self.put('http://localhost:8080/vospace/nodes/',
                                              data=node1.tostring())
            self.assertEqual(400, status, msg=response)

            #node1 = ContainerNode('/test')
            #resp = await self.put('http://localhost:8080/vospace/nodes/test', node1.tostring())
            #self.assertEqual(400, resp.status, msg=await resp.text())

            properties = [Property('ivo://ivoa.net/vospace/core#title', "Test", True),
                          Property('ivo://ivoa.net/vospace/core#description', "Hello", True)]
            node = ContainerNode('vos://icrar.org!vospace/test1', properties=properties)
            status, response = await self.put('http://localhost:8080/vospace/nodes/test1',
                                              data=node.tostring())
            self.assertEqual(201, status, msg=response)

            node1 = ContainerNode('vos://icrar.org!vospace/test1/test2')
            status, response = await self.put('http://localhost:8080/vospace/nodes/test1/test2',
                                              data=node1.tostring())
            self.assertEqual(201, status, msg=response)

            #log.debug(response)

            node2 = Node('vos://icrar.org!vospace/test1/data')
            status, response = await self.put('http://localhost:8080/vospace/nodes/test1/data',
                                              data=node2.tostring())
            self.assertEqual(201, status, msg=response)

            #log.debug(response)

            # Duplicate Node
            status, response = await self.put('http://localhost:8080/vospace/nodes/test1/test2',
                                              data=node1.tostring())
            self.assertEqual(409, status, msg=response)

            # Link Node
            node2 = LinkNode('vos://icrar.org!vospace/test1/test2/test3', 'http://google.com')
            status, response = await self.put('http://localhost:8080/vospace/nodes/test1/test2/test3',
                                              data=node2.tostring())
            self.assertEqual(201, status, msg=response)

            # Check that Link Node is in Path
            node3 = Node('vos://icrar.org!vospace/test1/test2/test3/test4')
            status, respomse = await self.put('http://localhost:8080/vospace/nodes/test1/test2/test3/test4',
                                              data=node3.tostring())
            self.assertEqual(400, status, msg=response)

            # Test failure cases of getNode
            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/',
                                              params=params)
            self.assertEqual(400, status, msg=response)

            # Node not found
            params = {'detail': 'max'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/mynode',
                                              params=params)
            self.assertEqual(404, status, msg=response)

            # Container Node not found
            node_not_found = Node('vos://icrar.org!vospace/test1/test2/test10/test11')
            status, response = await self.put('http://localhost:8080/vospace/nodes/test1/test2/test10/test11',
                                              data=node_not_found.tostring())
            self.assertEqual(404, status, msg=response)

            # Invalid parameters
            params = {'detail': 'invalid'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/test1',
                                              params=params)
            self.assertEqual(400, status, msg=response)

            params = {'limit': -1}
            status, response = await self.get('http://localhost:8080/vospace/nodes/test1',
                                              params=params)
            self.assertEqual(400, status, msg=response)

            params = {'limit': "string"}
            status, response = await self.get('http://localhost:8080/vospace/nodes/test1',
                                              params=params)
            self.assertEqual(400, status, msg=response)

            # get only one child of the container node, other should not be there.
            # Should be in alphabetical order
            params = {'detail': 'max', 'limit': 1}
            status, response = await self.get('http://localhost:8080/vospace/nodes/test1',
                                              params=params)
            self.assertEqual(200, status, msg=response)

            node = Node.fromstring(response)
            cmp_node = ContainerNode('vos://icrar.org!vospace/test1',
                                     properties=properties,
                                     nodes=[Node('vos://icrar.org!vospace/test1/data')])
            self.assertEqual(node, cmp_node)

            params = {'detail': 'min'}
            status, response = await self.get('http://localhost:8080/vospace/nodes/test1',
                                              params=params)
            self.assertEqual(200, status, msg=response)

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