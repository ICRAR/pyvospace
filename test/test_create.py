import unittest
import xml.etree.ElementTree as ET

from xml.etree.ElementTree import tostring

from test.test_base import TestBase
from pyvospace.core.model import *


class TestCreate(TestBase):

    def tearDown(self):
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/test1'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/datanode'))
        super().tearDown()

    def test_container_model(self):
        root = ContainerNode('/test')
        root.insert_node_into_tree(ContainerNode('/test/test1'))
        root.insert_node_into_tree(ContainerNode('/test/test1/test3'))
        with self.assertRaises(InvalidArgument):
            root.insert_node_into_tree(DataNode('/test/test1/test3/'))
        with self.assertRaises(InvalidArgument):
            root.insert_node_into_tree(Node('/test/test1/test3/test3/test4'))
        root.insert_node_into_tree(Node('/test/test1/'), True)
        with self.assertRaises(InvalidArgument):
            root.insert_node_into_tree(Node('/test/'), True)
        self.assertEqual(root, ContainerNode('/test', nodes=[Node('/test/test1/')]))

        # add to a node deeper in the tree
        root1 = ContainerNode('/root1/test2/test3')
        root1.insert_node_into_tree(ContainerNode('/root1/test2/test3/test4'))
        nodes = [n for n in Node.walk(root1)]
        self.assertEqual(len(nodes), 2)

        # check root
        root1 = ContainerNode('/')
        root1.insert_node_into_tree(ContainerNode('///root1'))
        nodes = [n for n in Node.walk(root1)]
        self.assertEqual(len(nodes), 2)


    def test_create_delete(self):
        async def run():
            node = ContainerNode('test1')
            await self.create_node(node)

            node = Node('/test1/zzz')
            await self.create_node(node)

            node = ContainerNode('/test1/test2')
            await self.create_node(node)

            node = ContainerNode('/test1/test2/anothercontainer')
            await self.create_node(node)

            node = DataNode('/test1/test2/anothercontainer/test.tar')
            await self.create_node(node)

            for i in range(100):
                node = Node(f'/test1/{i}')
                await self.create_node(node)

        self.loop.run_until_complete(run())

    def test_get_protocol(self):
        async def run():
            status, response = await self.get('http://localhost:8080/vospace/protocols', params=None)
            self.assertEqual(200, status, msg=response)

        self.loop.run_until_complete(run())

    def test_get_views(self):
        async def run():
            status, response = await self.get('http://localhost:8080/vospace/views', params=None)
            self.assertEqual(200, status, msg=response)

        self.loop.run_until_complete(run())

    def test_get_properties(self):
        async def run():
            properties = [Property('ivo://ivoa.net/vospace/core#title', "Hello1", False),
                          Property('ivo://ivoa.net/vospace/core#description', "Hello2", False)]
            node1 = ContainerNode('/test1', properties=properties)
            await self.create_node(node1)

            status, response = await self.get('http://localhost:8080/vospace/properties', params=None)
            self.assertEqual(200, status, msg=response)

        self.loop.run_until_complete(run())

    def test_set_properties(self):
        async def run():
            properties = [Property('ivo://ivoa.net/vospace/core#title', "Hello1", False),
                          Property('ivo://ivoa.net/vospace/core#description', "Hello2", False)]
            node1 = ContainerNode('/test1', properties=properties)

            await self.create_node(node1)

            # Set properties
            properties = [Property('ivo://ivoa.net/vospace/core#title', "NewTitle"),
                          DeleteProperty('ivo://ivoa.net/vospace/core#description')]
            node1 = ContainerNode('/test1', properties=properties)

            # Node doesnt exist
            await self.set_node_properties(Node('/test2'), expected_status=404)

            await self.set_node_properties(node1)

            params = {'detail': 'max'}
            node = await self.get_node('test1', params)
            node.remove_property('ivo://ivoa.net/vospace/core#length')
            node.remove_property('ivo://ivoa.net/vospace/core#btime')
            node.remove_property('ivo://ivoa.net/vospace/core#ctime')
            node.remove_property('ivo://ivoa.net/vospace/core#mtime')
            node.remove_property('ivo://icrar.org/vospace/core#statfs')
            prop = [Property('ivo://ivoa.net/vospace/core#title', "NewTitle", False)]
            orig_node = ContainerNode('/test1', properties=prop)
            self.assertEqual(node, orig_node)

        self.loop.run_until_complete(run())

    def test_create_node_fail(self):
        async def run():
            # XML Parse Error
            status, response = await self.put('http://localhost:8080/vospace/nodes/',
                                              data='<test><test>')
            self.assertEqual(500, status, msg=response)

            # URI name in node does not match URL path
            node = Node('data1')
            status, response = await self.put('http://localhost:8080/vospace/nodes/data',
                                              data=node.tostring())
            self.assertEqual(400, status, msg=response)

            # Invalid attribute
            node = Node('data1')
            root = ET.fromstring(node.tostring())
            root.attrib.pop('uri')
            xml = tostring(root)
            status, response = await self.put('http://localhost:8080/vospace/nodes/data1',
                                              data=xml)
            self.assertEqual(400, status, msg=response)

            # Invalid node type
            node = Node('data1')
            root = ET.fromstring(node.tostring())
            root.attrib['{http://www.w3.org/2001/XMLSchema-instance}type'] = 'vos:unknown'
            xml = tostring(root)
            status, response = await self.put('http://localhost:8080/vospace/nodes/data1',
                                              data=xml)
            self.assertEqual(400, status, msg=response)

        self.loop.run_until_complete(run())

    def test_create_node(self):
        async def run():
            properties = [Property('ivo://ivoa.net/vospace/core#title', "Test", True),
                          Property('ivo://ivoa.net/vospace/core#description', "Hello", True)]
            node = ContainerNode('test1', properties=properties)
            await self.create_node(node)

            node1 = ContainerNode('/test1/test2')
            await self.create_node(node1)

            node2 = Node('/test1/data')
            await self.create_node(node2)

            node3 = DataNode('/datanode')
            await self.create_node(node3)

            data_node = await self.get_node('/datanode', params={'detail': 'max'})
            self.assertGreater(len(data_node.provides), 0)
            self.assertGreater(len(data_node.accepts), 0)

            # Test failure cases of getNode
            params = {'detail': 'max'}
            await self.get_node('/', params, expected_status=200)

            # Duplicate Node
            await self.create_node(node1, expected_status=409)

            # Link Node
            link_node = LinkNode('/test1/test2/test3', 'http://google.com')
            await self.create_node(link_node)

            node = await self.get_node('/test1/test2/test3', params={'detail': 'max'})
            self.assertEqual(node.target, link_node.target)

            # Check that Link Node is in Path
            node3 = Node('/test1/test2/test3/test4')
            await self.create_node(node3, expected_status=400)

            # Node not found
            params = {'detail': 'max'}
            await self.get_node('mynode', params, expected_status=404)

            # Container Node not found
            node_not_found = Node('/test1/test2/test10/test11')
            await self.create_node(node_not_found, expected_status=404)

            # Invalid parameters
            params = {'detail': 'invalid'}
            await self.get_node('test1', params, expected_status=400)

            params = {'limit': -1}
            await self.get_node('test1', params, expected_status=400)

            params = {'limit': "string"}
            await self.get_node('test1', params, expected_status=400)

            # get only one child of the container node, other should not be there.
            # Should be in alphabetical order
            params = {'detail': 'max', 'limit': 1}
            node = await self.get_node('test1', params)
            node.remove_property('ivo://ivoa.net/vospace/core#length')
            node.remove_property('ivo://ivoa.net/vospace/core#btime')
            node.remove_property('ivo://ivoa.net/vospace/core#ctime')
            node.remove_property('ivo://ivoa.net/vospace/core#mtime')
            node.remove_property('ivo://icrar.org/vospace/core#statfs')
            cmp_node = ContainerNode('/test1',
                                     properties=properties,
                                     nodes=[Node('/test1/data')])

            self.assertEqual(node, cmp_node)

            params = {'detail': 'min'}
            node = await self.get_node('test1', params)

            # detail = min should not have properties
            cmp_node = ContainerNode('/test1',
                                     properties=properties,
                                     nodes=[Node('/test1/data'),
                                            ContainerNode('/test1/test2')])
            self.assertNotEqual(node, cmp_node)

            cmp_node = ContainerNode('test1',
                                     nodes=[Node('/test1/data'),
                                            ContainerNode('/test1/test2')])

            self.assertEqual(node, cmp_node)

        self.loop.run_until_complete(run())

if __name__ == '__main__':
    unittest.main()