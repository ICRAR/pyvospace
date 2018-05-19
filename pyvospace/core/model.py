import os
import copy
import lxml.etree as ET

from urllib.parse import urlparse
from collections import namedtuple

from .exception import *


Node_Type = namedtuple('NodeType', 'Node '
                                   'DataNode '
                                   'UnstructuredDataNode  '
                                   'StructuredDataNode '
                                   'ContainerNode '
                                   'LinkNode')

NodeLookup = {'vos:Node': 0,
              'vos:DataNode': 1,
              'vos:UnstructuredDataNode': 2,
              'vos:StructuredDataNode': 3,
              'vos:ContainerNode': 4,
              'vos:LinkNode': 5}

NodeTextLookup = {0: 'vos:Node',
                  1: 'vos:DataNode',
                  2: 'vos:UnstructuredDataNode',
                  3: 'vos:StructuredDataNode',
                  4: 'vos:ContainerNode',
                  5: 'vos:LinkNode'}

NodeType = Node_Type(0, 1, 2, 3, 4, 5)


class Property(object):
    def __init__(self, uri, value, read_only=True):
        self.uri = uri
        self.value = value
        self.read_only = read_only

    def __eq__(self, other):
        if not isinstance(other, Property):
            return False
        return self.uri == other.uri and self.value == other.value

    def tolist(self):
        return [self.uri, self.value, self.read_only]

    def __str__(self):
        return f"{self.uri}"

    def __repr__(self):
        return f'{self.uri, self.value}'


class DeleteProperty(Property):
    def __init__(self, uri):
        super().__init__(uri, None, False)


class Capability(object):
    def __init__(self, uri, endpoint, param):
        self.uri = uri
        self.endpoint = endpoint
        self.param = param


class Protocol(object):
    def __init__(self, uri):
        self.uri = uri

    def __eq__(self, other):
        if not isinstance(other, Protocol):
            return False

        return self.uri == other.uri

    def tostring(self):
        return f'<vos:protocol uri="{self.uri}"></vos:protocol>'


class HTTPPut(Protocol):
    def __init__(self):
        super().__init__('ivo://ivoa.net/vospace/core#httpput')


class HTTPGet(Protocol):
    def __init__(self):
        super().__init__('ivo://ivoa.net/vospace/core#httpget')


class View(object):
    def __init__(self, uri):
        self.uri = uri

    def __eq__(self, other):
        if not isinstance(other, View):
            return False

        return self.uri == other.uri

    def tostring(self):
        return f'<vos:view uri="{self.uri}"/>'


class Node(object):
    NS = {'vos': 'http://www.ivoa.net/xml/VOSpace/v2.1',
          'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
          'xs': 'http://www.w3.org/2001/XMLSchema-instance'}

    SPACE = 'icrar.org'

    def __init__(self,
                 path,
                 properties=[],
                 capabilities=[],
                 node_type='vos:Node'):

        self.node_type_text = node_type
        self.path = os.path.normpath(path).strip('.').lstrip('/')
        self._properties = []
        self.set_properties(properties, True)
        self.capabilities = capabilities

    def __repr__(self):
        return self.path

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False

        return self.path == other.path and \
               self.node_type_text == other.node_type_text and \
               self._properties == other._properties

    @property
    def node_type(self):
        return NodeType.Node

    def build_node(self, root):
        for node_property in root.xpath('/vos:node/vos:properties/vos:property', namespaces=Node.NS):
            prop_uri = node_property.attrib.get('uri', None)
            if prop_uri is None:
                raise InvalidURI("Property URI does not exist.")

            delete_prop = node_property.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}nil', None)
            prop_ro = node_property.attrib.get('readOnly', None)
            if prop_ro:
                prop_ro_bool = False
                if prop_ro == 'true':
                    prop_ro_bool = True
                prop_ro = prop_ro_bool

            prop_text = node_property.text

            if delete_prop is None:
                prop = Property(prop_uri, prop_text, prop_ro)
            else:
                prop = DeleteProperty(prop_uri)
            self._properties.append(prop)

        self.sort_properties()

    def sort_properties(self):
        self._properties.sort(key=lambda x: x.uri)

    @classmethod
    def create_node(cls, node_uri, node_type, node_busy=False):
        if node_uri is None:
            raise InvalidURI("Node URI does not exist.")

        if node_type is None:
            raise InvalidURI("Node Type does not exist.")

        node_path = urlparse(node_uri).path

        if node_type == 'vos:Node':
            return Node(node_path)
        elif node_type == 'vos:DataNode':
            return DataNode(node_path, busy=node_busy)
        elif node_type == 'vos:UnstructuredDataNode':
            return UnstructuredDataNode(node_path, busy=node_busy)
        elif node_type == 'vos:StructuredDataNode':
            return StructuredDataNode(node_path, busy=node_busy)
        elif node_type == 'vos:ContainerNode':
            return ContainerNode(node_path, busy=node_busy)
        elif node_type == 'vos:LinkNode':
            return LinkNode(node_path, None)
        else:
            raise InvalidURI('Unknown node type.')

    @classmethod
    def fromstring(cls, xml):
        root = ET.fromstring(xml)
        node_uri = root.attrib.get('uri', None)
        node_type = root.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type', None)
        node_busy = root.attrib.get('busy', 'false')
        if node_busy == 'true':
            node_busy = True
        else:
            node_busy = False
        node = Node.create_node(node_uri, node_type, node_busy)
        node.build_node(root)
        return node

    @property
    def properties(self):
        return self._properties

    def remove_properties(self):
        self._properties = []

    def set_properties(self, property_list, sort=False):
        assert isinstance(property_list, list) is True
        for prop in property_list:
            assert isinstance(prop, Property) is True
        self._properties = copy.deepcopy(property_list)
        if sort:
            self.sort_properties()

    def add_property(self, value, sort=False):
        assert isinstance(value, Property) is True
        self._properties.append(value)
        if sort:
            self.sort_properties()

    def properties_tolist(self):
        return [prop.tolist() for prop in self._properties]

    def to_uri(self):
        return f"vos://{Node.SPACE}!vospace/{self.path}"

    def tostring(self):
        root = self.toxml()
        return ET.tostring(root).decode("utf-8")

    def toxml(self):
        root = ET.Element("{http://www.ivoa.net/xml/VOSpace/v2.1}node", nsmap = Node.NS)
        root.set("{http://www.w3.org/2001/XMLSchema-instance}type", self.node_type_text)
        root.set("uri", self.to_uri())

        if self._properties:
            properties = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}properties")
            for prop in self._properties:
                property_element = ET.SubElement(properties, "{http://www.ivoa.net/xml/VOSpace/v2.1}property")
                property_element.set('uri', prop.uri)
                property_element.set('readOnly', str(prop.read_only).lower())
                property_element.text = prop.value
                if isinstance(prop, DeleteProperty):
                    property_element.set('{http://www.w3.org/2001/XMLSchema-instance}nil', 'true')
        return root


class LinkNode(Node):
    def __init__(self,
                 path,
                 uri_target,
                 properties=[],
                 capabilities=[]):
        super().__init__(path=path,
                         properties=properties,
                         capabilities=capabilities,
                         node_type='vos:LinkNode')
        self.node_uri_target = uri_target

    @property
    def target(self):
        return self.node_uri_target

    @property
    def node_type(self):
        return NodeType.LinkNode

    def __eq__(self, other):
        if not isinstance(other, LinkNode):
            return False

        return super().__eq__(other) and \
               self.node_uri_target == other.node_uri_target

    def build_node(self, root):
        super().build_node(root)
        target = root.find('vos:target', Node.NS)
        if target is None:
            raise InvalidURI('LinkNode target does not exist.')
        self.node_uri_target = target.text

    def tostring(self):
        root = super().toxml()
        ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}target").text = self.node_uri_target
        return ET.tostring(root).decode("utf-8")


class DataNode(Node):
    def __init__(self,
                 path,
                 properties=[],
                 capabilities=[],
                 accepts=[],
                 provides=[],
                 busy=False,
                 node_type='vos:DataNode'):
        super().__init__(path=path,
                         properties=properties,
                         capabilities=capabilities,
                         node_type=node_type)
        self._accepts = accepts
        self._provides = provides
        self._busy = busy

    def __eq__(self, other):
        if not isinstance(other, DataNode):
            return False
        return super().__eq__(other)

    @property
    def node_type(self):
        return NodeType.DataNode

    def _build_node(self, root):
        super().build_node(root)

        for view in root.xpath('/vos:node/vos:accepts/vos:view', namespaces=Node.NS):
            view_uri = view.attrib.get('uri', None)
            if view_uri is None:
                raise InvalidURI("Accepts URI does not exist.")
            self._accepts.append(View(view_uri))

        for view in root.xpath('/vos:node/vos:provides/vos:view', namespaces=Node.NS):
            view_uri = view.attrib.get('uri', None)
            if view_uri is None:
                raise InvalidURI("Provides URI does not exist.")
            self._provides.append(View(view_uri))

    @property
    def busy(self):
        return self._busy

    @property
    def accepts(self):
        return self._accepts

    @accepts.setter
    def accepts(self, value):
        assert isinstance(value, list) is True
        for val in value:
            assert isinstance(val, View)
        self._accepts = copy.deepcopy(value)

    def add_accepts_view(self, value):
        assert isinstance(value, View)
        self._accepts.append(value)

    @property
    def provides(self):
        return self._provides

    @provides.setter
    def provides(self, value):
        assert isinstance(value, list) is True
        for val in value:
            assert isinstance(val, View)
        self._provides = copy.deepcopy(value)

    def add_provides_view(self, value):
        assert isinstance(value, View)
        self._provides.append(value)

    def tostring(self):
        root = super().toxml()
        root.set("busy", str(self.busy).lower())
        return ET.tostring(root).decode("utf-8")


class ContainerNode(DataNode):
    def __init__(self,
                 path,
                 nodes=[],
                 properties=[],
                 capabilities=[],
                 accepts=[],
                 provides=[],
                 busy=False):
        super().__init__(path=path,
                         properties=properties,
                         capabilities=capabilities,
                         accepts=accepts,
                         provides=provides,
                         busy=busy,
                         node_type='vos:ContainerNode')

        self._nodes = []
        self.set_nodes(nodes, True)

    def __eq__(self, other):
        if not isinstance(other, DataNode):
            return False
        return super().__eq__(other) and self._nodes == other._nodes

    @property
    def nodes(self):
        return self._nodes

    @property
    def node_type(self):
        return NodeType.ContainerNode

    def sort_nodes(self):
        self._nodes.sort(key=lambda x: x.path)

    def build_node(self, root):
        super()._build_node(root)
        for nodes in root.xpath('/vos:node/vos:nodes/vos:node', namespaces=Node.NS):
            node_uri = nodes.attrib.get('uri', None)
            node_type = nodes.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type', None)
            node_busy = root.attrib.get('busy', 'false')
            if node_busy == 'true':
                node_busy = True
            else:
                node_busy = False
            node = Node.create_node(node_uri, node_type, node_busy)
            self._nodes.append(node)
        self.sort_nodes()

    def set_nodes(self, node_list, sort=False):
        assert isinstance(node_list, list) is True
        for node in node_list:
            assert isinstance(node, Node) is True
        self._nodes = copy.deepcopy(node_list)
        if sort:
            self.sort_nodes()

    def add_node(self, value, sort=False):
        assert isinstance(value, Node) is True
        self._nodes.append(value)
        if sort:
            self.sort_nodes()

    def tostring(self):
        root = super().toxml()
        nodes_element = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}nodes")
        for node in self.nodes:
            node_element = ET.SubElement(nodes_element, '{http://www.ivoa.net/xml/VOSpace/v2.1}node')
            node_element.set('uri', node.to_uri())
            node_element.set("{http://www.w3.org/2001/XMLSchema-instance}type", node.node_type_text)
        return ET.tostring(root).decode("utf-8")


class UnstructuredDataNode(DataNode):
    def __init__(self,
                 path,
                 properties=[],
                 capabilities=[],
                 accepts=[],
                 provides=[],
                 busy=False):
        super().__init__(path=path,
                         properties=properties,
                         capabilities=capabilities,
                         accepts=accepts,
                         provides=provides,
                         busy=busy,
                         node_type='vos:UnstructuredDataNode')

    def __eq__(self, other):
        if not isinstance(other, UnstructuredDataNode):
            return False
        return super().__eq__(other)

    @property
    def node_type(self):
        return NodeType.UnstructuredDataNode

    def build_node(self, root):
        super().build_node(root)


class StructuredDataNode(DataNode):
    def __init__(self,
                 path,
                 properties=[],
                 capabilities=[],
                 accepts=[],
                 provides=[],
                 busy=False):
        super().__init__(path=path,
                         properties=properties,
                         capabilities=capabilities,
                         accepts=accepts,
                         provides=provides,
                         busy=busy,
                         node_type='vos:StructuredDataNode')

    def __eq__(self, other):
        if not isinstance(other, StructuredDataNode):
            return False

        return super().__eq__(other)

    @property
    def node_type(self):
        return NodeType.StructuredDataNode

    def build_node(self, root):
        super().build_node(root)


class Transfer(object):

    def __init__(self, target, direction):
        self.target = target
        self.direction = direction

    def tostring(self):
        return Exception('Not implemented')


class NodeTransfer(Transfer):

    def __init__(self, target, direction, keep_bytes):
        super().__init__(target, direction)
        self.keep_bytes = keep_bytes

    def tostring(self):
        keep_bytes_str = 'false'
        if self.keep_bytes:
            keep_bytes_str = 'true'

        create_node_xml = f'<vos:transfer xmlns:xs="http://www.w3.org/2001/XMLSchema-instance"' \
                          f' xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1">' \
                          f'<vos:target>{self.target.to_uri()}</vos:target>' \
                          f'<vos:direction>{self.direction.to_uri()}</vos:direction>' \
                          f'<vos:keepBytes>{keep_bytes_str}</vos:keepBytes>' \
                          f'</vos:transfer>'
        return create_node_xml


class Copy(NodeTransfer):

    def __init__(self, target, direction):
        super().__init__(target=target,
                         direction=direction,
                         keep_bytes=True)


class Move(NodeTransfer):

    def __init__(self, target, direction):
        super().__init__(target=target,
                         direction=direction,
                         keep_bytes=False)


class PushToSpace(Transfer):

    def __init__(self, target, protocols, view=None):
        super().__init__(target=target,
                         direction='pushToVoSpace')

        self.protocols = protocols
        self.view = view

    def _view_tostring(self):
        if not self.view:
            return ''

        return self.view.tostring()

    def _protocols_tostring(self):
        protocol_array = []
        for protocol in self.protocols:
            protocol_array.append(protocol.tostring())
        return ''.join(protocol_array)

    def tostring(self):
        create_node_xml = f'<vos:transfer xmlns:xs="http://www.w3.org/2001/XMLSchema-instance"' \
                          f' xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1">' \
                          f'<vos:target>{self.target.to_uri()}</vos:target>' \
                          f'<vos:direction>{self.direction}</vos:direction>' \
                          f'{self._view_tostring()}' \
                          f'{self._protocols_tostring()}' \
                          f'</vos:transfer>'
        return create_node_xml


class PullFromSpace(Transfer):

    def __init__(self, target, protocols, view=None):
        super().__init__(target=target,
                         direction='pullFromVoSpace')

        self.protocols = protocols
        self.view = view

    def _view_tostring(self):
        if not self.view:
            return ''

        return self.view.tostring()

    def _protocols_tostring(self):
        protocol_array = []
        for protocol in self.protocols:
            protocol_array.append(protocol.tostring())
        return ''.join(protocol_array)

    def tostring(self):
        create_node_xml = f'<vos:transfer xmlns:xs="http://www.w3.org/2001/XMLSchema-instance"' \
                          f' xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1">' \
                          f'<vos:target>{self.target.to_uri()}</vos:target>' \
                          f'<vos:direction>{self.direction}</vos:direction>' \
                          f'{self._view_tostring()}' \
                          f'{self._protocols_tostring()}' \
                          f'</vos:transfer>'
        return create_node_xml