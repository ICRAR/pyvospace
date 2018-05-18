import os
import xml.etree.ElementTree as ET

from urllib.parse import urlparse
from collections import namedtuple


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
    def __init__(self, uri, value, read_only=None):
        self.uri = uri
        self.value = value
        self.read_only = read_only

    def __eq__(self, other):
        if not isinstance(other, Property):
            return False

        return self.uri == other.uri and \
               self.value == other.value

    def _ro_tostring(self):
        if self.read_only is None:
            return ''

        ro_test = 'false'
        if self.read_only:
            ro_test = 'true'

        return f'readOnly="{ro_test}"'

    def tolist(self):
        return [self.uri, self.value, self.read_only]

    def tostring(self):
        return f'<vos:property uri="{self.uri}" ' \
               f'{self._ro_tostring()}>' \
               f'{self.value}</vos:property>'

    def __str__(self):
        return self.uri, self.value

    def __repr__(self):
        return f'{self.uri, self.value}'


class DeleteProperty(Property):
    def __init__(self, uri):
        super().__init__(uri, None, None)

    def tostring(self):
        return f'<vos:property uri="{self.uri}" ' \
               f'xsi:nil="true"></vos:property>'


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

    NS = {'vos': 'http://www.ivoa.net/xml/VOSpace/v2.1'}
    SPACE = 'icrar.org'

    def __init__(self,
                 path,
                 properties=[],
                 capabilities=[],
                 node_type='vos:Node',
                 node_uri_target=None):

        self.node_type_text = node_type
        self.node_uri_target = node_uri_target
        self.path = os.path.normpath(path).strip('.').lstrip('/')
        self.properties = properties
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

    def _build_node(self, root):
        for properties in root.findall('vos:properties', Node.NS):
            for node_property in properties.findall('vos:property', Node.NS):
                prop_uri = node_property.attrib.get('uri', None)
                if prop_uri is None:
                    raise Exception("Property URI does not exist.")

                prop_ro = node_property.attrib.get('readOnly', None)
                if prop_ro:
                    prop_ro_bool = False
                    if prop_ro == 'true':
                        prop_ro_bool = True
                    prop_ro = prop_ro_bool

                prop_text = node_property.text
                self._properties.append(Property(prop_uri, prop_text, prop_ro))

        self._sort_properties()

    def _sort_properties(self):
        self._properties.sort(key=lambda x: x.uri)

    @classmethod
    def _create_node(cls, node_uri, node_type, node_busy=False):
        if node_uri is None:
            raise Exception("Node URI does not exist.")

        node_path = urlparse(node_uri).path

        if node_type is None:
            raise Exception("Node Type does not exist.")

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
            raise Exception('Unknown node type.')

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
        node = Node._create_node(node_uri, node_type, node_busy)
        node._build_node(root)

        return node

    @property
    def properties(self):
        return self._properties

    @properties.setter
    def properties(self, value):
        self._properties = []
        if value:
            self._properties = value
            self._sort_properties()

    def add_property(self, value):
        self._properties.append(value)
        self._sort_properties()

    def _properties_tostring(self):
        return ''.join([prop.tostring() for prop in self._properties])

    def to_uri(self):
        return f"vos://{Node.SPACE}!vospace/{self.path}"

    def tostring(self):
        busy = ''
        if isinstance(self, DataNode):
            busy = f'busy="{str(self.busy).lower()}"'

        target = ''
        if isinstance(self, LinkNode):
            if self.node_uri_target is not None:
                target = f'<vos:target>{self.node_uri_target}</vos:target>'

        create_node_xml = f'<vos:node xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' \
                          f' xmlns:xs="http://www.w3.org/2001/XMLSchema-instance"' \
                          f' xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1"' \
                          f' xsi:type="{self.node_type_text}" uri="{self.to_uri()}" {busy}>' \
                          f'{target}' \
                          f'<vos:properties>{self._properties_tostring()}</vos:properties>' \
                          f'<vos:accepts/><vos:provides/>' \
                          f'<vos:capabilities/></vos:node>'
        return create_node_xml


class LinkNode(Node):
    def __init__(self,
                 path,
                 uri_target,
                 properties=[],
                 capabilities=[]):
        super().__init__(path=path,
                         properties=properties,
                         capabilities=capabilities,
                         node_type='vos:LinkNode',
                         node_uri_target=uri_target)

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

    def _build_node(self, root):
        super()._build_node(root)

        target = root.find('vos:target', Node.NS)
        if target is None:
            raise Exception('LinkNode target does not exist.')

        self.node_uri_target = target.text


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
        super()._build_node(root)

        for accepts in root.findall('vos:accepts', Node.NS):
            for view in accepts.findall('vos:view', Node.NS):
                view_uri = view.attrib.get('uri', None)
                if view_uri is None:
                    raise Exception("Accepts URI does not exist.")
                self._accepts.append(View(view_uri))

        for provides in root.findall('vos:provides', Node.NS):
            for view in provides.findall('vos:view', Node.NS):
                view_uri = view.attrib.get('uri', None)
                if view_uri is None:
                    raise Exception("Provides URI does not exist.")
                self._provides.append(View(view_uri))

    @property
    def busy(self):
        return self._busy

    @property
    def accepts(self):
        return self._accepts

    @accepts.setter
    def accepts(self, value):
        self._accepts = []
        if value:
            self._accepts = value

    def add_accepts_view(self, value):
        self._accepts.append(value)

    @property
    def provides(self):
        return self._provides

    @provides.setter
    def provides(self, value):
        self._provides = []
        if value:
            self._provides = value

    def add_provides_view(self, value):
        self._provides.append(value)


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

        self.nodes = nodes

    def __eq__(self, other):
        if not isinstance(other, DataNode):
            return False

        return super().__eq__(other) and self._nodes == other._nodes

    @property
    def node_type(self):
        return NodeType.ContainerNode

    def _sort_nodes(self):
        self._nodes.sort(key=lambda x: x.path)

    def _build_node(self, root):
        super()._build_node(root)

        for nodes in root.findall('vos:nodes', Node.NS):
            for node_obj in nodes.findall('vos:node', Node.NS):
                node_uri = node_obj.attrib.get('uri', None)
                node_type = node_obj.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type', None)
                node_busy = root.attrib.get('busy', 'false')
                if node_busy == 'true':
                    node_busy = True
                else:
                    node_busy = False
                node = Node._create_node(node_uri, node_type, node_busy)
                self._nodes.append(node)

        self._sort_nodes()

    @property
    def nodes(self):
        return self._nodes

    @nodes.setter
    def nodes(self, node_list):
        self._nodes = []
        if node_list:
            if not isinstance(node_list, list):
                raise Exception('Node not a list.')
            self._nodes = node_list
            self._sort_nodes()

    def add_node(self, value):
        self._nodes.append(value)
        self._sort_nodes()


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

    def _build_node(self, root):
        super()._build_node(root)


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

    def _build_node(self, root):
        super()._build_node(root)


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