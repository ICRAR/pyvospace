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
        self._uri = None
        self.uri = uri
        self.value = value
        self.read_only = read_only

    def __eq__(self, other):
        if not isinstance(other, Property):
            return False
        return self.uri == other.uri and \
               self.value == other.value and \
               self.read_only == other.read_only

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, value):
        Property.validate_property_uri(value)
        self._uri = copy.deepcopy(value)

    @classmethod
    def validate_property_uri(cls, uri):
        assert uri
        parsed = urlparse(uri)
        assert parsed.scheme == 'ivo'
        assert parsed.path
        assert parsed.path.startswith('/vospace')
        assert parsed.fragment

    def tolist(self):
        return [self.uri, self.value, self.read_only]

    def __str__(self):
        return f"{self.uri}"

    def __repr__(self):
        return f'{self.uri, self.value, self.read_only}'


class DeleteProperty(Property):
    def __init__(self, uri):
        super().__init__(uri, None, False)


class Capability(object):
    def __init__(self, uri, endpoint, param):
        self.uri = uri
        self.endpoint = endpoint
        self.param = param


class Endpoint(object):
    def __init__(self, url):
        self._url = url

    @property
    def url(self):
        return self._url

    def __str__(self):
        return self._url


class Protocol(object):
    def __init__(self, uri, endpoint=None):
        self._uri = uri
        self._endpoint = None
        self.endpoint = endpoint

    def __str__(self):
        return self.uri

    @property
    def endpoint(self):
        return self._endpoint

    @endpoint.setter
    def endpoint(self, value):
        if value:
            assert isinstance(value, Endpoint)
            self._endpoint = value

    @property
    def uri(self):
        return self._uri

    def __eq__(self, other):
        if not isinstance(other, Protocol):
            return False
        return self.uri == other.uri


class Protocols(object):
    def __init__(self, accepts, provides):
        assert isinstance(accepts, list)
        for view in accepts:
            assert isinstance(view, Protocol)
        self.accepts = accepts
        assert isinstance(accepts, list)
        for view in accepts:
            assert isinstance(view, Protocol)
        self.provides = provides

    def tostring(self):
        root = self.toxml()
        return ET.tostring(root).decode("utf-8")

    def toxml(self):
        root = ET.Element("{http://www.ivoa.net/xml/VOSpace/v2.1}protocols", nsmap = Node.NS)
        if self.accepts:
            accepts_elem = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}accepts")
            for prop in self.accepts:
                protocol_element = ET.SubElement(accepts_elem, "{http://www.ivoa.net/xml/VOSpace/v2.1}protocol")
                protocol_element.set('uri', prop.uri)
        if self.provides:
            provides_elem = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}provides")
            for prop in self.provides:
                protocol_element = ET.SubElement(provides_elem, "{http://www.ivoa.net/xml/VOSpace/v2.1}protocol")
                protocol_element.set('uri', prop.uri)
        return root


class HTTPPut(Protocol):
    def __init__(self, endpoint=None):
        super().__init__('ivo://ivoa.net/vospace/core#httpput', endpoint)


class HTTPGet(Protocol):
    def __init__(self, endpoint=None):
        super().__init__('ivo://ivoa.net/vospace/core#httpget', endpoint)


class HTTPSPut(Protocol):
    def __init__(self, endpoint=None):
        super().__init__('ivo://ivoa.net/vospace/core#httpsput', endpoint)


class HTTPSGet(Protocol):
    def __init__(self, endpoint=None):
        super().__init__('ivo://ivoa.net/vospace/core#httpsget', endpoint)


class View(object):
    def __init__(self, uri):
        self._uri = uri

    @property
    def uri(self):
        return self._uri

    def __eq__(self, other):
        if not isinstance(other, View):
            return False
        return self.uri == other.uri

    def __str__(self):
        return self.uri


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

        self._path = Node.uri_to_path(path)
        self.node_type_text = node_type
        self._properties = []
        self.set_properties(properties, True)
        self.capabilities = capabilities
        # clients do not have access to these attributes
        self.owner = None
        self.group_read = None
        self.group_write = None

    def __str__(self):
        return self.path

    def __repr__(self):
        return self.path

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False
        return self.path == other.path and \
               self.node_type_text == other.node_type_text and \
               self._properties == other._properties

    @classmethod
    def uri_to_path(cls, uri):
        uri_parsed = urlparse(uri)
        if not uri_parsed.path:
            raise InvalidURI("URI does not exist.")
        if '.' in uri_parsed.path:
            raise InvalidURI("Invalid character: '.' in URI")
        return os.path.normpath(uri_parsed.path).lstrip('/')

    @property
    def path(self):
        return self._path

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
        if node_type == 'vos:Node':
            return Node(node_uri)
        elif node_type == 'vos:DataNode':
            return DataNode(node_uri, busy=node_busy)
        elif node_type == 'vos:UnstructuredDataNode':
            return UnstructuredDataNode(node_uri, busy=node_busy)
        elif node_type == 'vos:StructuredDataNode':
            return StructuredDataNode(node_uri, busy=node_busy)
        elif node_type == 'vos:ContainerNode':
            return ContainerNode(node_uri, busy=node_busy)
        elif node_type == 'vos:LinkNode':
            return LinkNode(node_uri, None)
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
        assert isinstance(property_list, list)
        for prop in property_list:
            assert isinstance(prop, Property)
        self._properties = copy.deepcopy(property_list)
        if sort:
            self.sort_properties()

    def add_property(self, value, sort=False):
        assert isinstance(value, Property)
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
        self._accepts = []
        self.accepts = accepts
        self._provides = []
        self.provides = provides
        self._busy = busy

    def __eq__(self, other):
        if not isinstance(other, DataNode):
            return False
        return super().__eq__(other)

    @property
    def node_type(self):
        return NodeType.DataNode

    def build_node(self, root):
        super().build_node(root)
        #for i in root:
        #    print(i)
        #print('xpath', root.xpath('//vos:node/vos:accepts/vos:view', namespaces=Node.NS))
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
        assert isinstance(value, list)
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
        assert isinstance(value, list)
        for val in value:
            assert isinstance(val, View)
        self._provides = copy.deepcopy(value)

    def add_provides_view(self, value):
        assert isinstance(value, View)
        self._provides.append(value)

    def tostring(self):
        root = super().toxml()
        root.set("busy", str(self.busy).lower())
        if self.accepts:
            accepts_elem = ET.SubElement(root, '{http://www.ivoa.net/xml/VOSpace/v2.1}accepts')
            for view in self.accepts:
                view_element = ET.SubElement(accepts_elem, '{http://www.ivoa.net/xml/VOSpace/v2.1}view')
                view_element.set('uri', view.uri)
        if self.provides:
            accepts_elem = ET.SubElement(root, '{http://www.ivoa.net/xml/VOSpace/v2.1}provides')
            for view in self.provides:
                view_element = ET.SubElement(accepts_elem, '{http://www.ivoa.net/xml/VOSpace/v2.1}view')
                view_element.set('uri', view.uri)
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
        super().build_node(root)
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
        assert isinstance(node_list, list)
        for node in node_list:
            assert isinstance(node, Node)
            assert node.path.startswith(self.path)
        self._nodes = copy.deepcopy(node_list)
        if sort:
            self.sort_nodes()

    def add_node(self, value, sort=False):
        assert isinstance(value, Node)
        assert value.path.startswith(self.path)
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
        self._target = None
        self.target = target
        self._direction = direction

    @property
    def target(self):
        return self._target

    @property
    def direction(self):
        return self._direction

    @target.setter
    def target(self, value):
        assert isinstance(value, Node)
        self._target = value

    def build_node(self, root):
        return NotImplementedError()

    def toxml(self):
        root = ET.Element("{http://www.ivoa.net/xml/VOSpace/v2.1}transfer", nsmap=Node.NS)
        target = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}target")
        target.text = str(self.target)
        direction = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}direction")
        direction.text = str(self.direction)
        return root

    def tostring(self):
        root = self.toxml()
        return ET.tostring(root).decode("utf-8")

    @classmethod
    def create_node(cls, target, direction, keep_bytes):
        if direction == 'pushToVoSpace':
            return PushToSpace(Node(target))
        elif direction == 'pullFromVoSpace':
            return PullFromSpace(Node(target))
        elif direction == 'pushFromVoSpace':
            raise NotImplementedError()
        elif direction == 'pullToVoSpace':
            raise NotImplementedError()
        else:
            if keep_bytes:
                return Copy(Node(target), Node(direction))
            return Move(Node(target), Node(direction))

    @classmethod
    def fromroot(cls, root):
        target = root.xpath('/vos:transfer/vos:target', namespaces=Node.NS)
        if not target:
            raise InvalidURI('target not found')
        direction = root.xpath('/vos:transfer/vos:direction', namespaces=Node.NS)
        if not direction:
            raise InvalidURI('direction not found')
        keep_bytes = root.xpath('/vos:transfer/vos:keepBytes', namespaces=Node.NS)
        if keep_bytes:
            if keep_bytes[0].text == 'false':
                keep_bytes = False
            elif keep_bytes[0].text == 'true':
                keep_bytes = True
            else:
                raise InvalidURI('keepBytes invalid')
        node = Transfer.create_node(target[0].text, direction[0].text, keep_bytes)
        node.build_node(root)
        return node

    @classmethod
    def fromstring(cls, xml):
        root = ET.fromstring(xml)
        return Transfer.fromroot(root)


class NodeTransfer(Transfer):
    def __init__(self, target, direction, keep_bytes):
        super().__init__(target, direction)
        self._keep_bytes = keep_bytes

    @property
    def keep_bytes(self):
        return self._keep_bytes

    def tostring(self):
        root = super().toxml()
        keep_bytes_str = 'false'
        if self.keep_bytes:
            keep_bytes_str = 'true'

        keep_bytes = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}keepBytes")
        keep_bytes.text = keep_bytes_str
        return ET.tostring(root).decode("utf-8")


class Copy(NodeTransfer):
    def __init__(self, target, direction):
        super().__init__(target=target,
                         direction=direction,
                         keep_bytes=True)

    def build_node(self, root):
        pass


class Move(NodeTransfer):
    def __init__(self, target, direction):
        super().__init__(target=target,
                         direction=direction,
                         keep_bytes=False)

    def build_node(self, root):
        pass


class ProtocolTransfer(Transfer):
    def __init__(self, target, direction, protocols=[], view=None):
        super().__init__(target=target, direction=direction)
        self._protocols = []
        self.set_protocols(protocols)
        self._view = view

    @property
    def view(self):
        return self._view

    @property
    def protocols(self):
        return self._protocols

    def set_protocols(self, protocol_list,):
        assert isinstance(protocol_list, list)
        for protocol in protocol_list:
            assert isinstance(protocol, Protocol)
        self._protocols = copy.deepcopy(protocol_list)

    def tostring(self):
        root = super().toxml()
        if self.view:
            view_elem = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}view")
            view_elem.set('uri', str(self.view))
        for protocol in self._protocols:
            protocol_elem = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}protocol")
            protocol_elem.set('uri', str(protocol))
            if protocol.endpoint:
                endpoint_tag = ET.SubElement(protocol_elem, "{http://www.ivoa.net/xml/VOSpace/v2.1}endpoint")
                endpoint_tag.text = str(protocol.endpoint)
        return ET.tostring(root).decode("utf-8")

    def build_node(self, root):
        view_elem = root.xpath('/vos:transfer/vos:view', namespaces=Node.NS)
        if view_elem:
            view_uri = view_elem.attrib.get('uri', None)
            self._view = View(view_uri)
        protocols = root.xpath('/vos:transfer/vos:protocol', namespaces=Node.NS)
        for protocol in protocols:
            uri = protocol.attrib.get('uri', None)
            endpoint = None
            endpoint_elem = protocol.xpath('vos:endpoint', namespaces=Node.NS)
            if endpoint_elem:
                endpoint = Endpoint(endpoint_elem[0].text)
            if uri == 'ivo://ivoa.net/vospace/core#httpput':
                self._protocols.append(HTTPPut(endpoint))
            elif uri == 'ivo://ivoa.net/vospace/core#httpget':
                self._protocols.append(HTTPGet(endpoint))
            elif uri == 'ivo://ivoa.net/vospace/core#httpsput':
                self._protocols.append(HTTPSPut(endpoint))
            elif uri == 'ivo://ivoa.net/vospace/core#httpsget':
                self._protocols.append(HTTPSGet(endpoint))
            else:
                raise InvalidURI("unknown protocol")


class PushToSpace(ProtocolTransfer):
    def __init__(self, target, protocols=[], view=None):
        super().__init__(target=target,
                         direction='pushToVoSpace',
                         protocols=protocols,
                         view=view)


class PullFromSpace(ProtocolTransfer):
    def __init__(self, target, protocols=[], view=None):
        super().__init__(target=target,
                         direction='pullFromVoSpace',
                         protocols=protocols,
                         view=view)


##### UWS JOBS ######
UWS_Phase = namedtuple('NodeType', 'Pending '
                                   'Queued '
                                   'Executing '
                                   'Completed '
                                   'Error '
                                   'Aborted '
                                   'Unknown '
                                   'Held '
                                   'Suspended '
                                   'Archived')

UWSPhase = UWS_Phase(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

PhaseLookup = {0: 'PENDING',
               1: 'QUEUED',
               2: 'EXECUTING',
               3: 'COMPLETED',
               4: 'ERROR',
               5: 'ABORTED',
               6: 'UNKNOWN',
               7: 'HELD',
               8: 'SUSPENDED',
               9: 'ARCHIVED'}


class UWSResult(object):
    def __init__(self, id, attrs):
        self.id = id
        self.attrs = attrs

    def toxml(self, root):
        result = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}result')
        result.set('id', self.id)
        for key, value in self.attrs.items():
            result.set(key, value)

    @classmethod
    def fromroot(cls, root):
        result_set = []
        for result in root.xpath('/uws:results/uws:result', namespaces=UWSJob.NS):
            id = result.attrib.get('id', None)
            assert id
            del result.attrib['id']
            result_set.append(UWSResult(id, result.attrib))
        return result_set

    @classmethod
    def fromstring(cls, xml):
        root = ET.fromstring(xml)
        return UWSResult.fromroot(root)


class UWSJob(object):

    NS = {'uws': 'http://www.ivoa.net/xml/UWS/v1.0',
          'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
          'xlink': 'http://www.w3.org/1999/xlink',
          'vos': 'http://www.ivoa.net/xml/VOSpace/v2.1'}

    def __init__(self, job_id, phase, destruction, job_info=None, results=None, error=None):
        self.job_id = str(job_id)
        self.phase = phase
        self.destruction = destruction
        if job_info:
            assert isinstance(job_info, Transfer)
        self.job_info = job_info
        self._results = None
        self.results = results
        self.error = error
        self.transfer = None

    @property
    def results(self):
        return self._results

    @results.setter
    def results(self, value):
        if value:
            assert isinstance(value, list)
            for result in value:
                assert isinstance(result, UWSResult)
                self._results = copy.deepcopy(value)

    def toxml(self):
        root = ET.Element("{http://www.ivoa.net/xml/UWS/v1.0}job", nsmap=UWSJob.NS)
        ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}jobId').text = self.job_id
        owner_element = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}ownerId')
        owner_element.set('{http://www.w3.org/2001/XMLSchema-instance}nil', 'true')
        ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}phase').text = PhaseLookup[self.phase]
        ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}quote').text = None
        starttime_element = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}startTime')
        starttime_element.set('{http://www.w3.org/2001/XMLSchema-instance}nil', 'true')
        endtime_element = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}endTime')
        endtime_element.set('{http://www.w3.org/2001/XMLSchema-instance}nil', 'true')
        ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}executionDuration').text = str(0)
        ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}destruction').text = str(self.destruction)
        if self.job_info:
            job_info_elem = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}jobInfo')
            job_info_elem.append(self.job_info.toxml())
        if self.results:
            results = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}results')
            for result in results:
                result.toxml(results)
        if self.error:
            error_summary = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}errorSummary')
            ET.SubElement(error_summary, '{http://www.ivoa.net/xml/UWS/v1.0}message').text = self.error
        return root

    def results_tostring(self):
        if not self.results:
            return None
        results_elem = ET.Element('{http://www.ivoa.net/xml/UWS/v1.0}results', nsmap=UWSJob.NS)
        for result in self.results:
            result.toxml(results_elem)
        return ET.tostring(results_elem).decode("utf-8")

    def tostring(self):
        root = self.toxml()
        return ET.tostring(root).decode("utf-8")

    @classmethod
    def fromstring(cls, xml):
        root = ET.fromstring(xml)
        root_elem = root.xpath('/uws:job/uws:jobId', namespaces=UWSJob.NS)
        assert root_elem
        job_id = root_elem[0].text
        phase_elem = root.xpath('/uws:job/uws:phase', namespaces=UWSJob.NS)
        assert phase_elem
        phase = phase_elem[0].text
        destruction_elem = root.xpath('/uws:job/uws:destruction', namespaces=UWSJob.NS)
        assert destruction_elem
        destruction = destruction_elem[0].text
        result_set = UWSResult.fromroot(root)
        job_info = None
        job_info_elem = root.xpath('/uws:job/uws:jobInfo/vos:transfer', namespaces=UWSJob.NS)
        if job_info_elem:
            job_info = Transfer.fromstring(ET.tostring(job_info_elem[0]))
        error = None
        error_summary_elem = root.xpath('/uws:job/uws:errorSummary/uws:message', namespaces=UWSJob.NS)
        if error_summary_elem:
            error = error_summary_elem[0].text
        return UWSJob(job_id, phase, destruction, job_info, result_set, error)
