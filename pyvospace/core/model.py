import os
import uuid
import copy
import lxml.etree as ET

from urllib.parse import urlparse
from collections import namedtuple, OrderedDict

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


def validate_property_uri(uri):
    if not uri:
        raise InvalidArgument('uri empty')
    parsed = urlparse(uri)
    if parsed.scheme != 'ivo':
        raise InvalidArgument('uri does not start with ivo')
    if not parsed.path:
        raise InvalidArgument('path empty')
    if not parsed.path.startswith('/vospace'):
        raise InvalidArgument('path does not start with /vospace')
    if not parsed.fragment:
        raise InvalidArgument('fragment empty')
    return parsed


class Parameter(object):
    """
    VOSpace Parameter.

    :param uri: uri.
    :param value: value.

    e.g. ivo://ivoa.net/vospace/core#length value: 1024
    """
    def __init__(self, uri, value):
        self.uri = uri
        self.value = value

    def __eq__(self, other):
        if not isinstance(other, Parameter):
            return False
        return self.uri == other.uri and \
               self.value == other.value

    @property
    def uri(self):
        return self._uri

    @property
    def name(self):
        return self._name

    @uri.setter
    def uri(self, value):
        parsed = validate_property_uri(value)
        self._name = parsed.fragment
        self._uri = copy.deepcopy(value)

    def tolist(self):
        return [self.uri, self.value]

    def __str__(self):
        return f"{self.uri}"

    def __repr__(self):
        return f'{self.uri, self.value}'


class Property(object):
    """
    VOSpace Property.

    :param uri: uri.
    :param value: value.

    e.g. uri: ivo://ivoa.net/vospace/core#length value: 1024
    """
    def __init__(self, uri, value, read_only=True, persist=True):
        self.uri = uri
        self.value = value
        self.read_only = read_only
        # Persist property in store, not avaliable to client
        self.persist = persist

    def __eq__(self, other):
        if not isinstance(other, Property):
            return False
        return self.uri == other.uri and \
               self.value == other.value and \
               self.read_only == other.read_only

    @property
    def persist(self):
        return self._persist

    @persist.setter
    def persist(self, value):
        if not isinstance(value, bool):
            raise InvalidArgument('invalid value')
        self._persist = value

    @property
    def uri(self):
        return self._uri

    @property
    def name(self):
        return self._name

    @uri.setter
    def uri(self, value):
        parsed = validate_property_uri(value)
        self._name = parsed.fragment
        self._uri = copy.deepcopy(value)

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


class SecurityMethod(object):
    """
    SecurityMethod required for PushToSpace or PullFromSpace.

    :param url: url.

    e.g. url: ivo://ivoa.net/sso#cookie
    """
    def __init__(self, url):
        if url is None:
            raise InvalidArgument("SecurityMethod URI is None.")
        self._url = url

    @property
    def url(self):
        return self._url

    def __str__(self):
        return self._url


class Endpoint(object):
    """
    Storage Endpoint.

    :param url: url.

    e.g. url: https://localhost:8000/pushtospace/
    """
    def __init__(self, url):
        if url is None:
            raise InvalidArgument("Endpoint URI is None.")
        self._url = url

    @property
    def url(self):
        return self._url

    def __str__(self):
        return self._url


class Protocol(object):
    """
    Baseclass for storage protocol.
    """
    def __init__(self, uri, endpoint=None, security_method=None):
        self.uri = uri
        self._endpoint = None
        self.endpoint = endpoint
        self._security_method = None
        self.security_method = security_method

    def __str__(self):
        return self._uri

    @property
    def uri(self):
        return self._uri

    @property
    def name(self):
        return self._name

    @uri.setter
    def uri(self, value):
        parsed = validate_property_uri(value)
        self._name = parsed.fragment
        self._uri = copy.deepcopy(value)

    @classmethod
    def create_protocol(cls, uri, security_method_uri=None):
        security_obj = None
        if security_method_uri:
            security_obj = SecurityMethod(security_method_uri)
        if uri == 'ivo://ivoa.net/vospace/core#httpput':
            return HTTPPut(security_method=security_obj)
        elif uri == 'ivo://ivoa.net/vospace/core#httpget':
            return HTTPGet(security_method=security_obj)
        elif uri == 'ivo://ivoa.net/vospace/core#httpsput':
            return HTTPSPut(security_method=security_obj)
        elif uri == 'ivo://ivoa.net/vospace/core#httpsget':
            return HTTPSGet(security_method=security_obj)
        else:
            return Protocol(uri, security_method=security_obj)

    @property
    def endpoint(self):
        return self._endpoint

    @endpoint.setter
    def endpoint(self, value):
        if value:
            if not isinstance(value, Endpoint):
                raise InvalidArgument('invalid Endpoint')
            self._endpoint = value

    @property
    def security_method(self):
        return self._security_method

    @security_method.setter
    def security_method(self, value):
        if value:
            if not isinstance(value, SecurityMethod):
                raise InvalidArgument('invalid SecurityMethod')
            self._security_method = value

    def __eq__(self, other):
        if not isinstance(other, Protocol):
            return False
        return self.uri == other.uri


class Protocols(object):
    """
    Protocols that a VOSpace accepts and provides.

    :param accepts: list of :func:`Protocol <pyvospace.core.model.Protocol>` the space accepts.
    :param provides: list of :func:`Protocol <pyvospace.core.model.Protocol>` the space provides.
    """
    def __init__(self, accepts, provides):
        if not isinstance(accepts, list):
            raise InvalidArgument('invalid list')
        for view in accepts:
            if not isinstance(view, Protocol):
                raise InvalidArgument('invalid Protocol')
        self.accepts = accepts
        if not isinstance(accepts, list):
            raise InvalidArgument('invalid list')
        for view in accepts:
            if not isinstance(view, Protocol):
                raise InvalidArgument('invalid Protocol')
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


class Properties(object):
    """
    Properties that the VOSpace accepts and provides.

    :param accepts: list of :func:`Property <pyvospace.core.model.Property>` the space accepts.
    :param provides: list of :func:`Property <pyvospace.core.model.Property>` the space provides.
    """
    def __init__(self, accepts, provides):
        if not isinstance(accepts, list):
            raise InvalidArgument('invalid list')
        for prop in accepts:
            if not isinstance(prop, Property):
                raise InvalidArgument('invalid Property')
        self.accepts = accepts
        if not isinstance(accepts, list):
            raise InvalidArgument('invalid list')
        for prop in accepts:
            if not isinstance(prop, Property):
                raise InvalidArgument('invalid Property')
        self.provides = provides
        self.contains = []

    def tostring(self):
        root = self.toxml()
        return ET.tostring(root).decode("utf-8")

    def toxml(self):
        root = ET.Element("{http://www.ivoa.net/xml/VOSpace/v2.1}properties", nsmap = Node.NS)
        if self.accepts:
            accepts_elem = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}accepts")
            for prop in self.accepts:
                prop_element = ET.SubElement(accepts_elem, "{http://www.ivoa.net/xml/VOSpace/v2.1}property")
                prop_element.set('uri', prop.uri)
        if self.provides:
            provides_elem = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}provides")
            for prop in self.provides:
                prop_element = ET.SubElement(provides_elem, "{http://www.ivoa.net/xml/VOSpace/v2.1}property")
                prop_element.set('uri', prop.uri)
        if self.contains:
            contains_elem = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}contains")
            for prop in self.contains:
                prop_element = ET.SubElement(contains_elem, "{http://www.ivoa.net/xml/VOSpace/v2.1}property")
                prop_element.set('uri', prop.uri)
        return root


class HTTPPut(Protocol):
    """
    HTTPPut Protocol.

    :param endpoint: :func:`Endpoint <pyvospace.core.model.Endpoint>`
    :param security_method: :func:`SecurityMethod <pyvospace.core.model.SecurityMethod>`
    """
    def __init__(self, endpoint=None, security_method=None):
        super().__init__(uri='ivo://ivoa.net/vospace/core#httpput',
                         endpoint=endpoint, security_method=security_method)


class HTTPGet(Protocol):
    """
    HTTPGet Protocol.

    :param endpoint: :func:`Endpoint <pyvospace.core.model.Endpoint>`
    :param security_method: :func:`SecurityMethod <pyvospace.core.model.SecurityMethod>`
    """
    def __init__(self, endpoint=None, security_method=None):
        super().__init__(uri='ivo://ivoa.net/vospace/core#httpget',
                         endpoint=endpoint, security_method=security_method)


class HTTPSPut(Protocol):
    """
    HTTPSPut Protocol.

    :param endpoint: :func:`Endpoint <pyvospace.core.model.Endpoint>`
    :param security_method: :func:`SecurityMethod <pyvospace.core.model.SecurityMethod>`
    """
    def __init__(self, endpoint=None, security_method=None):
        super().__init__(uri='ivo://ivoa.net/vospace/core#httpsput',
                         endpoint=endpoint, security_method=security_method)


class HTTPSGet(Protocol):
    """
    HTTPSGet Protocol.

    :param endpoint: :func:`Endpoint <pyvospace.core.model.Endpoint>`
    :param security_method: :func:`SecurityMethod <pyvospace.core.model.SecurityMethod>`
    """
    def __init__(self, endpoint=None, security_method=None):
        super().__init__(uri='ivo://ivoa.net/vospace/core#httpsget',
                         endpoint=endpoint, security_method=security_method)


class Views(object):
    """
    Views into and out of the VOSpace.

    :param accepts: list of :func:`View <pyvospace.core.model.View>` the space accepts.
    :param provides: list of :func:`View <pyvospace.core.model.View>` the space provides.
    """
    def __init__(self, accepts, provides):
        if not isinstance(accepts, list):
            raise InvalidArgument('invalid list')
        for view in accepts:
            if not isinstance(view, View):
                raise InvalidArgument('invalid View')
        self.accepts = accepts
        if not isinstance(accepts, list):
            raise InvalidArgument('invalid list')
        for view in accepts:
            if not isinstance(view, View):
                raise InvalidArgument('invalid View')
        self.provides = provides

    def tostring(self):
        root = self.toxml()
        return ET.tostring(root).decode("utf-8")

    def toxml(self):
        root = ET.Element("{http://www.ivoa.net/xml/VOSpace/v2.1}views", nsmap = Node.NS)
        if self.accepts:
            accepts_elem = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}accepts")
            for prop in self.accepts:
                protocol_element = ET.SubElement(accepts_elem, "{http://www.ivoa.net/xml/VOSpace/v2.1}view")
                protocol_element.set('uri', prop.uri)
        if self.provides:
            provides_elem = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}provides")
            for prop in self.provides:
                protocol_element = ET.SubElement(provides_elem, "{http://www.ivoa.net/xml/VOSpace/v2.1}view")
                protocol_element.set('uri', prop.uri)
        return root


class View(object):
    """
    VOSpace view.

    :param uri: uri.

    e.g. Using ivo://ivoa.net/vospace/core#tar
    on a container node will tell the space the user wants a tar for that nodes tree.
    """
    def __init__(self, uri):
        self.uri = uri

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, value):
        parsed = validate_property_uri(value)
        self._name = parsed.fragment
        self._uri = copy.deepcopy(value)

    @property
    def name(self):
        return self._name

    def __eq__(self, other):
        if not isinstance(other, View):
            return False
        return self.uri == other.uri

    def __str__(self):
        return self.uri


class Node(object):
    """
    Node representation for the VOSpace.

    :param path: space path. e.g. node/container/data1
    :param properties: list of :func:`Property <pyvospace.core.model.Property>`
    :param capabilities: capabilities of the space.
    :param owner: Server side only property.
    :param group_read: Server side only property.
    :param group_write: Server side only property.
    :param id: Server side only property.
    """

    NS = {'vos': 'http://www.ivoa.net/xml/VOSpace/v2.1',
          'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
          'xs': 'http://www.w3.org/2001/XMLSchema-instance'}

    SPACE = 'icrar.org'

    def __init__(self, path, properties=None, capabilities=None,
                 owner=None, group_read=None, group_write=None, id=None):
        self._path = Node.uri_to_path(path)
        self.name = os.path.basename(self._path)
        self.dirname = os.path.dirname(self._path)

        self._properties = {}
        self.set_properties(properties)
        self.capabilities = capabilities

        # =======================
        # Server side properties
        # =======================
        self._id = id
        if self._id is None:
            self._id = uuid.uuid4()
        self.owner = owner
        self.group_read = group_read
        self.group_write = group_write
        self.path_modified = 0
        self.size = 0
        self.storage = None

    def __lt__(self, other):
        return self.path < other.path

    def __str__(self):
        return self.path

    def __repr__(self):
        return self.path

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False
        return self.path == other.path and self.properties == other.properties

    @classmethod
    def walk(cls, node):
        yield node
        if isinstance(node, ContainerNode):
            for i in node.nodes:
                if isinstance(i, ContainerNode):
                    for j in Node.walk(i):
                        yield j
                else:
                    yield i

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, value):
        self._owner = value

    @property
    def group_read(self):
        return self._group_read

    @group_read.setter
    def group_read(self, value):
        if value is None:
            self._group_read = []
            return
        if not isinstance(value, list):
            raise InvalidArgument('invalid list')
        for val in value:
            if not isinstance(val, str):
                raise InvalidArgument('invalid string')
        self._group_read = copy.deepcopy(value)

    @property
    def group_write(self):
        return self._group_write

    @group_write.setter
    def group_write(self, value):
        if value is None:
            self._group_write = []
            return
        if not isinstance(value, list):
            raise InvalidArgument('invalid list')
        for val in value:
            if not isinstance(val, str):
                raise InvalidArgument('invalid string')
        self._group_write = copy.deepcopy(value)

    @classmethod
    def uri_to_path(cls, uri):
        if not uri:
            raise InvalidURI("URI does not exist.")
        uri_new = uri
        if uri.startswith('/'):
            if uri == '/' or uri == '//':
                return '/'
            uri_new = uri.lstrip('/')
        uri_parsed = urlparse(uri_new)
        if not uri_parsed.path:
            raise InvalidURI("URI does not exist.")
        new_path = os.path.normpath(uri_parsed.path).lstrip('/')
        return f"/{new_path}"

    @property
    def path(self):
        return self._path

    @property
    def node_type(self):
        return NodeType.Node

    def node_type_text(self):
        return 'vos:Node'

    def build_node(self, root):
        for node_property in root.xpath('/vos:node/vos:properties/vos:property', namespaces=Node.NS):
            prop_uri = node_property.attrib.get('uri', None)
            if prop_uri is None:
                raise InvalidXML("vos:property URI does not exist.")

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
            self._properties[prop.uri] = prop

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
        self._properties = {}

    def set_properties(self, property_list):
        if not property_list:
            return
        if not isinstance(property_list, list):
            raise InvalidArgument('invalid list')
        for prop in property_list:
            if not isinstance(prop, Property):
                raise InvalidArgument('invalid Property')
            self._properties[prop.uri] = copy.deepcopy(prop)

    def add_property(self, value):
        if not isinstance(value, Property):
            raise InvalidArgument('invalid Property')
        self._properties[value.uri] = copy.deepcopy(value)

    def remove_property(self, uri_key):
        return self._properties.pop(uri_key)

    def to_uri(self):
        return f"vos://{Node.SPACE}!vospace/{self.path}"

    def tostring(self):
        root = self.toxml()
        return ET.tostring(root).decode("utf-8")

    def toxml(self):
        root = ET.Element("{http://www.ivoa.net/xml/VOSpace/v2.1}node", nsmap=Node.NS)
        root.set("{http://www.w3.org/2001/XMLSchema-instance}type", self.node_type_text())
        root.set("uri", self.to_uri())

        if self._properties:
            properties = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}properties")
            for prop in self._properties.values():
                property_element = ET.SubElement(properties, "{http://www.ivoa.net/xml/VOSpace/v2.1}property")
                property_element.set('uri', prop.uri)
                property_element.set('readOnly', str(prop.read_only).lower())
                property_element.text = str(prop.value)
                if isinstance(prop, DeleteProperty):
                    property_element.set('{http://www.w3.org/2001/XMLSchema-instance}nil', 'true')
        return root


class LinkNode(Node):
    """
    LinkNode representation for the VOSpace.

    :param path: space path. e.g. node/container/data1
    :param properties: list of :func:`Property <pyvospace.core.model.Property>`
    :param capabilities: capabilities of the space.
    :param owner: server side only property.
    :param group_read: server side only property.
    :param group_write: server side only property.
    :param id: server side only property.
    """
    def __init__(self, path, uri_target, properties=None, capabilities=None,
                 owner=None, group_read=None, group_write=None, id=None):
        super().__init__(path=path, properties=properties, capabilities=capabilities,
                         owner=owner, group_read=group_read, group_write=group_write, id=id)
        self.node_uri_target = uri_target

    @property
    def target(self):
        return self.node_uri_target

    @property
    def node_type(self):
        return NodeType.LinkNode

    def node_type_text(self):
        return 'vos:LinkNode'

    def __eq__(self, other):
        if not isinstance(other, LinkNode):
            return False

        return super().__eq__(other) and \
               self.node_uri_target == other.node_uri_target

    def tolist(self):
        return [self.node_type, self.name, self.path, self.owner,
                self.group_read, self.group_write, self.target, self.id]

    def build_node(self, root):
        super().build_node(root)
        target = root.find('vos:target', Node.NS)
        if target is None:
            raise InvalidXML('vos:target target does not exist.')
        self.node_uri_target = target.text

    def tostring(self):
        root = super().toxml()
        ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}target").text = self.node_uri_target
        return ET.tostring(root).decode("utf-8")


class DataNode(Node):
    """
    DataNode representation for the VOSpace.

    :param path: space path. e.g. node/container/data1
    :param properties: list of :func:`Property <pyvospace.core.model.Property>`
    :param capabilities: capabilities of the space.
    :param accepts: list of :func:`View <pyvospace.core.model.View>`
    :param provides: list of :func:`View <pyvospace.core.model.View>`
    :param busy: set node to busy.
    :param owner: server side only property.
    :param group_read: server side only property.
    :param group_write: server side only property.
    :param id: server side only property.
    """
    def __init__(self, path, properties=None, capabilities=None,
                 accepts=None, provides=None, busy=False,
                 owner=None, group_read=None, group_write=None, id=None):
        super().__init__(path=path, properties=properties, capabilities=capabilities,
                         owner=owner, group_read=group_read, group_write=group_write, id=id)
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

    def node_type_text(self):
        return 'vos:DataNode'

    def build_node(self, root):
        super().build_node(root)
        for view in root.xpath('/vos:node/vos:accepts/vos:view', namespaces=Node.NS):
            view_uri = view.attrib.get('uri', None)
            if view_uri is None:
                raise InvalidXML("Accepts URI does not exist.")
            self._accepts.append(View(view_uri))

        for view in root.xpath('/vos:node/vos:provides/vos:view', namespaces=Node.NS):
            view_uri = view.attrib.get('uri', None)
            if view_uri is None:
                raise InvalidXML("Provides URI does not exist.")
            self._provides.append(View(view_uri))

    @property
    def busy(self):
        return self._busy

    @property
    def accepts(self):
        return self._accepts

    @accepts.setter
    def accepts(self, value):
        if value is None:
            self._accepts = []
            return
        if not isinstance(value, list):
            raise InvalidArgument('invalid list')
        for val in value:
            if not isinstance(val, View):
                raise InvalidArgument('invalid View')
        self._accepts = copy.deepcopy(value)

    def add_accepts_view(self, value):
        if not isinstance(value, View):
            raise InvalidArgument('invalid View')
        self._accepts.append(value)

    @property
    def provides(self):
        return self._provides

    @provides.setter
    def provides(self, value):
        if value is None:
            self._provides = []
            return
        if not isinstance(value, list):
            raise InvalidArgument('invalid list')
        for val in value:
            if not isinstance(val, View):
                raise InvalidArgument('invalid View')
        self._provides = copy.deepcopy(value)

    def add_provides_view(self, value):
        if not isinstance(value, View):
            raise InvalidArgument('invalid View')
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
    """
    ContainerNode representation for the VOSpace.
    This Node can contain other :func:`Node <pyvospace.core.model.Node>` which can represent a tree structure.

    :param path: space path. e.g. node/container/data1
    :param nodes: list of :func:`Node <pyvospace.core.model.Node>`
    :param properties: list of :func:`Property <pyvospace.core.model.Property>`
    :param capabilities: capabilities of the space.
    :param accepts: list of :func:`View <pyvospace.core.model.View>`
    :param provides: list of :func:`View <pyvospace.core.model.View>`
    :param busy: set node to busy.
    :param owner: server side only property.
    :param group_read: server side only property.
    :param group_write: server side only property.
    :param id: server side only property.
    """
    def __init__(self, path, nodes=None, properties=None, capabilities=None,
                 accepts=None, provides=None, busy=False, owner=None,
                 group_read=None, group_write=None, id=None):
        super().__init__(path=path, properties=properties, capabilities=capabilities,
                         accepts=accepts, provides=provides, busy=busy,
                         owner=owner, group_read=group_read, group_write=group_write, id=id)
        self._nodes = OrderedDict()
        self.nodes = nodes

    def __eq__(self, other):
        if not isinstance(other, ContainerNode):
            return False
        return super().__eq__(other) and self._nodes == other._nodes

    @property
    def nodes(self):
        return list(self._nodes.values())

    @property
    def node_type(self):
        return NodeType.ContainerNode

    def node_type_text(self):
        return 'vos:ContainerNode'

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
            self.add_node(node)

    def check_path(self, child):
        if not isinstance(child, Node):
            raise InvalidArgument('invalid Node')
        if self.path != child.dirname:
            raise InvalidArgument(f"{self.path} is not a parent of {child.path}")

    @nodes.setter
    def nodes(self, value):
        if not value:
            return
        if not isinstance(value, list):
            raise InvalidArgument('invalid list')
        self._nodes = OrderedDict()
        for node in value:
            self.add_node(node)

    def _insert_node_into_tree(self, parent_node, path_split, node_to_insert, overwrite):
        while path_split:
            name = path_split.pop(0)
            node = parent_node._nodes.get(name)
            if node:
                if isinstance(node, ContainerNode) and path_split:
                    return self._insert_node_into_tree(node, path_split, node_to_insert, overwrite)
                else:
                    if overwrite:
                        parent_node.add_node(node_to_insert)
                        if path_split:
                            raise InvalidArgument('path_split should be empty')
                        break
                    raise InvalidArgument(f'duplicate node {node_to_insert.path}')
            else:
                # its a leaf
                if len(path_split) == 0:
                    parent_node.add_node(node_to_insert)
                    if path_split:
                        raise InvalidArgument('path_split should be empty')
                    break
                # there is no node in parent but the node is not a leaf, so there is no path
                else:
                    raise InvalidArgument(f'no path to {node_to_insert.path}')

    def insert_node_into_tree(self, node, overwrite=False):
        if not node.path.startswith(self.path):
            raise InvalidArgument('Invalid node path.')
        if node.path == self.path:
            raise InvalidArgument('Can not insert over root node.')
        # normalise the node path relative to root path
        node_path = node.path[len(self.path):]
        path_split = list(filter(None, node_path.split('/')))
        return self._insert_node_into_tree(self, path_split, node, overwrite)

    def add_node(self, node, overwrite=True):
        self.check_path(node)
        if not overwrite:
            if self._nodes.get(node.name):
                raise InvalidArgument(f'duplicate node {node.path}')
        self._nodes[node.name] = copy.deepcopy(node)

    def tostring(self):
        root = super().toxml()
        nodes_element = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}nodes")
        for node in self.nodes:
            node_element = ET.SubElement(nodes_element, '{http://www.ivoa.net/xml/VOSpace/v2.1}node')
            node_element.set('uri', node.to_uri())
            node_element.set("{http://www.w3.org/2001/XMLSchema-instance}type", node.node_type_text())
        return ET.tostring(root).decode("utf-8")


class UnstructuredDataNode(DataNode):
    """
    UnstructuredDataNode representation for the VOSpace.

    :param path: space path. e.g. node/container/data1
    :param properties: list of :func:`Property <pyvospace.core.model.Property>`
    :param capabilities: capabilities of the space.
    :param accepts: list of :func:`View <pyvospace.core.model.View>`
    :param provides: list of :func:`View <pyvospace.core.model.View>`
    :param busy: set node to busy.
    :param owner: server side only property.
    :param group_read: server side only property.
    :param group_write: server side only property.
    :param id: server side only property.
    """
    def __init__(self, path, properties=None, capabilities=None, accepts=None, provides=None, busy=False,
                 owner=None, group_read=None, group_write=None, id=None):
        super().__init__(path=path, properties=properties, capabilities=capabilities,
                         accepts=accepts, provides=provides, busy=busy,
                         owner=owner, group_read=group_read, group_write=group_write, id=id)

    def __eq__(self, other):
        if not isinstance(other, UnstructuredDataNode):
            return False
        return super().__eq__(other)

    @property
    def node_type(self):
        return NodeType.UnstructuredDataNode

    def node_type_text(self):
        return 'vos:UnstructuredDataNode'

    def build_node(self, root):
        super().build_node(root)


class StructuredDataNode(DataNode):
    """
    StructuredDataNode representation for the VOSpace.

    :param path: space path. e.g. node/container/data1
    :param properties: list of :func:`Property <pyvospace.core.model.Property>`
    :param capabilities: capabilities of the space.
    :param accepts: list of :func:`View <pyvospace.core.model.View>`
    :param provides: list of :func:`View <pyvospace.core.model.View>`
    :param busy: set node to busy.
    :param owner: server side only property.
    :param group_read: server side only property.
    :param group_write: server side only property.
    :param id: server side only property.
    """
    def __init__(self, path, properties=None, capabilities=None,
                 accepts=None, provides=None, busy=False, owner=None,
                 group_read=None, group_write=None, id=None):
        super().__init__(path=path, properties=properties, capabilities=capabilities,
                         accepts=accepts, provides=provides, busy=busy,
                         owner=owner, group_read=group_read, group_write=group_write, id=id)

    def __eq__(self, other):
        if not isinstance(other, StructuredDataNode):
            return False
        return super().__eq__(other)

    @property
    def node_type(self):
        return NodeType.StructuredDataNode

    def node_type_text(self):
        return 'vos:StructuredDataNode'

    def build_node(self, root):
        super().build_node(root)


class Transfer(object):
    """
    Base class for a VOSpace Transfer request.
    """
    def __init__(self, target, direction):
        self.target = target
        self._direction = direction

    @property
    def target(self):
        return self._target

    @target.setter
    def target(self, value):
        self._target = value

    @property
    def direction(self):
        return self._direction

    def build_node(self, root):
        return NotImplementedError()

    def toxml(self):
        root = ET.Element("{http://www.ivoa.net/xml/VOSpace/v2.1}transfer", nsmap=Node.NS)
        target = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}target")
        if isinstance(self.target, ContainerNode):
            target.text = f"{self.target.path}/"
        else:
            target.text = str(self.target)
        direction = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}direction")
        if isinstance(self.direction, ContainerNode):
            direction.text = f"{self.direction.path}/"
        else:
            direction.text = str(self.direction)
        return root

    def tomap(self):
        return {}

    def tostring(self):
        root = self.toxml()
        return ET.tostring(root).decode("utf-8")

    @classmethod
    def create_transfer(cls, target, direction, keep_bytes):
        if not target:
            raise InvalidArgument('target is empty')
        if not direction:
            raise InvalidArgument('direction is empty')

        if direction == 'pushToVoSpace':
            if target.endswith('/'):
                node = ContainerNode(target)
            else:
                node = Node(target)
            return PushToSpace(node)
        elif direction == 'pullFromVoSpace':
            if target.endswith('/'):
                node = ContainerNode(target)
            else:
                node = Node(target)
            return PullFromSpace(node)
        elif direction == 'pushFromVoSpace':
            raise NotImplementedError()
        elif direction == 'pullToVoSpace':
            raise NotImplementedError()
        else:
            if target.endswith('/'):
                tnode = ContainerNode(target)
            else:
                tnode = Node(target)

            if direction.endswith('/'):
                dnode = ContainerNode(direction)
            else:
                dnode = Node(direction)

            if keep_bytes:
                return Copy(tnode, dnode)
            return Move(tnode, dnode)

    @classmethod
    def fromroot(cls, root):
        target = root.xpath('/vos:transfer/vos:target', namespaces=Node.NS)
        if not target:
            raise InvalidXML('vos:target not found')
        direction = root.xpath('/vos:transfer/vos:direction', namespaces=Node.NS)
        if not direction:
            raise InvalidXML('vos:direction not found')
        keep_bytes = root.xpath('/vos:transfer/vos:keepBytes', namespaces=Node.NS)
        if keep_bytes:
            if keep_bytes[0].text == 'false':
                keep_bytes = False
            elif keep_bytes[0].text == 'true':
                keep_bytes = True
            else:
                raise InvalidXML('keepBytes invalid')
        node = Transfer.create_transfer(target[0].text, direction[0].text, keep_bytes)
        node.build_node(root)
        return node

    @classmethod
    def fromstring(cls, xml):
        root = ET.fromstring(xml)
        return Transfer.fromroot(root)


class NodeTransfer(Transfer):
    """
    Base class for a VOSpace node transfer request (Copy or Move).

    :param target: source :func:`Node <pyvospace.core.model.Node>`
    :param direction: target :func:`Node <pyvospace.core.model.Node>`
    """
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
    """
    Copy for VOSpace request.

    :param target: source :func:`Node <pyvospace.core.model.Node>`
    :param direction: target :func:`Node <pyvospace.core.model.Node>`
    """
    def __init__(self, target, direction):
        super().__init__(target=target,
                         direction=direction,
                         keep_bytes=True)

    def build_node(self, root):
        pass


class Move(NodeTransfer):
    """
    Move for VOSpace request.

    :param target: source :func:`Node <pyvospace.core.model.Node>`
    :param direction: target :func:`Node <pyvospace.core.model.Node>`
    """
    def __init__(self, target, direction):
        super().__init__(target=target,
                         direction=direction,
                         keep_bytes=False)

    def build_node(self, root):
        pass


class ProtocolTransfer(Transfer):
    """
    Base class for VOSpace protocol transfer request.

    :param target: push data to :func:`Node <pyvospace.core.model.Node>`
    :param direction: pushToVoSpace or pullFromVoSpace string
    :param protocols:  :func:`Protocol <pyvospace.core.model.Protocol>` to use for transfer
    :param view: :func:`View <pyvospace.core.model.View>`
    :param params: list of :func:`Parameter <pyvospace.core.model.Parameter>`
    """
    def __init__(self, target, direction, protocols=None, view=None, params=None):
        super().__init__(target=target, direction=direction)
        self._protocols = []
        self.set_protocols(protocols)
        self._view = None
        self.view = view
        self._parameters = []
        self.set_parameters(params)

    @property
    def view(self):
        return self._view

    @view.setter
    def view(self, value):
        if value:
            if not isinstance(value, View):
                raise InvalidArgument('invalid View')
            self._view = value

    @property
    def protocols(self):
        return self._protocols

    def set_protocols(self, protocol_list):
        if protocol_list is None:
            self._protocols = []
            return
        if not isinstance(protocol_list, list):
            raise InvalidArgument('invalid list')
        for protocol in protocol_list:
            if not isinstance(protocol, Protocol):
                raise InvalidArgument('invalid Protocol')
        self._protocols = copy.deepcopy(protocol_list)

    @property
    def parameters(self):
        return self._parameters

    def set_parameters(self, params):
        if params is None:
            self._parameters = []
            return
        if not isinstance(params, list):
            raise InvalidArgument('invalid list')
        for param in params:
            if not isinstance(param, Parameter):
                raise InvalidArgument('invalid Parameter')
        self._parameters = copy.deepcopy(params)

    def tomap(self):
        params = {"TARGET": str(self.target),
                  "DIRECTION": str(self.direction),
                  "PROTOCOL": str(self.protocols[0])}
        if self.view:
            params["VIEW"] = str(self.view)
        if isinstance(self, PullFromSpace):
            if self.redirect:
                params["REQUEST"] = "redirect"
        return params

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
            if protocol.security_method:
                security_tag = ET.SubElement(protocol_elem, "{http://www.ivoa.net/xml/VOSpace/v2.1}securityMethod")
                security_tag.set('uri', str(protocol.security_method))
        for param in self._parameters:
            param_elem = ET.SubElement(root, "{http://www.ivoa.net/xml/VOSpace/v2.1}param")
            param_elem.set('uri', str(param))
            param_elem.text = str(param.value)
        return ET.tostring(root).decode("utf-8")

    def build_node(self, root):
        view_elem = root.xpath('/vos:transfer/vos:view', namespaces=Node.NS)
        if view_elem:
            view_uri = view_elem[0].attrib.get('uri', None)
            self._view = View(view_uri)

        protocols = root.xpath('/vos:transfer/vos:protocol', namespaces=Node.NS)
        for protocol in protocols:
            protocol_uri = protocol.attrib.get('uri', None)

            # Security Methods
            security_obj = None
            security_elem = protocol.xpath('vos:securityMethod', namespaces=Node.NS)
            if security_elem:
                security_uri = security_elem[0].attrib.get('uri', None)
                if security_uri is None:
                    raise InvalidXML('vos:securityMethod URI does not exist.')
                security_obj = SecurityMethod(security_uri)

            # Endpoint
            endpoint = None
            endpoint_elem = protocol.xpath('vos:endpoint', namespaces=Node.NS)
            if endpoint_elem:
                endpoint = Endpoint(endpoint_elem[0].text)

            if protocol_uri == 'ivo://ivoa.net/vospace/core#httpput':
                protocol_obj = HTTPPut(endpoint, security_obj)
            elif protocol_uri == 'ivo://ivoa.net/vospace/core#httpget':
                protocol_obj = HTTPGet(endpoint, security_obj)
            elif protocol_uri == 'ivo://ivoa.net/vospace/core#httpsput':
                protocol_obj = HTTPSPut(endpoint, security_obj)
            elif protocol_uri == 'ivo://ivoa.net/vospace/core#httpsget':
                protocol_obj = HTTPSGet(endpoint, security_obj)
            else:
                protocol_obj = Protocol(endpoint, security_obj)
            self._protocols.append(protocol_obj)

        params = root.xpath('/vos:transfer/vos:param', namespaces=Node.NS)
        for param in params:
            param_uri = param.attrib.get('uri', None)
            self._parameters.append(Parameter(param_uri, param.text))


class PushToSpace(ProtocolTransfer):
    """
    Push to VOSpace transfer request.

    :param target: push data to :func:`Node <pyvospace.core.model.Node>`
    :param protocols:  :func:`Protocol <pyvospace.core.model.Protocol>` to use for transfer
    :param view: :func:`View <pyvospace.core.model.View>`
    :param params: list of :func:`Parameter <pyvospace.core.model.Parameter>`
    """
    def __init__(self, target, protocols=None, view=None, params=None):
        super().__init__(target=target, direction='pushToVoSpace',
                         protocols=protocols, view=view, params=params)


class PullFromSpace(ProtocolTransfer):
    """
    Pull from VOSpace transfer request.

    :param target: pull data from :func:`Node <pyvospace.core.model.Node>`
    :param protocols: :func:`Protocol <pyvospace.core.model.Protocol>` to use for transfer
    :param view: :func:`View <pyvospace.core.model.View>`
    :param params: list of :func:`Parameter <pyvospace.core.model.Parameter>`
    """
    def __init__(self, target, protocols=None, view=None, params=None, redirect=True):
        super().__init__(target=target, direction='pullFromVoSpace',
                         protocols=protocols, view=view, params=params)
        self.redirect = redirect


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

UWSPhaseLookup = {0: 'PENDING',
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
            if not id:
                raise InvalidXML('id is empty')
            del result.attrib['id']
            result_set.append(UWSResult(id, result.attrib))
        return result_set

    @classmethod
    def fromstring(cls, xml):
        root = ET.fromstring(xml)
        return UWSResult.fromroot(root)


class UWSJob(object):
    """
    UWS Job representation.

    :param job_id: job id.
    :param phase: job :func:`Phase <pyvospace.core.model.UWSPhase>`
    :param destruction: distruction time.
    :param job_info: job info :func:`Transfer <pyvospace.core.model.Transfer>`.
    :param results: job :func:`Result <pyvospace.core.model.UWSResult>`
    :param error: job errors.
    """

    NS = {'uws': 'http://www.ivoa.net/xml/UWS/v1.0',
          'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
          'xlink': 'http://www.w3.org/1999/xlink',
          'vos': 'http://www.ivoa.net/xml/VOSpace/v2.1'}

    def __init__(self, job_id, phase, destruction, job_info=None, results=None, error=None):
        self.job_id = str(job_id)
        self.phase = phase
        self.destruction = destruction
        if job_info:
            if not isinstance(job_info, Transfer):
                raise InvalidArgument('invalid Transfer')
        self.job_info = job_info
        self._results = None
        self.results = results
        self.error = error
        self.transfer = None
        self.owner = None
        self.node_path_modified = None

    @property
    def results(self):
        return self._results

    @results.setter
    def results(self, value):
        if value:
            if not isinstance(value, list):
                raise InvalidArgument('invalid list')
            for result in value:
                if not isinstance(result, UWSResult):
                    raise InvalidArgument('invalid UWSResult')
                self._results = copy.deepcopy(value)

    def toxml(self):
        root = ET.Element("{http://www.ivoa.net/xml/UWS/v1.0}job", nsmap=UWSJob.NS)
        ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}jobId').text = self.job_id
        owner_element = ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}ownerId')
        owner_element.set('{http://www.w3.org/2001/XMLSchema-instance}nil', 'true')
        ET.SubElement(root, '{http://www.ivoa.net/xml/UWS/v1.0}phase').text = UWSPhaseLookup[self.phase]
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
        if not root_elem:
            raise InvalidXML('uws:jobId does not exist')
        job_id = root_elem[0].text
        phase_elem = root.xpath('/uws:job/uws:phase', namespaces=UWSJob.NS)
        if not phase_elem:
            raise InvalidXML('uws:phase does not exist')
        phase = phase_elem[0].text
        destruction_elem = root.xpath('/uws:job/uws:destruction', namespaces=UWSJob.NS)
        if not destruction_elem:
            raise InvalidXML('uws:phase does not exist')
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


class Storage(object):

    def __init__(self, storage_id, space_name, host, port, parameters, https, enabled):
        if not storage_id:
            raise InvalidArgument('storage_id is empty.')
        self.storage_id = storage_id
        if not space_name:
            raise InvalidArgument('space_name is empty.')
        self.space_name = space_name
        if not host:
            raise InvalidArgument('host is empty.')
        self.host = host
        if not port:
            raise InvalidArgument('port is empty.')
        self.port = port
        self.parameters = parameters
        self.https = https
        self.enabled = enabled