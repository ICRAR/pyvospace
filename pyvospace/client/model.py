import xml.etree.ElementTree as ET


class Property(object):

    def __init__(self, uri, value, read_only):
        self.uri = uri
        self.value = value
        self.read_only = read_only

    def __eq__(self, other):
        if not isinstance(other, Property):
            return False

        return self.uri == other.uri and \
               self.value == other.value

    def tostring(self):
        return f'<vos:property uri="{self.uri}" ' \
               f'readOnly="{self.read_only}">' \
               f'{self.value}</vos:property>'

    def __str__(self):
        return self.uri, self.value

    def __repr__(self):
        return f'{self.uri, self.value}'


class Capability(object):

    def __init__(self, uri, endpoint, param):
        self.uri = uri
        self.endpoint = endpoint
        self.param = param


class View(object):

    def __init__(self, uri):
        self.uri = uri

    def __eq__(self, other):
        if not isinstance(other, View):
            return False

        return self.uri == other.uri


class Node(object):

    NS = {'vos': 'http://www.ivoa.net/xml/VOSpace/v2.1'}

    def __init__(self,
                 uri,
                 properties=[],
                 capabilities=[],
                 node_type='vos:Node'):
        self.node_type = node_type
        self.uri = uri
        self.properties = properties
        self.capabilities = capabilities

    def __repr__(self):
        return self.uri

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False

        return self.uri == other.uri and \
               self.node_type == other.node_type and \
               self._properties == other._properties

    def _build_node(self, root):
        for properties in root.findall('vos:properties', Node.NS):
            for node_property in properties.findall('vos:property', Node.NS):
                prop_uri = node_property.attrib.get('uri', None)
                prop_ro = node_property.attrib.get('readOnly', None)
                prop_text = node_property.text
                if prop_uri is None:
                    raise Exception("Property URI does not exist.")
                self._properties.append(Property(prop_uri, prop_text, prop_ro))

        self._sort_properties()

    def _sort_properties(self):
        self._properties.sort(key=lambda x: x.uri)

    @classmethod
    def _create_node(cls, node_uri, node_type):
        if node_uri is None:
            raise Exception("Node URI does not exist.")

        if node_type is None:
            raise Exception("Node Type does not exist.")

        if node_type == 'vos:Node':
            return Node(node_uri)

        elif node_type == 'vos:DataNode':
            return DataNode(node_uri)

        elif node_type == 'vos:UnstructuredDataNode':
            return UnstructuredDataNode(node_uri)

        elif node_type == 'vos:StructuredDataNode':
            return StructuredDataNode(node_uri)

        elif node_type == 'vos:ContainerNode':
            return ContainerNode(node_uri)

        elif node_type == 'vos:LinkNode':
            return LinkNode(node_uri, None)

        else:
            raise Exception('Unknown node type.')

    @classmethod
    def fromstring(cls, xml):
        root = ET.fromstring(xml)

        node_uri = root.attrib.get('uri', None)
        node_type = root.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type', None)

        node = Node._create_node(node_uri, node_type)
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

    def tostring(self):
        create_node_xml = f'<vos:node xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' \
                          f' xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1"' \
                          f' xsi:type="{self.node_type}"' \
                          f' uri="{self.uri}">' \
                          f'<vos:properties>' \
                          f'{self._properties_tostring()}' \
                          f'</vos:properties>' \
                          f'<vos:accepts/>' \
                          f'<vos:provides/>' \
                          f'<vos:capabilities/>' \
                          f'</vos:node>'
        return create_node_xml


class LinkNode(Node):

    def __init__(self,
                 uri,
                 uri_target,
                 properties=[],
                 capabilities=[]):
        super().__init__(uri=uri,
                         properties=properties,
                         capabilities=capabilities,
                         node_type='vos:LinkNode')
        self.uri_target = uri_target

    def __eq__(self, other):
        if not isinstance(other, LinkNode):
            return False

        return super().__eq__(other) and \
               self.uri_target == other.uri_target

    def _build_node(self, root):
        super()._build_node(root)

        target = root.attrib.get('target', None)
        if target is None:
            raise Exception('LinkNode target does not exist.')

        self.uri_target = target


class DataNode(Node):

    def __init__(self,
                 uri,
                 properties=[],
                 capabilities=[],
                 accepts=[],
                 provides=[],
                 busy=False,
                 node_type='vos:DataNode'):
        super().__init__(uri=uri,
                         properties=properties,
                         capabilities=capabilities,
                         node_type=node_type)
        self._accepts = accepts
        self._provides = provides
        self.busy = busy

    def __eq__(self, other):
        if not isinstance(other, DataNode):
            return False

        return super().__eq__(other)


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
                 uri,
                 nodes=[],
                 properties=[],
                 capabilities=[],
                 accepts=[],
                 provides=[],
                 busy=False):
        super().__init__(uri=uri,
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

        return super().__eq__(other) and \
               self._nodes == other._nodes

    def _sort_nodes(self):
        self._nodes.sort(key=lambda x: x.uri)

    def _build_node(self, root):
        super()._build_node(root)

        for nodes in root.findall('vos:nodes', Node.NS):
            for node_obj in nodes.findall('vos:node', Node.NS):
                node_uri = node_obj.attrib.get('uri', None)
                node_type = node_obj.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type', None)
                node = Node._create_node(node_uri, node_type)
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
                 uri,
                 properties=[],
                 capabilities=[],
                 accepts=[],
                 provides=[],
                 busy=False):
        super().__init__(uri=uri,
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

    def _build_node(self, root):
        super()._build_node(root)


class StructuredDataNode(DataNode):

    def __init__(self,
                 uri,
                 properties=[],
                 capabilities=[],
                 accepts=[],
                 provides=[],
                 busy=False):
        super().__init__(uri=uri,
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

    def _build_node(self, root):
        super()._build_node(root)


class Transfer(object):

    def __init__(self, target, direction, keep_bytes):
        self.target = target
        self.direction = direction
        self.keep_bytes = keep_bytes

    def tostring(self):
        keep_bytes_str = 'false'
        if self.keep_bytes:
            keep_bytes_str = 'true'

        create_node_xml = f'<vos:transfer xmlns:xs="http://www.w3.org/2001/XMLSchema-instance"' \
                          f' xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1">' \
                          f'<vos:target>{self.target.uri}</vos:target>' \
                          f'<vos:direction>{self.direction.uri}</vos:direction>' \
                          f'<vos:keepBytes>{keep_bytes_str}</vos:keepBytes>' \
                          f'</vos:transfer>'
        return create_node_xml


class Copy(Transfer):

    def __init__(self, target, direction):
        super().__init__(target=target,
                         direction=direction,
                         keep_bytes=True)


class Move(Transfer):

    def __init__(self, target, direction):
        super().__init__(target=target,
                         direction=direction,
                         keep_bytes=False)
