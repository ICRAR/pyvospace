
class Property(object):

    def __init__(self, uri, value, read_only):
        self.uri = uri
        self.value = value
        self.read_only = read_only

    def __str__(self):
        return f'<vos:property uri="{self.uri}" ' \
               f'readOnly="{self.read_only}">{self.value}</vos:property>'


class Capability(object):

    def __init__(self, uri, endpoint, param):
        self.uri = uri
        self.endpoint = endpoint
        self.param = param


class View(object):

    def __init__(self, uri, param, original):
        self.uri = uri
        self.param = param
        self.original = original


class Node(object):

    def __init__(self,
                 uri,
                 properties=[],
                 capabilities=[],
                 node_type='vos:Node'):
        self.node_type = node_type
        self.uri = uri
        self.properties = properties
        self.capabilities = capabilities

    @property
    def properties(self):
        return self.__properties

    @properties.setter
    def properties(self, value):
        if value is None:
            self.__properties = []
        else:
            self.__properties = value

    def add_property(self, property):
        self.__properties.append(property)

    def _properties_to_xml(self):
        return ''.join([str(prop) for prop in self.__properties])

    def __str__(self):
        create_node_xml = f'<vos:node xmlns:xsi="http://www.w3.org/2001/XMLSchema"' \
                          f' xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1"' \
                          f' xsi:type="{self.node_type}"' \
                          f' uri="{self.uri}">' \
                          f'<vos:properties>' \
                          f'{self._properties_to_xml()}' \
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
        self.accepts = accepts
        self.provides = provides
        self.busy = busy


class ContainerNode(DataNode):

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
                         node_type='vos:ContainerNode')


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

