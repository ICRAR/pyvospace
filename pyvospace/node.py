import asyncpg

import xml.etree.ElementTree as ET

from urllib.parse import urlparse
from collections import namedtuple

from .exception import VOSpaceError


Node_Type = namedtuple('NodeType', 'Node '
                                   'DataNode '
                                   'UnstructuredDataNode  '
                                   'StructuredDataNode '
                                   'ContainerNode '
                                   'LinkNode')

NodeLookup = {'vos:node': 0,
              'vos:datanode': 1,
              'vos:unstructureddatanode': 2,
              'vos:structureddatanode': 3,
              'vos:containernode': 4,
              'vos:linknode': 5}

NodeType = Node_Type(0, 1, 2, 3, 4, 5)

Property = ['ivo://ivoa.net/vospace/core#description']

Views = ['ivo://ivoa.net/vospace/core#anyview']

NS = {'vos': 'http://www.ivoa.net/xml/VOSpace/v2.1'}


def generate_accept_xml(node_views):
    node_accept_array = []
    for i in node_views:
        node_accept_array.append(f'<vos:view uri="{i}"/>')
    return ''.join(node_accept_array)


def generate_property_xml(node_property):
    node_property_array = []
    for i in node_property:
        node_property_array.append(f'<vos:property uri="{i[0]}">{i[1]}</vos:property>')
    return ''.join(node_property_array)


def generate_create_node_response(node_path,
                                  node_type,
                                  node_property,
                                  node_views):
    xml = f'<vos:node xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' \
          f' xmlns="http://www.ivoa.net/xml/VOSpace/v2.1"' \
          f' xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1"' \
          f' type="{node_type}"' \
          f' uri="vos://icrar.org!vospace/{node_path}">' \
          f'<vos:properties>{ generate_property_xml(node_property) }</vos:properties>' \
          f'<vos:accepts>{ generate_accept_xml(node_views) }</vos:accepts>' \
          f'<vos:provides/><vos:capabilities/></vos:node>'
    return xml


async def delete_node(db_pool, path):

    path_array = list(filter(None, path.split('/')))
    path_tree = '.'.join(path_array)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            delete_result = await conn.fetch("select * from nodes "
                                             "where path <@ $1 for update",
                                             path_tree)

            if len(delete_result) == 0:
                raise VOSpaceError(404, f"Node Not Found. {path}")

            for row in delete_result:
                if row['type'] == NodeType.LinkNode:
                    raise VOSpaceError(400, f"Link Found. "
                                            f"Link Node {row['name']} found in path.")

            await conn.execute("delete from nodes "
                               "where path <@ $1",
                               path_tree)


async def create_node(db_pool, xml_text, url_path):
    try:
        root = ET.fromstring(xml_text)

        uri = root.attrib.get('uri', None)
        if uri is None:
            raise VOSpaceError(400, "Invalid URI. "
                                    "URI does not exist.")

        node_type = root.attrib.get('type', None)
        if node_type is None:
            node_type = 0 # if not specified then default is Node
        else:
            node_type_text = node_type.lower()
            node_type = NodeLookup.get(node_type_text, None)
            if node_type is None:
                raise VOSpaceError(400, f"Type Not Supported. "
                                        f"Invalid xsi:type.")

        uri_path = urlparse(uri)

        # make sure the request path and the URL are the same
        # exclude '.' characters as they are used in ltree
        if url_path != uri_path.path or '.' in url_path:
            raise VOSpaceError(400, "Invalid URI. "
                                    "URI node path does not "
                                    "match the node path of the HTTP")

        user_props = []
        for properties in root.findall('vos:properties', NS):
            for node_property in properties.findall('vos:property', NS):
                prop_uri = node_property.attrib.get('uri', None)
                if prop_uri is not None:
                    prop_uri = prop_uri.lower()
                    if prop_uri in Property:
                        user_props.append([prop_uri, node_property.text, True])

        url_path = url_path.lower()

        # remove empty entries as a result of strip
        user_path = list(filter(None, url_path.split('/')))

        user_path_parent = user_path[:-1]
        node_name = user_path[-1]

        user_path_parent_tree = '.'.join(user_path_parent)
        user_path_tree = '.'.join(user_path)

        user_path_parent_len = len(user_path_parent)

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.fetch("SELECT id, type, name, path, nlevel(path) "
                                          "FROM nodes WHERE path @> $1 for update",
                                          user_path_parent_tree)

                if user_path_parent_len != len(result):
                    raise VOSpaceError(404, f"Container Not Found. "
                                            f"Container path {url_path} does not exist.")

                for row in result:
                    if row['type'] == NodeType.LinkNode:
                        raise VOSpaceError(400, f"Link Found. "
                                                f"Link Node {row['name']} found in path.")

                    if row['type'] != NodeType.ContainerNode:
                        raise VOSpaceError(404, f"Container Not Found. "
                                                f"{row['name']} is not a container.")

                node_result = await conn.fetchrow(("INSERT INTO nodes (type, name, path) "
                                                   "VALUES ($1, $2, $3) RETURNING id"),
                                                   node_type, node_name, user_path_tree)
                for prop in user_props:
                    prop.append(node_result['id'])

                await conn.executemany(("INSERT INTO properties (uri, value, read_only, node_id) "
                                        "VALUES ($1, $2, $3, $4)"),
                                        user_props)

        xml_response = generate_create_node_response(node_name,
                                                     node_type_text,
                                                     user_props,
                                                     Views)
        return xml_response

    except asyncpg.exceptions.UniqueViolationError as f:
        raise VOSpaceError(409, f"DuplicateNode. {node_name} already exists.")