import os
import asyncpg

import lxml.etree as ET

from urllib.parse import urlparse
from collections import namedtuple
from xml.etree.ElementTree import ParseError

from .exception import VOSpaceError


Create_Response = namedtuple('CreateResponse', 'node_name '
                                               'node_type_text '
                                               'node_busy '
                                               'node_properties ')

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

Property = ['ivo://ivoa.net/vospace/core#description',
            'ivo://ivoa.net/vospace/core#title']

Views = ['ivo://ivoa.net/vospace/core#anyview']
Provides = ['ivo://ivoa.net/vospace/core#binaryview']

NS = {'vos': 'http://www.ivoa.net/xml/VOSpace/v2.1'}


def generate_view_xml(node_views):
    if not node_views:
        return ''

    node_view_array = []
    for view in node_views:
        node_view_array.append(f'<vos:view uri="{view}"/>')
    return ''.join(node_view_array)


def generate_property_xml(node_property):
    if not node_property:
        return ''

    node_property_array = []
    for prop in node_property:
        uri = prop['uri']
        value = prop['value']
        ro = prop['read_only']
        node_property_array.append(f'<vos:property uri="{uri}" '
                                   f'readOnly="{"true" if ro else "false"}">'
                                   f'{value}</vos:property>')
    return ''.join(node_property_array)


def generate_node_summary_xml(space_name, nodes):
    if not nodes:
        return ''

    node_array = []
    for node in nodes:
        uri = node['path'].replace('.', '/')
        uri_str = f"vos://{space_name}!vospace/{uri}"
        node_type = node['type']
        node_array.append(f'<vos:node uri="{uri_str}" xsi:type="{NodeTextLookup[node_type]}"/>')
    return f"<vos:nodes>{''.join(node_array)}</vos:nodes>"


def generate_node_response(space_name,
                           node_path,
                           node_type,
                           node_busy,
                           node_property=[],
                           node_accepts_views=[],
                           node_provides_views=[],
                           node_container=[]):
    xml = '<vos:node xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' \
          ' xmlns="http://www.ivoa.net/xml/VOSpace/v2.1"' \
          ' xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1"' \
          f' xsi:type="{node_type}" uri="vos://{space_name}!vospace/{node_path}" ' \
          f'busy="{"true" if node_busy else "false"}" >' \
          f'<vos:properties>{ generate_property_xml(node_property) }</vos:properties>' \
          f'<vos:accepts>{ generate_view_xml(node_accepts_views) }</vos:accepts>' \
          f'<vos:provides>{ generate_view_xml(node_provides_views) }</vos:provides>' \
          '<vos:capabilities/>' \
          f'{generate_node_summary_xml(space_name, node_container)}' \
          '</vos:node>'
    return xml


def generate_protocol_xml(protocol):
    if not protocol:
        return ''

    protocol_array = []
    for prot in protocol:
        protocol_array.append(f'<vos:protocol uri="{prot}"></vos:protocol>')

    return ''.join(protocol_array)


def generate_protocol_response(accepts, provides):
    xml = '<vos:protocols xmlns="http://www.ivoa.net/xml/VOSpace/v2.1"' \
          ' xmlns:xs="http://www.w3.org/2001/XMLSchema-instance">' \
          f'<vos:accepts>{generate_protocol_xml(accepts)}</vos:accepts>' \
          f'<vos:provides>{generate_protocol_xml(provides)}</vos:provides>' \
          f'</vos:protocols>'
    return xml


async def get_node(conn, path):
    # remove empty entries as a result of strip
    path_list = list(filter(None, path.split('/')))

    if len(path_list) == 0:
        raise VOSpaceError(400, "Invalid URI. Path is empty")

    path_parent = path_list[:-1]
    path_parent_tree = '.'.join(path_parent)
    path_tree = '.'.join(path_list)

    # share lock both node and parent, important so we
    # dont have a dead lock with move/copy/create
    result = await conn.fetch("select * from nodes where path = $1 or path = $2 "
                              "order by path asc for share",
                              path_tree, path_parent_tree)
    result_len = len(result)
    if result_len == 1:
        # If its just one result and we have a parent tree
        # it can not be the node. It has to be just the parent!
        if path_parent_tree:
            assert result[0]['path'] == path_parent_tree

        # If the only result is the node then return it.
        # This assumes its a root node.
        return result[0]
    elif result_len == 2:
        # If there are 2 results the first is the parent
        # the second is the node in question.
        return result[1]
    return None


async def get_node_request(app, path, params):

    detail = params.get('detail', 'max')
    if detail:
        if detail not in ['min', 'max', 'properties']:
            raise VOSpaceError(400, f'Invalid URI. detail invalid: {detail}')

    limit_str = ''
    limit = params.get('limit', None)
    if limit:
        try:
            limit_int = int(limit)
            if limit_int <= 0:
                raise Exception()
            # add +1 to include the root element when doing the limit query
            limit_str = f'limit {limit_int+1}'
        except:
            raise VOSpaceError(400, f'Invalid URI. limit invalid: {limit}')

    path_array = list(filter(None, path.split('/')))

    if len(path_array) == 0:
        raise VOSpaceError(400, "Invalid URI. Path is empty")

    path_tree = '.'.join(path_array)

    properties = []
    views = []
    provides = []

    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            results = await conn.fetch("select * from nodes where path <@ $1 and "
                                       "nlevel(path)-nlevel($1)<=1 "
                                       f"order by path asc for share {limit_str}",
                                       path_tree)
            if len(results) == 0:
                raise VOSpaceError(404, f"Node Not Found. {path} not found.")

            if detail != 'min':
                properties = await conn.fetch("select * from properties where node_path=$1",
                                              results[0]['path'])

    busy = results[0]['busy']
    node_type_int = results[0]['type']
    node_type = NodeTextLookup[node_type_int]
    if NodeType.Node <= node_type_int <= NodeType.ContainerNode:
        if detail == 'max':
            node_type_text = NodeTextLookup[node_type_int]
            views = app['plugin'].get_supported_import_accepts_views(
                path, node_type_text)
            provides = app['plugin'].get_supported_export_provides_views(
                path, node_type_text)

    # remove root element in tree so we can output children
    results.pop(0)
    if limit:
        results = results[:int(limit)]

    xml_response = generate_node_response(space_name=app['space_name'],
                                          node_path=path,
                                          node_type=node_type,
                                          node_busy=busy,
                                          node_property=properties,
                                          node_accepts_views=views,
                                          node_provides_views=provides,
                                          node_container=results)
    return xml_response


async def delete_node(app, path):

    path_array = list(filter(None, path.split('/')))
    path_tree = '.'.join(path_array)

    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            result = await conn.fetch("delete from nodes where path <@ $1 returning path",
                                      path_tree)
    if not result:
        raise VOSpaceError(404, f"Node Not Found. {path} not found.")


async def create_node_request(app, xml_text, url_path):
    root = ET.fromstring(xml_text)
    uri_xml = root.attrib.get('uri', None)

    uri_path_xml = urlparse(uri_xml)

    if not uri_path_xml.path:
        raise VOSpaceError(400, "Invalid URI. URI does not exist.")

    # make sure the request path and the URL are the same
    # exclude '.' characters as they are used in ltree
    if '.' in uri_path_xml.path:
        raise VOSpaceError(400, f"Invalid URI. Invalid character: '.' in URI")

    uri_path_norm = os.path.normpath(uri_path_xml.path).lstrip('/')
    url_path_norm = os.path.normpath(url_path).lstrip('/')

    if url_path_norm != uri_path_norm:
        raise VOSpaceError(400, f"Invalid URI. URI node path does not "
                                f"match request: {url_path} != {uri_path_xml.path}")

    node_type = root.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type', None)

    props = []
    for properties in root.findall('vos:properties', NS):
        for node_property in properties.findall('vos:property', NS):
            prop_uri = node_property.attrib.get('uri', None)
            if prop_uri is not None:
                prop_uri = prop_uri.lower()
                if prop_uri in Property:
                    props.append({'uri': prop_uri,
                                  'value': node_property.text,
                                  'read_only': False})

    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            return await create_node(app, conn, uri_path_norm, node_type, props)


async def create_node(app,
                      conn,
                      uri_path,
                      node_type,
                      properties):
    try:
        if node_type is None:
            node_type = NodeType.Node # if not specified then default is Node
            node_type_text = 'vos:Node'
        else:
            node_type_text = node_type
            node_type = NodeLookup.get(node_type_text, None)
            if node_type is None:
                raise VOSpaceError(400, "Type Not Supported. Invalid type.")

        # remove empty entries as a result of strip
        path = list(filter(None, uri_path.split('/')))

        if len(path) == 0:
            raise VOSpaceError(400, "Invalid URI. Path is empty")

        path_parent = path[:-1]
        node_name = path[-1]

        path_parent_tree = '.'.join(path_parent)
        path_tree = '.'.join(path)

        if properties:
            properties_list = [list(p.values()) for p in properties]

        # get parent node and check if its valid to add node to it
        row = await conn.fetchrow("SELECT type, name, path, nlevel(path) "
                                  "FROM nodes WHERE path=$1 for share",
                                  path_parent_tree)

        # if the parent is not found but its expected to exist
        if not row and len(path_parent) > 0:
            raise VOSpaceError(404, f"Container Not Found. {'/'.join(path_parent)} not found.")

        if row:
            if row['type'] == NodeType.LinkNode:
                raise VOSpaceError(400, f"Link Found. {row['name']} found in path.")

            if row['type'] != NodeType.ContainerNode:
                raise VOSpaceError(404, f"Container Not Found. {row['name']} is not a container.")

        await conn.fetchrow("INSERT INTO nodes (type, name, path) VALUES ($1, $2, $3)",
                             node_type, node_name, path_tree)
        if properties:
            for prop in properties_list:
                prop.append(path_tree)

            await conn.executemany("INSERT INTO properties (uri, value, read_only, node_path) "
                                   "VALUES ($1, $2, $3, $4)",
                                   properties_list)

        return Create_Response(node_name, node_type_text, False, properties)

    except ParseError as p:
        raise VOSpaceError(500, f"Internal Error. XML error: {str(p)}.")

    except asyncpg.exceptions.UniqueViolationError as f:
        raise VOSpaceError(409, f"Duplicate Node. {node_name} already exists.")


async def set_node_properties(app, xml_text, url_path):
    try:
        root = ET.fromstring(xml_text)

        uri = root.attrib.get('uri', None)
        if uri is None:
            raise VOSpaceError(400, "Invalid URI. URI does not exist.")

        node_type = root.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type',
                                    None)
        if node_type is None:
            raise VOSpaceError(400, "Invalid URI. Type does not exist.")

        node_type = NodeLookup.get(node_type, None)
        if node_type is None:
            raise VOSpaceError(400, "Type Not Supported. Invalid type.")

        uri_path = urlparse(uri)

        if '.' in url_path:
            raise VOSpaceError(400, "Invalid URI. Invalid character: '.' in URI")

        # make sure the request path and the URL are the same
        # exclude '.' characters as they are used in ltree
        if url_path != uri_path.path:
            raise VOSpaceError(400, f"Invalid URI. URI node path does not "
                                    f"match request: {url_path} != {uri_path.path}")

        # remove empty entries as a result of strip
        node_url_path = list(filter(None, url_path.split('/')))

        if len(node_url_path) == 0:
            raise VOSpaceError(400, "Invalid URI. Path is empty")

        node_path_tree = '.'.join(node_url_path)

        node_props_insert = []
        node_props_delete = []
        for properties in root.findall('vos:properties', NS):
            for node_property in properties.findall('vos:property', NS):
                prop_uri = node_property.attrib.get('uri', None)
                prop_nil = node_property.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}nil',
                                                    None)
                if prop_uri is not None:
                    prop_uri = prop_uri.lower()
                    if prop_uri in Property:
                        if prop_nil == 'true':
                            node_props_delete.append(prop_uri)
                        else:
                            node_props_insert.append([prop_uri,
                                                      node_property.text,
                                                      True,
                                                      node_path_tree])

        async with app['db_pool'].acquire() as conn:
            async with conn.transaction():
                results = await conn.fetch("select * from nodes where path=$1 and type=$2 for update",
                                           node_path_tree, node_type)
                if len(results) == 0:
                    raise VOSpaceError(404, f"Node Not Found. {url_path} not found.")

                # if a property already exists then update it, only if read_only = False
                await conn.executemany("INSERT INTO properties (uri, value, read_only, node_path) "
                                       "VALUES ($1, $2, $3, $4) on conflict (uri, node_path) "
                                       "do update set value=$2 where properties.read_only=False",
                                       node_props_insert)

                # only delete properties where read_only=False
                await conn.execute("DELETE FROM properties WHERE uri=any($1::text[]) "
                                   "AND node_path=$2 and read_only=False",
                                   node_props_delete, node_path_tree)

                properties_result = await conn.fetch("select * from properties where node_path=$1",
                                                     node_path_tree)

        xml_response = generate_node_response(space_name=app['space_name'],
                                              node_path=url_path,
                                              node_type=NodeTextLookup[node_type],
                                              node_busy=results['busy'],
                                              node_property=properties_result)
        return xml_response

    except ParseError as p:
        raise VOSpaceError(500, f"Internal Error. XML error: {str(p)}.")

    except asyncpg.exceptions.UniqueViolationError as f:
        raise VOSpaceError(409, f"Duplicate Node. {url_path} already exists.")


async def delete_properties(conn, uri_path):
    path = list(filter(None, uri_path.split('/')))
    path_tree = '.'.join(path)

    await conn.execute("delete from properties where node_path=$1", path_tree)