import os
import asyncpg

import lxml.etree as ET

from urllib.parse import urlparse
from collections import namedtuple
from xml.etree.ElementTree import ParseError
from contextlib import suppress

from .exception import VOSpaceError, PermissionDenied


CreateResponse = namedtuple('CreateResponse', 'node_name '
                                              'node_type_text '
                                              'node_busy '
                                              'node_link '
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
        node_property_array.append(f'<vos:property uri="{prop[0]}" '
                                   f'readOnly="{"true" if prop[2] else "false"}">'
                                   f'{prop[1]}</vos:property>')
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
                           node_container=[],
                           node_target=None):
    xml = '<vos:node xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' \
          ' xmlns="http://www.ivoa.net/xml/VOSpace/v2.1"' \
          ' xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1"' \
          f' xsi:type="{node_type}" uri="vos://{space_name}!vospace/{node_path}" ' \
          f'busy="{"true" if node_busy else "false"}" >' \
          f'{ "<vos:target>"+node_target+"</vos:target>" if node_target else ""}' \
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


async def get_node(conn, path, space_id):
    # remove empty entries as a result of strip
    path_list = list(filter(None, path.split('/')))

    if len(path_list) == 0:
        raise VOSpaceError(400, "Invalid URI. Path is empty")

    path_parent = path_list[:-1]
    path_parent_tree = '.'.join(path_parent)
    path_tree = '.'.join(path_list)

    # share lock both node and parent, important so we
    # dont have a dead lock with move/copy/create
    result = await conn.fetch("select * from nodes where path=$1 or path=$2 "
                              "and space_id=$3 order by path asc for update",
                              path_tree, path_parent_tree, space_id)
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


async def _get_node_request(app, path, params):

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
        async with conn.transaction(isolation='read_committed'):
            results = await conn.fetch("select * from nodes where path <@ $1 and "
                                       "nlevel(path)-nlevel($1)<=1 and space_id=$2"
                                       f"order by path asc for share {limit_str}",
                                       path_tree, app['space_id'])
            if len(results) == 0:
                raise VOSpaceError(404, f"Node Not Found. {path} not found.")

            if detail != 'min':
                properties = await conn.fetch("select * from properties "
                                              "where node_path=$1 and space_id=$2",
                                              results[0]['path'], app['space_id'])
    target = results[0]['link']
    busy = results[0]['busy']
    node_type_int = results[0]['type']
    node_type = NodeTextLookup[node_type_int]
    if NodeType.Node <= node_type_int <= NodeType.ContainerNode:
        if detail == 'max':
            node_type_text = NodeTextLookup[node_type_int]
            views = app['accepts_views'].get(node_type_text, [])
            provides = app['provides_views'].get(node_type_text, [])

    # remove root element in tree so we can output children
    results.pop(0)
    if limit:
        results = results[:int(limit)]

    xml_response = generate_node_response(space_name=app['uri'],
                                          node_path=path,
                                          node_type=node_type,
                                          node_busy=busy,
                                          node_property=properties,
                                          node_accepts_views=views,
                                          node_provides_views=provides,
                                          node_container=results,
                                          node_target=target)
    return xml_response


async def delete_node(app, path):

    path_array = list(filter(None, path.split('/')))
    path_tree = '.'.join(path_array)

    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            result = await conn.fetch("delete from nodes where "
                                      "path <@ $1 and space_id=$2 returning path, type",
                                      path_tree, app['space_id'])
            if not result:
                raise VOSpaceError(404, f"Node Not Found. {path} not found.")

            with suppress(OSError):
                await app['abstract_space'].delete_storage_node(app, result[0]['type'], '/'.join(path_array))


async def _create_node_request(app, xml_text, url_path):
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

    node_type = NodeType.Node
    node_type_text = root.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type', None)
    if node_type_text:
        node_type = NodeLookup.get(node_type_text, None)
        if node_type is None:
            raise VOSpaceError(400, "Type Not Supported. Invalid type.")

    # get link if it exists
    node_target = root.find('{http://www.ivoa.net/xml/VOSpace/v2.1}target', NS)
    if node_target is not None:
        node_target = node_target.text

    props = []
    for properties in root.findall('vos:properties', NS):
        for node_property in properties.findall('vos:property', NS):
            prop_uri = node_property.attrib.get('uri', None)
            if prop_uri is not None:
                prop_uri = prop_uri.lower()
                if prop_uri in app['readonly_properties']:
                    raise PermissionDenied(f'Permission Denied. uri: {prop_uri}.')
                props.append([prop_uri, node_property.text, False])

    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            return await create_node(app, conn, uri_path_norm, node_type, props, node_target)


async def create_node(app, conn, uri_path, node_type, properties=None, node_target=None):
    space_id = app['space_id']
    try:
        node_type_text = NodeTextLookup.get(node_type, None)
        if node_type_text is None:
            raise VOSpaceError(400, "Type Not Supported. Invalid type.")

        # We can not have a target unless its a link node
        if node_type != NodeType.LinkNode and node_target is not None:
            raise VOSpaceError(400, f"Type Not Supported. {node_type_text} can not have a target.")

        if node_type == NodeType.LinkNode:
            if node_target is None:
                raise VOSpaceError(400, f"Type Not Supported. {node_type_text} does not have a target.")

        # remove empty entries as a result of strip
        path = list(filter(None, uri_path.split('/')))

        if len(path) == 0:
            raise VOSpaceError(400, "Invalid URI. Path is empty")

        path_parent = path[:-1]
        node_name = path[-1]

        path_parent_tree = '.'.join(path_parent)
        path_tree = '.'.join(path)

        # get parent node and check if its valid to add node to it
        row = await conn.fetchrow("SELECT type, name, path, nlevel(path) "
                                  "FROM nodes WHERE path=$1 and space_id=$2 for update",
                                  path_parent_tree, space_id)
        # if the parent is not found but its expected to exist
        if not row and len(path_parent) > 0:
            raise VOSpaceError(404, f"Container Not Found. {'/'.join(path_parent)} not found.")

        if row:
            if row['type'] == NodeType.LinkNode:
                raise VOSpaceError(400, f"Link Found. {row['name']} found in path.")

            if row['type'] != NodeType.ContainerNode:
                raise VOSpaceError(404, f"Container Not Found. {row['name']} is not a container.")

        await conn.fetchrow("INSERT INTO nodes (type, name, path, space_id, link) VALUES ($1, $2, $3, $4, $5)",
                             node_type, node_name, path_tree, space_id, node_target)

        # call the specific provider, opportunity to get properties
        await app['abstract_space'].create_storage_node(app, node_type, '/'.join(path))

        if properties:
            for prop in properties:
                prop.append(path_tree)
                prop.append(space_id)

            await conn.executemany("INSERT INTO properties (uri, value, read_only, node_path, space_id) "
                                   "VALUES ($1, $2, $3, $4, $5)",
                                   properties)

        return CreateResponse(node_name, node_type_text, False, node_target, properties)

    except ParseError as p:
        raise VOSpaceError(500, f"Internal Error. XML error: {str(p)}.")

    except asyncpg.exceptions.UniqueViolationError as f:
        raise VOSpaceError(409, f"Duplicate Node. {node_name} already exists.")


async def set_node_properties(app, xml_text, url_path):
    space_id = app['space_id']
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
                    if prop_uri in app['readonly_properties']:
                        raise PermissionDenied(f'Permission Denied. uri: {prop_uri}.')
                    if prop_nil == 'true':
                        node_props_delete.append(prop_uri)
                    else:
                        node_props_insert.append([prop_uri,
                                                  node_property.text,
                                                  False,
                                                  node_path_tree,
                                                  space_id])

        async with app['db_pool'].acquire() as conn:
            async with conn.transaction():
                results = await conn.fetchrow("select * from nodes where path=$1 "
                                              "and type=$2 and space_id=$3 for update",
                                              node_path_tree, node_type, space_id)
                if not results:
                    raise VOSpaceError(404, f"Node Not Found. {url_path} not found.")

                # if a property already exists then update it, only if read_only = False
                await conn.executemany("INSERT INTO properties (uri, value, read_only, node_path, space_id) "
                                       "VALUES ($1, $2, $3, $4, $5) on conflict (uri, node_path, space_id) "
                                       "do update set value=$2 where properties.read_only=False "
                                       "and properties.value!=$2",
                                       node_props_insert)

                # only delete properties where read_only=False
                await conn.fetch("DELETE FROM properties WHERE uri=any($1::text[]) "
                                 "AND node_path=$2 and space_id=$3 and read_only=False",
                                 node_props_delete, node_path_tree, space_id)

                properties_result = await conn.fetch("select * from properties "
                                                     "where node_path=$1 and space_id=$2",
                                                     node_path_tree, space_id)

        xml_response = generate_node_response(space_name=app['uri'],
                                              node_path=url_path,
                                              node_type=NodeTextLookup[node_type],
                                              node_busy=results['busy'],
                                              node_property=properties_result,
                                              node_target=results['link'])
        return xml_response

    except ParseError as p:
        raise VOSpaceError(500, f"Internal Error. XML error: {str(p)}.")

    except asyncpg.exceptions.UniqueViolationError as f:
        raise VOSpaceError(409, f"Duplicate Node. {url_path} already exists.")


async def delete_properties(conn, uri_path, space_id):
    path = list(filter(None, uri_path.split('/')))
    path_tree = '.'.join(path)

    await conn.execute("delete from properties where node_path=$1 and space_id=$2",
                       path_tree, space_id)