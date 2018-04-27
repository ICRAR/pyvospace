import os
import asyncpg

import lxml.etree as ET

from urllib.parse import urlparse
from collections import namedtuple
from xml.etree.ElementTree import ParseError

from .exception import VOSpaceError
from .uws import UWSPhase


Create_Response = namedtuple('CreateResponse', 'node_name '
                                               'node_type_text '
                                               'node_properties '
                                               'node_import_views '
                                               'node_updated')

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

VOSpaceName = 'vos://icrar.org!vospace'


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


def generate_node_summary_xml(nodes):
    if not nodes:
        return ''

    node_array = []
    for node in nodes:
        uri = node['path'].replace('.', '/')
        uri_str = f"{VOSpaceName}/{uri}"
        node_type = node['type']
        node_array.append(f'<vos:node uri="{uri_str}" '
                          f'xsi:type="{NodeTextLookup[node_type]}"/>')
    return f"<vos:nodes>{''.join(node_array)}</vos:nodes>"


def generate_node_response(node_path,
                           node_type,
                           node_property=[],
                           node_accepts_views=[],
                           node_provides_views=[],
                           node_container=[]):
    xml = f'<vos:node xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' \
          f' xmlns="http://www.ivoa.net/xml/VOSpace/v2.1"' \
          f' xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1"' \
          f' xsi:type="{node_type}"' \
          f' uri="{VOSpaceName}{node_path}">' \
          f'<vos:properties>{ generate_property_xml(node_property) }</vos:properties>' \
          f'<vos:accepts>{ generate_view_xml(node_accepts_views) }</vos:accepts>' \
          f'<vos:provides>{ generate_view_xml(node_provides_views) }</vos:provides>' \
          f'<vos:capabilities/>' \
          f'{generate_node_summary_xml(node_container)}' \
          f'</vos:node>'
    return xml


def generate_protocol_xml(protocol):
    if not protocol:
        return ''

    protocol_array = []
    for prot in protocol:
        protocol_array.append(f'<vos:protocol uri="{prot}"></vos:protocol>')

    return ''.join(protocol_array)


def generate_protocol_response(accepts, provides):
    xml = f'<vos:protocols xmlns="http://www.ivoa.net/xml/VOSpace/v2.1"' \
          f'xmlns:xs="http://www.w3.org/2001/XMLSchema-instance">' \
          f'<vos:accepts>' \
          f'{generate_protocol_xml(accepts)}' \
          f'</vos:accepts>' \
          f'<vos:provides>' \
          f'{generate_protocol_xml(provides)}' \
          f'</vos:provides>' \
          f'</vos:protocols>'
    return xml


async def get_transfer_job(conn, job_id):

    results = await conn.fetchrow(f'select * from uws_jobs inner join '
                                  f'nodes on uws_jobs.target = nodes.path '
                                  f'where uws_jobs.id = $1 '
                                  f'for share of nodes',
                                  job_id)

    if not results:
        raise VOSpaceError(404, f"Job or node does not exist. "
                                f"JobID: {job_id}")

    if results['phase'] != UWSPhase.Executing:
        raise VOSpaceError(400, f"Job not in EXECUTING phase."
                                f"JobID: {job_results['phase']}")

    # Want to allow nodes to be appended to containers in parallel.
    # Every other node should be blocked while uploading.
    if results['busy'] is True and results['type'] != NodeType.ContainerNode:
        raise VOSpaceError(400, f"Node Busy. "
                                f"Node: {results['path']} is busy.")

    return results


async def set_node_busy(conn, path, busy):
    path_array = list(filter(None, path.split('/')))
    path_tree = '.'.join(path_array)

    path = await conn.fetchrow("update nodes set busy=$2 "
                               "where path=$1 returning path",
                               path_tree,
                               busy)
    if not path:
        raise VOSpaceError(404, f"Node Not Found. "
                                f"{path} not found.")


async def get_node(app, path, params):

    detail = params.get('detail', 'max')
    if detail:
        if detail not in ['min', 'max', 'properties']:
            raise VOSpaceError(400, f'Invalid URI. '
                                    f'detail invalid: {detail}')

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
            raise VOSpaceError(400, f'Invalid URI. '
                                    f'limit invalid: {limit}')

    path_array = list(filter(None, path.split('/')))

    if len(path_array) == 0:
        raise VOSpaceError(400, f"Invalid URI. "
                                f"Path is empty")

    path_tree = '.'.join(path_array)

    properties = []
    views = []
    provides = []

    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            results = await conn.fetch(f"select * from nodes "
                                       f"where path <@ $1 and "
                                       f"nlevel(path)-nlevel($1)<=1 "
                                       f"order by path asc for share {limit_str}",
                                       path_tree)
            if len(results) == 0:
                raise VOSpaceError(404, f"Node Not Found. "
                                        f"{path} not found.")

            if detail != 'min':
                properties = await conn.fetch("select * from properties "
                                              "where node_path=$1 for share",
                                              results[0]['path'])

    node_type_int = results[0]['type']
    node_type = NodeTextLookup[node_type_int]
    if NodeType.Node <= node_type_int <= NodeType.ContainerNode:
        if detail == 'max':
            views = Views
            provides = Provides

    # remove root element in tree so we can output children
    results.pop(0)
    if limit:
        results = results[:int(limit)]

    xml_response = generate_node_response(path,
                                          node_type,
                                          properties,
                                          views,
                                          provides,
                                          results)
    return xml_response


async def delete_node(app, path):

    path_array = list(filter(None, path.split('/')))
    path_tree = '.'.join(path_array)

    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            result = await conn.fetch("delete from nodes "
                                      "where path <@ $1 returning path",
                                      path_tree)
    if not result:
        raise VOSpaceError(404, f"Node Not Found. "
                                f"{path} not found.")


async def create_node_request(app, xml_text, url_path):
    root = ET.fromstring(xml_text)
    uri_xml = root.attrib.get('uri', None)

    uri_path = urlparse(uri_xml)

    if not uri_path.path:
        raise VOSpaceError(400, "Invalid URI. "
                                "URI does not exist.")

    # make sure the request path and the URL are the same
    # exclude '.' characters as they are used in ltree
    if '.' in uri_path.path:
        raise VOSpaceError(400, f"Invalid URI. "
                                f"Invalid character: '.' in URI")

    uri_path_norm = os.path.normpath(uri_path.path)

    if os.path.normpath(url_path) != uri_path_norm:
        raise VOSpaceError(400, f"Invalid URI. "
                                f"URI node path does not "
                                f"match request: {url_path} != {uri_path.path}")

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
            create_response = await create_node(app,
                                                conn,
                                                uri_path_norm,
                                                node_type,
                                                props)

    if create_response.node_updated is True:
        raise VOSpaceError(409, f"Duplicate Node. {uri_path_norm} already exists.")

    return create_response


async def create_node(app,
                      conn,
                      uri_path,
                      node_type,
                      properties,
                      check_valid_view_uri=None):
    try:
        if node_type is None:
            node_type = NodeType.Node # if not specified then default is Node
            node_type_text = 'vos:Node'
        else:
            node_type_text = node_type
            node_type = NodeLookup.get(node_type_text, None)
            if node_type is None:
                raise VOSpaceError(400, f"Type Not Supported. "
                                        f"Invalid type.")

        # remove empty entries as a result of strip
        path = list(filter(None, uri_path.split('/')))

        if len(path) == 0:
            raise VOSpaceError(400, f"Invalid URI. "
                                    f"Path is empty")

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
            raise VOSpaceError(404, f"Container Not Found. "
                                    f"{'/'.join(path_parent)} not found.")

        if row:
            if row['type'] == NodeType.LinkNode:
                raise VOSpaceError(400, f"Link Found. "
                                        f"{row['name']} found in path.")

            if row['type'] != NodeType.ContainerNode:
                raise VOSpaceError(404, f"Container Not Found. "
                                        f"{row['name']} is not a container.")

        node_result = await conn.fetchrow(("INSERT INTO nodes (type, name, path) "
                                           "VALUES ($1, $2, $3) ON CONFLICT (path)"
                                           "DO UPDATE SET name=EXCLUDED.name "
                                           "RETURNING type, path, (xmax::text::int>0) AS updated"),
                                          node_type,
                                          node_name,
                                          path_tree)
        if properties:
            for prop in properties_list:
                prop.append(node_result['path'])

            await conn.executemany(("INSERT INTO properties (uri, value, read_only, node_path) "
                                    "VALUES ($1, $2, $3, $4)"),
                                   properties_list)

        import_views = app['plugin'].get_supported_import_views(node_type_text)
        if check_valid_view_uri is not None:
            if check_valid_view_uri not in import_views:
                raise VOSpaceError(400, f"View Not Supported. "
                                        f"View {check_valid_view_uri} not supported.")

        return Create_Response(node_name,
                               node_type_text,
                               properties,
                               import_views,
                               node_result['updated'])

    except ParseError as p:
        raise VOSpaceError(500, f"Internal Error. XML error: {str(p)}.")

    except asyncpg.exceptions.UniqueViolationError as f:
        raise VOSpaceError(409, f"Duplicate Node. {node_name} already exists.")


async def set_node_properties(app, xml_text, url_path):
    try:
        root = ET.fromstring(xml_text)

        uri = root.attrib.get('uri', None)
        if uri is None:
            raise VOSpaceError(400, "Invalid URI. "
                                    "URI does not exist.")

        node_type = root.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type',
                                    None)
        if node_type is None:
            raise VOSpaceError(400, "Invalid URI. "
                                    "Type does not exist.")

        node_type = NodeLookup.get(node_type, None)
        if node_type is None:
            raise VOSpaceError(400, f"Type Not Supported. "
                                    f"Invalid type.")

        uri_path = urlparse(uri)

        if '.' in url_path:
            raise VOSpaceError(400, f"Invalid URI. "
                                    f"Invalid character: '.' in URI")

        # make sure the request path and the URL are the same
        # exclude '.' characters as they are used in ltree
        if url_path != uri_path.path:
            raise VOSpaceError(400, f"Invalid URI. "
                                    f"URI node path does not "
                                    f"match request: {url_path} != {uri_path.path}")

        # remove empty entries as a result of strip
        node_url_path = list(filter(None, url_path.split('/')))

        if len(node_url_path) == 0:
            raise VOSpaceError(400, f"Invalid URI. "
                                    f"Path is empty")

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
                results = await conn.fetch(f"select * from nodes "
                                           f"where path=$1 and "
                                           f"type=$2 for share",
                                           node_path_tree,
                                           node_type)
                if len(results) == 0:
                    raise VOSpaceError(404, f"Node Not Found. "
                                            f"{url_path} not found.")

                # if a property already exists then update it, only if read_only = False
                await conn.executemany("INSERT INTO properties (uri, value, read_only, node_path) "
                                       "VALUES ($1, $2, $3, $4) on conflict (uri, node_path) "
                                       "do update set value=$2 where properties.read_only=False",
                                       node_props_insert)

                # only delete properties where read_only=False
                await conn.execute("DELETE FROM properties WHERE "
                                   "uri=any($1::text[]) "
                                   "AND node_path=$2 and read_only=False",
                                   node_props_delete,
                                   node_path_tree)

                properties_result = await conn.fetch("select * from properties "
                                                     "where node_path=$1 for share",
                                                     node_path_tree)

        xml_response = generate_node_response(node_path=url_path,
                                              node_type=NodeTextLookup[node_type],
                                              node_property=properties_result)
        return xml_response

    except ParseError as p:
        raise VOSpaceError(500, f"Internal Error. XML error: {str(p)}.")

    except asyncpg.exceptions.UniqueViolationError as f:
        raise VOSpaceError(409, f"Duplicate Node. {url_path} already exists.")


async def delete_properties(conn, uri_path):
    path = list(filter(None, uri_path.split('/')))
    path_tree = '.'.join(path)

    await conn.execute("delete from properties "
                       "where node_path=$1",
                       path_tree)