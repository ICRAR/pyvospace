import os
import asyncpg
import xmltodict

import lxml.etree as ET

from aiohttp_security import permits, authorized_userid
from urllib.parse import urlparse
from collections import namedtuple
from xml.etree.ElementTree import ParseError
from contextlib import suppress

from pyvospace.core.exception import *
from pyvospace.core.model import *


NS = {'vos': 'http://www.ivoa.net/xml/VOSpace/v2.1'}
namespaces = {'http://www.ivoa.net/xml/VOSpace/v2.1': 'vos',
              'http://www.w3.org/2001/XMLSchema-instance': 'xs'}


async def _get_node_request(request):
    path = request.path.replace('/vospace/nodes', '')

    detail = request.query.get('detail', 'max')
    if detail:
        if detail not in ['min', 'max', 'properties']:
            raise InvalidURI(f'detail invalid: {detail}')

    limit = request.query.get('limit', None)
    if limit:
        try:
            limit = int(limit)
            if limit <= 0:
                raise Exception()
        except:
            raise InvalidURI(f'limit invalid: {limit}')

    async with request.app['db_pool'].acquire() as conn:
        async with conn.transaction():
            node = await request.app['db'].directory(path, conn)

    if detail == 'min':
        node.remove_properties()

    if isinstance(node, DataNode):
        if detail == 'max':
            node_type_text = NodeTextLookup[node.node_type]
            accepts = request.app['accepts_views'].get(node_type_text, [])
            provides = request.app['provides_views'].get(node_type_text, [])
            node.accepts = [View(accept) for accept in accepts]
            node.provides = [View(provide) for provide in provides]

    if isinstance(node, ContainerNode):
        if limit:
            node.set_nodes(node.nodes[:limit])

    return node


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
                await app['abstract_space'].delete_storage_node(result[0]['type'], '/'.join(path_array))


def uri_to_path(uri):
    uri_parsed = urlparse(uri)

    if not uri_parsed.path:
        raise VOSpaceError(400, "Invalid URI. URI does not exist.")

    if '.' in uri_parsed.path:
        raise VOSpaceError(400, f"Invalid URI. Invalid character: '.' in URI")

    return os.path.normpath(uri_parsed.path).lstrip('/')


async def _create_node_request(request):
    identity = await authorized_userid(request)

    xml_request = await request.text()
    url_path = request.path.replace('/vospace/nodes', '')

    node = Node.fromstring(xml_request)

    if node.path != uri_to_path(url_path):
        raise InvalidURI("Paths do not match")

    if permits(identity, 'createNode', context=node) is False:
        raise Exception('not allowed')

    async with request.app['db_pool'].acquire() as conn:
        async with conn.transaction():
            await request.app['db'].insert(node, conn)
            await request.app['abstract_space'].create_storage_node(node)

    return node


async def _set_node_properties_request(request):
    identity = await authorized_userid(request)

    xml_request = await request.text()
    path = request.path.replace('/vospace/nodes', '')

    node = Node.fromstring(xml_request)

    if node.path != uri_to_path(path):
        raise InvalidURI("Paths do not match")

    if permits(identity, 'setNode', context=node) is False:
        raise Exception('not allowed')

    async with request.app['db_pool'].acquire() as conn:
        async with conn.transaction():
            await request.app['db'].update(node, conn)
            node = await request.app['db'].directory(path, conn)
    return node
