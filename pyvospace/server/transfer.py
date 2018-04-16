import os
import json
import asyncpg
import lxml.etree as ET

from urllib.parse import urlparse

from .uws import set_uws_extras, set_uws_phase, \
    UWSPhase, get_uws_job, create_uws_job
from .node import NS, NodeType, create_node, NodeTextLookup, \
    VOSpaceName, NodeLookup, delete_properties
from .exception import VOSpaceError


def transfer_details_response(target, direction, protocol_endpoints):
    prot = protocol_endpoints['protocol']
    endpoints = protocol_endpoints['endpoint']

    prot_end = []
    for end in endpoints:
        end_str = f'<vos:protocol uri="{prot}">' \
                  f'<vos:endpoint>{end}</vos:endpoint>' \
                  f'</vos:protocol>'
        prot_end.append(end_str)

    xml = f'<?xml version="1.0" encoding="UTF-8"?>' \
          f'<vos:transfer xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1">' \
          f'<vos:target>{target}</vos:target>' \
          f'<vos:direction>{direction}</vos:direction>' \
          f'{"".join(prot_end)}' \
          f'</vos:transfer>'

    return xml


async def generate_xml_transfer_details(app, job_id):
    job = await get_uws_job(app['db_pool'], job_id)

    if not job['extras']:
        raise VOSpaceError(500, 'Internal Fault. '
                                'Job details not found.')

    extras = json.loads(job['extras'])
    node_type = extras['node']['type']
    node_path = extras['node']['path']
    target = extras['transfer']['target']
    direction = extras['transfer']['direction']
    protocol = extras['transfer']['protocol']
    view = extras['transfer']['view']
    params = extras['transfer']['params']

    end_points = app['plugin'].get_protocol_endpoints(job_id,
                                                      node_path,
                                                      node_type,
                                                      direction,
                                                      protocol,
                                                      view,
                                                      params)

    return transfer_details_response(target, direction, end_points)


async def create_transfer_job(app, job_xml):
    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            return await _create_transfer_job(app, conn, job_xml)


async def _create_transfer_job(app, conn, job_xml):
    try:
        root = ET.fromstring(job_xml)

        transfer = root.tag
        if transfer is None:
            raise VOSpaceError(500, "Internal Fault. "
                                    "vos:transfer root does not exist")

        target = root.find('vos:target', NS)
        if target is None:
            raise VOSpaceError(500, "Internal Fault. "
                                    "vos:target does not exist")

        direction = root.find('vos:direction', NS)
        if direction is None:
            raise VOSpaceError(500, "Internal Fault. "
                                    "vos:direction does not exist")

        target_path = urlparse(target.text)
        if not target_path.path:
            raise VOSpaceError(400, "Invalid URI. "
                                    "URI does not exist.")

        target_path_norm = os.path.normpath(target_path.path)

        # check transfer type
        if direction.text == 'pushToVoSpace':
            direction_path_norm = direction.text

            trans_params = []
            for params in root.findall('vos:param', NS):
                param_uri = params.attrib.get('uri', None)
                if param_uri is None:
                    raise VOSpaceError(400, f"Invalid Argument. Invalid parameter.")

                trans_params.append({'uri': param_uri, 'value': params.text})

            prot = root.find('vos:protocol', NS)
            if prot is None:
                raise VOSpaceError(400, "Invalid Argument. Protocol not found.")

            prot_uri = prot.attrib.get('uri', None)
            if prot_uri is None:
                raise VOSpaceError(400, "Invalid Argument. Protocol uri not found.")

            provides_protocols = app['plugin'].get_provides_protocols()
            if prot_uri not in provides_protocols:
                raise VOSpaceError(400, f"Protocol Not Supported. "
                                        f"Protocol {prot_uri} not supported.")

            node_view_uri = None
            node_view = root.find('vos:view', NS)
            if node_view is not None:
                node_view_uri = node_view.attrib.get('uri', None)
                if node_view_uri is None:
                    raise VOSpaceError(400, "Invalid Argument. View uri not found.")


            # If there is no Node at the target URI, then the service SHALL
            # create a new Node using the uri and the default xsi:type for the space.
            response = await create_node(app=app,
                                         conn=conn,
                                         uri_path=target_path_norm,
                                         node_type=NodeTextLookup[NodeType.DataNode],
                                         properties=None,
                                         check_valid_view_uri=node_view_uri)

            # If a Node already exists at the target URI,
            # then the data SHALL be imported into the existing Node
            # and the Node properties SHALL be cleared unless the node is a ContainerNode.
            if response.node_updated is True:
                if NodeLookup[response.node_type_text] != NodeType.ContainerNode:
                    await delete_properties(conn, target_path_norm)

            extras = {'node': {'path': target_path_norm,
                               'type': response.node_type_text},
                      'transfer': {'direction': 'pushToVoSpace',
                                   'target': target.text,
                                   'protocol': prot_uri,
                                   'view': node_view_uri,
                                   'params': trans_params}}

        else:
            direction_path = urlparse(direction.text)
            if not direction_path.path:
                raise VOSpaceError(400, "Invalid URI. "
                                        "URI does not exist.")

            direction_path_norm = os.path.normpath(direction_path.path)

            keep_bytes = root.find('vos:keepBytes', NS)
            if keep_bytes is None:
                raise VOSpaceError(500, "Internal Fault. "
                                        "vos:keepBytes does not exist")

            if keep_bytes.text == 'false' or keep_bytes.text == 'False':
                copy_node = False

            elif keep_bytes.text == 'true' or keep_bytes.text == 'True':
                copy_node = True
            else:
                raise VOSpaceError(500, "Unknown keepBytes value.")

            extras = {'target': target_path_norm,
                      'direction': direction_path_norm,
                      'copy': copy_node}

        id = await create_uws_job(conn,
                                  target_path_norm,
                                  direction_path_norm,
                                  job_xml,
                                  json.dumps(extras))

        return id

    except VOSpaceError as f:
        raise f

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise VOSpaceError(500, str(e))


async def run_job(app, job_id, job):

    direction = job['direction']

    if direction == 'pushToVoSpace':
        extras = json.loads(job['extras'])
        target_path_norm = extras['node']['path']

        results = [{'id': 'transferDetails',
                    'xlink:href': f'/vospace/transfers/{job_id}'
                                  f'/results/transferDetails'},
                   {'id': "dataNode",
                    'xlink:href': f'{VOSpaceName}{target_path_norm}'}]

        extras['results'] = results

        async with app['db_pool'].acquire() as conn:
            async with conn.transaction():
                await set_uws_extras(conn,
                                     job_id,
                                     UWSPhase.Executing,
                                     json.dumps(extras))

    else:
        extras = json.loads(job['extras'])

        target_path_norm = extras['target']
        direction_path_norm = extras['direction']
        copy_node = extras['copy']

        await set_uws_phase(app['db_pool'],
                            job_id,
                            UWSPhase.Executing)

        await move_nodes(app=app,
                         target_path=target_path_norm,
                         direction_path=direction_path_norm,
                         perform_copy=copy_node)

        await set_uws_phase(app['db_pool'],
                            job_id,
                            UWSPhase.Completed)


async def move_nodes(app, target_path, direction_path, perform_copy):

    try:
        target_path_array = list(filter(None, target_path.split('/')))
        direction_path_array = list(filter(None, direction_path.split('/')))

        target_path_tree = '.'.join(target_path_array)
        destination_path_tree = '.'.join(direction_path_array)

        async with app['db_pool'].acquire() as conn:
            async with conn.transaction():
                result = await conn.fetch("select name, type, path, "
                                          "path = subltree($2, 0, nlevel(path)) as common "
                                          "from nodes where path <@ $1 "
                                          "or path = $2 order by path asc for update",
                                          target_path_tree,
                                          destination_path_tree)

                result_dict = {r['path']: r for r in result}

                target_record = result_dict.get(target_path_tree, None)
                dest_record = result_dict.get(destination_path_tree, None)

                if target_record is None:
                    raise VOSpaceError(404, f"Node Not Found. "
                                            f"{target_path} not found.")

                if dest_record is None:
                    raise VOSpaceError(404, f"Node Not Found. "
                                            f"{destination_path_tree} not found.")

                if dest_record['type'] != NodeType.ContainerNode:
                    raise VOSpaceError(400, f"Duplicate Node. "
                                            f"{direction_path} already exists "
                                            f"and is not a container.")

                if target_record['common'] is True and target_record['type'] == NodeType.ContainerNode:
                    raise VOSpaceError(400, f"Invalid URI. "
                                            f"Moving {target_path} -> {direction_path} "
                                            f"is invalid.")

                if perform_copy:
                    # copy properties
                    prop_results = await conn.fetch("select properties.uri, properties.value, "
                                                    "properties.read_only, "
                                                    "$2||subpath(node_path, nlevel($1)-1) as concat "
                                                    "from nodes inner join properties "
                                                    "on nodes.path = properties.node_path "
                                                    "where path <@ $1 for update",
                                                     target_path_tree,
                                                     destination_path_tree)

                    await conn.execute("insert into nodes(name, type, path) ( "
                                       "select name, type, $2||subpath(path, nlevel($1)-1) as concat "
                                       "from nodes where path <@ $1)",
                                       target_path_tree,
                                       destination_path_tree)

                    user_props_insert = []
                    for prop in prop_results:
                        user_props_insert.append(tuple(prop))

                    await conn.executemany(("INSERT INTO properties (uri, value, read_only, node_path) "
                                            "VALUES ($1, $2, $3, $4)"),
                                           user_props_insert)

                else:
                    await conn.execute("update nodes set "
                                       "path = $2 || subpath(path, nlevel($1)-1) "
                                       "where path <@ $1",
                                       target_path_tree,
                                       destination_path_tree)

    except asyncpg.exceptions.UniqueViolationError as f:
        raise VOSpaceError(409, f"Duplicate Node. {f.detail}")
