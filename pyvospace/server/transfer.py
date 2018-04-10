import os
import asyncpg
import lxml.etree as ET

from urllib.parse import urlparse

from .uws import set_uws_results_and_phase, results_dict_list_to_xml, set_uws_phase, UWSPhase
from .node import NS, NodeType, create_node, NodeTextLookup, VOSpaceName
from .exception import VOSpaceError


async def do_transfer(db_pool, job_id, job):
    try:
        job_xml = job['job_info']
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
            trans_params = []
            for params in root.findall('vos:param', NS):
                param_uri = params.attrib.get('uri', None)
                if param_uri is None:
                    raise VOSpaceError(400, f"Invalid Argument. Invalid parameter.")

                trans_params.append({'uri': param_uri, 'value': params.text})

            try:
                await create_node(db_pool=db_pool,
                                  uri_path=target_path_norm,
                                  node_type=NodeTextLookup[NodeType.DataNode],
                                  properties=None)

            except VOSpaceError as e:
                # if its a duplicate node then just continue
                if e.code != 409:
                    raise e

            results = [{'id': 'transferDetails',
                        'xlink:href': f'/vospace/transfers/{job_id}/results/transferDetails'},
                       {'id': "dataNode",
                        'xlink:href': f'{VOSpaceName}{target_path_norm}'}]

            xml_results = results_dict_list_to_xml(results)

            await set_uws_results_and_phase(db_pool, job_id, xml_results, UWSPhase.Executing)

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

            await set_uws_phase(db_pool, job_id, UWSPhase.Executing)

            await move_nodes(db_pool=db_pool,
                             target_path=target_path_norm,
                             direction_path=direction_path_norm,
                             perform_copy=copy_node)

            await set_uws_phase(db_pool, job_id, UWSPhase.Completed)

    except VOSpaceError as f:
        raise f

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise VOSpaceError(500, str(e))


async def move_nodes(db_pool, target_path, direction_path, perform_copy):

    try:
        target_path_array = list(filter(None, target_path.split('/')))
        direction_path_array = list(filter(None, direction_path.split('/')))

        target_path_tree = '.'.join(target_path_array)
        destination_path_tree = '.'.join(direction_path_array)

        async with db_pool.acquire() as conn:
            async with conn.transaction(isolation='repeatable_read'):
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
