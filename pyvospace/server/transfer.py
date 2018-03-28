import asyncpg
import xml.etree.ElementTree as ET

from urllib.parse import urlparse

from .node import NS, NodeType
from .exception import VOSpaceError


async def do_transfer(db_pool, job):
    try:
        job_xml = job['job_info']
        root = ET.fromstring(job_xml)

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

        await transfer_nodes(db_pool, root, copy_node)

    except VOSpaceError as f:
        raise f

    except Exception as e:
        raise VOSpaceError(500, str(e))


async def transfer_nodes(db_pool, root, perform_copy):

    try:
        transfer = root.tag
        target = root.find('vos:target', NS)
        direction = root.find('vos:direction', NS)
        if transfer is None:
            raise VOSpaceError(500, "Internal Fault. "
                                    "vos:transfer root does not exist")

        if target is None:
            raise VOSpaceError(500, "Internal Fault. "
                                    "vos:target does not exist")

        if direction is None:
            raise VOSpaceError(500, "Internal Fault. "
                                    "vos:direction does not exist")

        target_path = urlparse(target.text).path
        direction_path = urlparse(direction.text).path

        target_path_array = list(filter(None, target_path.split('/')))
        direction_path_array = list(filter(None, direction_path.split('/')))

        target_path_tree = '.'.join(target_path_array)
        destination_path_tree = '.'.join(direction_path_array)

        async with db_pool.acquire() as conn:
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
                                                    "where path <@ $1",
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
