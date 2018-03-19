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
            raise VOSpaceError(500, "InternalFault. "
                                    "vos:keepBytes does not exist")

        if keep_bytes.text == 'false':
            await move_node(db_pool, root)

        elif keep_bytes.text == 'true':
            raise VOSpaceError(500, "InternalFault. "
                                    "copyNode not implemented.")
        else:
            raise VOSpaceError(500, "Unknown Type. "
                                    "copyNode not implemented.")
    except Exception as e:
        raise VOSpaceError(500, str(e))


async def move_node(db_pool, root):

    transfer = root.tag
    target = root.find('vos:target', NS)
    direction = root.find('vos:direction', NS)
    if transfer is None:
        raise VOSpaceError(500, "InternalFault. "
                                "vos:transfer root does not exist")

    if target is None:
        raise VOSpaceError(500, "InternalFault. "
                                "vos:target does not exist")

    if direction is None:
        raise VOSpaceError(500, "InternalFault. "
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
                                      "from nodes where path <@ $1 for update",
                                      target_path_tree,
                                      destination_path_tree)

            if len(result) == 0:
                raise VOSpaceError(404, f"Node Not Found. "
                                        f"{target_path} not found.")

            if result[0]['common'] is True and result[0]['type'] == NodeType.ContainerNode:
                raise VOSpaceError(400, f"Invalid URI. "
                                        f"Moving {target_path} -> {direction_path} "
                                        f"is invalid.")

            dest_result = await conn.fetchrow("select name, type, path "
                                              "from nodes where path = $1 for update",
                                              destination_path_tree)

            if dest_result and dest_result['type'] != NodeType.ContainerNode:
                raise VOSpaceError(400, f"Duplicate Node. "
                                        f"{direction_path} already exists "
                                        f"and is not a container.")

            move_tree_result = await conn.execute("update nodes set "
                                                  "path = $2 || subpath(path, nlevel($1)-1) "
                                                  "where path <@ $1",
                                                  target_path_tree,
                                                  destination_path_tree)

            print(move_tree_result)
