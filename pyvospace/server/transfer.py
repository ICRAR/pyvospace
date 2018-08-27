import copy
import asyncio
import asyncpg

from contextlib import suppress

from pyvospace.core.exception import VOSpaceError, NodeDoesNotExistError, PermissionDenied, InvalidArgument
from pyvospace.core.model import UWSPhase, UWSResult, NodeTransfer, ProtocolTransfer, PushToSpace, \
    NodeType, DataNode, ContainerNode
from pyvospace.server import fuzz
from .database import NodeDatabase


async def perform_transfer_job(job, app, identity, sync, redirect=False):
    try:
        with suppress(asyncio.CancelledError):
            return await asyncio.shield(_perform_transfer_job(job, app, identity, sync, redirect))
    except VOSpaceError as v:
        with suppress(asyncio.CancelledError):
            await asyncio.shield(app['executor'].set_error(job.job_id, v.error))
        if sync:
            raise


async def _perform_transfer_job(job, app, identity, sync, redirect):
    db_pool = app['db_pool']
    try:
        if isinstance(job.job_info, ProtocolTransfer):

            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    _, child_row = await app['db']._get_node_and_parent(job.job_info.target.path, conn)

                    if isinstance(job.job_info, PushToSpace):
                        # If there is no Node at the target URI, then the service SHALL
                        # create a new Node using the uri and the default xsi:type for the space.
                        if child_row:
                            node = NodeDatabase._resultset_to_node([child_row], [])
                            # If a Node already exists at the target URI,
                            # then the data SHALL be imported into the existing Node
                            # and the Node properties SHALL be cleared unless the node is a ContainerNode.
                            if node.node_type != NodeType.ContainerNode:
                                await app['db'].delete_properties(path=job.job_info.target.path, conn=conn)
                                node.remove_properties()
                        else:
                            node = DataNode(path=job.job_info.target.path)
                            await app['db'].insert(node=node, conn=conn, identity=identity)
                            await app['abstract_space'].create_storage_node(node)

                        '''import_views = app['accepts_views'].get(node.node_type_text, [])
                        if transfer.view:
                            if transfer.view.uri not in import_views:
                                raise VOSpaceError(400, f"View Not Supported. "
                                                        f"View {transfer.view.uri} not supported.")'''
                    else:
                        if not child_row:
                            raise NodeDoesNotExistError(f"{job.job_info.target.path} not found.")
                        node = NodeDatabase._resultset_to_node([child_row], [])

                    # Can't upload or download data to/from linknode
                    # Left out ContainerNode as the specific storage implementation might want to unpack
                    # it and create nodes.
                    if node.node_type == NodeType.LinkNode:
                        raise VOSpaceError(400, 'Operation Not Supported. No data transfer to a LinkNode.')

                    job.node_path_modified = node.path_modified
                    job.job_info.target = node
                    job.transfer = copy.deepcopy(job.job_info)
                    new_protocols = await app['abstract_space'].get_transfer_protocols(job)
                    job.transfer.set_protocols(new_protocols)

                    job.results = [UWSResult('transferDetails',
                                            {'{http://www.w3.org/1999/xlink}href':
                                                 f"/vospace/transfers/{job.job_id}/results/transferDetails"}),
                                   UWSResult('dataNode',
                                            {'{http://www.w3.org/1999/xlink}href':
                                                 f"vos://{app['uri']}!vospace/{job.job_info.target.path}"})]

                    endpoint = None
                    if redirect:
                        assert len(job.transfer.protocols) > 0, "Protocol endpoint not found."
                        endpoint = str(job.transfer.protocols[0].endpoint.url)

                    await fuzz()
                    job.phase = UWSPhase.Executing
                    await app['executor']._update_uws_job(job, conn)
                    return endpoint
        else:
            if sync is True:
                raise VOSpaceError(403, "Permission Denied. Move/Copy denied.")

            assert isinstance(job.job_info, NodeTransfer) is True
            await app['executor'].set_executing(job.job_id)

            with suppress(asyncio.CancelledError):
                await asyncio.shield(_move_nodes(app=app,
                                                 target_path=job.job_info.target.path,
                                                 direction_path=job.job_info.direction.path,
                                                 perform_copy=job.job_info.keep_bytes,
                                                 identity=identity))

            # need to shield because we have successfully completed a potentially expensive operation
            with suppress(asyncio.CancelledError):
                await asyncio.shield(app['executor'].set_completed(job.job_id))

    except VOSpaceError:
        raise

    except AssertionError as g:
        raise InvalidArgument(str(g))

    except asyncpg.exceptions.UniqueViolationError:
        raise VOSpaceError(409, f"Duplicate Node. {job.job_info.target.path} already exists.")

    except asyncpg.exceptions.ForeignKeyViolationError:
        raise VOSpaceError(404, f"Node Not Found. {job.job_info.target.path} not found.")

    except BaseException as e:
        raise VOSpaceError(500, str(e))


async def _move_nodes(app, target_path, direction_path, perform_copy, identity):
    space_id = app['space_id']
    try:
        target_path_tree = NodeDatabase.path_to_ltree(target_path)
        # check if destination is the root of the filesystem which in not technically a node
        if not any(direction_path in s for s in ['/', '//']):
            destination_path_tree = NodeDatabase.path_to_ltree(direction_path)
        else:
            destination_path_tree = ''

        async with app['db_pool'].acquire() as conn:
            async with conn.transaction():
                target_record = None
                dest_record = None

                if destination_path_tree:
                    results = await conn.fetch("select *, path = subltree($2, 0, nlevel(path)) as common "
                                               "from nodes where path <@ $1 or path <@ $2 and space_id=$3 "
                                               "order by path asc for update",
                                               target_path_tree, destination_path_tree, space_id)
                    for result in results:
                        if result['path'] == target_path_tree:
                            target_record = result
                            continue
                        if result['path'] == destination_path_tree:
                            dest_record = result
                            continue
                        if target_record and dest_record:
                            break

                    if target_record is None:
                        raise VOSpaceError(404, f"Node Not Found. {target_path} not found.")

                    if dest_record is None:
                        raise VOSpaceError(404, f"Node Not Found. {direction_path} not found.")

                    if dest_record['type'] != NodeType.ContainerNode:
                        raise VOSpaceError(400, f"Duplicate Node. {direction_path} already exists "
                                                f"and is not a container.")

                    if target_record['common'] is True and target_record['type'] == NodeType.ContainerNode:
                        raise VOSpaceError(400, f"Invalid URI. Moving {target_path} -> {direction_path} "
                                                f"is invalid.")

                    dest = NodeDatabase.resultset_to_node_tree([dest_record], [])
                else:
                    results = await conn.fetch("select * from nodes where path <@ $1 and space_id=$2 "
                                               "order by path asc for update",
                                               target_path_tree, space_id)
                    for result in results:
                        if result['path'] == target_path_tree:
                            target_record = result
                            break

                    if target_record is None:
                        raise VOSpaceError(404, f"Node Not Found. {target_path} not found.")

                    dest = ContainerNode('/', group_read=[identity])

                src = NodeDatabase.resultset_to_node_tree([target_record], [])

                if perform_copy:
                    if not await app.permits(identity, 'copyNode', context=(src, dest)):
                        raise PermissionDenied('copyNode denied.')

                    prop_results = await conn.fetch("select properties.uri, properties.value, "
                                                    "properties.read_only, properties.space_id, "
                                                    "$2||subpath(node_path, nlevel($1)-1) as concat "
                                                    "from nodes inner join properties on "                    
                                                    "nodes.path = properties.node_path and "
                                                    "nodes.space_id = properties.space_id "
                                                    "where nodes.path <@ $1 and nodes.space_id=$3",
                                                    target_path_tree, destination_path_tree, space_id)

                    await conn.execute("insert into nodes(name, type, owner, groupread, groupwrite, "
                                       "space_id, link, path) "
                                       "(select name, type, owner, groupread, groupwrite, "
                                       "space_id, link, $2||subpath(path, nlevel($1)-1) as concat "
                                       "from nodes where path <@ $1 and space_id=$3)",
                                       target_path_tree, destination_path_tree, space_id)

                    user_props_insert = []
                    for prop in prop_results:
                        user_props_insert.append(tuple(prop))

                    await conn.executemany("insert into properties (uri, value, read_only, space_id, node_path) "
                                           "values ($1, $2, $3, $4, $5)",
                                           user_props_insert)

                    await app['abstract_space'].copy_storage_node(src, dest)

                else:
                    if not await app.permits(identity, 'moveNode', context=(src, dest)):
                        raise PermissionDenied('moveNode denied.')

                    await conn.execute("update nodes set path = $2 || subpath(path, nlevel($1)-1) "
                                       "where path <@ $1 and space_id=$3",
                                       target_path_tree, destination_path_tree, space_id)

                    await app['abstract_space'].move_storage_node(src, dest)

    except asyncpg.exceptions.UniqueViolationError as f:
        raise VOSpaceError(409, f"Duplicate Node. {f.detail}")
