import os
import asyncio
import asyncpg
import lxml.etree as ET

from contextlib import suppress
from urllib.parse import urlparse


from .uws import set_uws_phase_to_executing, set_uws_phase_to_completed, \
    get_uws_job, get_uws_job_conn, UWSPhase, update_uws_job, \
    set_uws_phase_to_error, UWSKey, set_uws_phase_to_abort
from .node import NS, NodeType, NodeTextLookup
from pyvospace.core.exception import VOSpaceError, InvalidJobStateError, NodeDoesNotExistError
from pyvospace.core.model import DataNode

def xml_transfer_details(target, direction, protocol_endpoints):
    prot_end = []
    prot = protocol_endpoints['protocol']
    endpoints = protocol_endpoints['endpoints']

    for end in endpoints:
        end_str = f'<vos:protocol uri="{prot}"><vos:endpoint>{end}</vos:endpoint></vos:protocol>'
        prot_end.append(end_str)

    xml = '<?xml version="1.0" encoding="UTF-8"?>' \
          '<vos:transfer xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1">' \
          f'<vos:target>{target}</vos:target><vos:direction>{direction}</vos:direction>' \
          f'{"".join(prot_end)}</vos:transfer>'

    return xml


async def get_transfer_details(db_pool, space_id, job_id):
    job = await get_uws_job(db_pool, space_id, job_id)

    if job['phase'] < UWSPhase.Executing:
        raise VOSpaceError(400, f'Job not EXECUTING. Job: {job_id}')

    if not job['transfer']:
        raise VOSpaceError(400, f'No transferDetails for this job. Job: {job_id}')

    return job['transfer']


async def modify_transfer_job_phase(app, job_id, uws_cmd):
    if not uws_cmd:
        raise VOSpaceError(400, "Invalid Request. Empty UWS phase input.")

    db_pool = app['db_pool']
    exec = app['executor']
    phase = uws_cmd.upper()
    space_id = app['space_id']
    key = UWSKey(space_id, job_id)

    if phase == "PHASE=RUN":
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                job = await get_uws_job_conn(conn=conn, space_id=space_id,
                                             job_id=job_id, for_update=True)
                # Can only start a PENDING Job
                if job['phase'] != UWSPhase.Pending:
                    raise InvalidJobStateError('Invalid Job State')
                # Only run job if pending has not already been called.
                # If we dont do this then pending can be called many times before
                # the running job transitions to execute creating many tasks for the same job.
                exec.execute(run_job, key, app, job)

    elif phase == "PHASE=ABORT":
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                job = await get_uws_job_conn(conn=conn, space_id=space_id,
                                             job_id=job_id, for_update=True)

                if job['phase'] in (UWSPhase.Completed, UWSPhase.Error):
                    raise InvalidJobStateError("Invalid Request. "
                                               "Can't cancel a job that is COMPLETED or in ERROR.")
                # if its a move or copy operation
                if job['direction'] not in ('pushToVoSpace', 'pullFromVoSpace'):
                    # if the move or copy is being performed then do not abort
                    # we dont want to abort because undoing a file copy/move is
                    # difficult to do if the transaction fails
                    if job['phase'] >= UWSPhase.Executing:
                        raise InvalidJobStateError("Invalid Request. "
                                                   "Can't abort a move/copy that is EXECUTING.")

                with suppress(asyncio.CancelledError):
                    await asyncio.shield(set_uws_phase_to_abort(conn, space_id, job_id))

        with suppress(asyncio.CancelledError):
            await asyncio.shield(exec.abort(UWSKey(space_id, job_id)))
    else:
        raise VOSpaceError(400, f"Invalid Request. Unknown UWS phase input {uws_cmd}")


async def run_job(space_job_id, app, job):
    await perform_transfer_job(app=app, space_job_id=space_job_id, job_xml=job['job_info'])


async def perform_transfer_job(app, space_job_id, job_xml, sync=False):
    try:
        await _perform_transfer_job(app=app, space_job_id=space_job_id, job_xml=job_xml, sync=sync)
    except VOSpaceError as e:
        await asyncio.shield(set_uws_phase_to_error(app['db_pool'], space_job_id.space_id,
                                                    space_job_id.job_id, e.error))
        raise e


async def _get_storage_endpoints(app, space_id, job_id, node_type, node_path, protocol, direction):
    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            results = await conn.fetch("select storage.id, storage.host, storage.port from storage "
                                       "inner join space on space.name=storage.name "
                                       "where space.id=$1", space_id)

    storage_results = [dict(r) for r in results]
    filtered = await app['abstract_space'].filter_storage_endpoints(storage_results, node_type,
                                                                    node_path, protocol, direction)
    endpoints = []
    for row in filtered:
        prot = 'http' if protocol.split('#')[1].startswith('http') else 'https'
        endpoints.append(f'{prot}://{row["host"]}:{row["port"]}/vospace/{direction}/{job_id}')
    return {'protocol': protocol, 'endpoints': endpoints}


async def _perform_transfer_job(app, space_job_id, job_xml, sync):
    db_pool = app['db_pool']
    try:
        root = ET.fromstring(job_xml)

        transfer = root.tag
        if transfer is None:
            raise VOSpaceError(500, "Internal Fault. vos:transfer root does not exist")

        target = root.find('vos:target', NS)
        if target is None:
            raise VOSpaceError(500, "Internal Fault. vos:target does not exist")

        direction = root.find('vos:direction', NS)
        if direction is None:
            raise VOSpaceError(500, "Internal Fault. vos:direction does not exist")

        target_path = urlparse(target.text)
        if not target_path.path:
            raise VOSpaceError(400, "Invalid URI. URI does not exist.")

        target_path_norm = os.path.normpath(target_path.path).lstrip('/')

        # check transfer type
        if direction.text == 'pushToVoSpace' or direction.text == 'pullFromVoSpace':
            direction_path_norm = direction.text

            '''trans_params = []
            for params in root.findall('vos:param', NS):
                param_uri = params.attrib.get('uri', None)
                if param_uri is None:
                    raise VOSpaceError(400, f"Invalid Argument. Invalid parameter.")

                trans_params.append({'uri': param_uri, 'value': params.text})'''

            prot = root.find('vos:protocol', NS)
            if prot is None:
                raise VOSpaceError(400, "Invalid Argument. Protocol not found.")

            prot_uri = prot.attrib.get('uri', None)
            if prot_uri is None:
                raise VOSpaceError(400, "Invalid Argument. Protocol uri not found.")

            # check that the protocol for the operation given is supported
            if prot_uri not in app['provides_protocols']:
                raise VOSpaceError(400, f"Protocol Not Supported. "
                                        f"Protocol {prot_uri} for {direction_path_norm} not supported.")

            node_view_uri = None
            node_view = root.find('vos:view', NS)
            if node_view is not None:
                node_view_uri = node_view.attrib.get('uri', None)
                if node_view_uri is None:
                    raise VOSpaceError(400, "Invalid Argument. View uri not found.")

            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    _, child_row = await app['db']._get_node_and_parent(target_path_norm, conn)

                    if direction.text == 'pushToVoSpace':
                        # If there is no Node at the target URI, then the service SHALL
                        # create a new Node using the uri and the default xsi:type for the space.
                        if child_row:
                            node = app['db']._resultset_to_node([child_row], [])
                            # If a Node already exists at the target URI,
                            # then the data SHALL be imported into the existing Node
                            # and the Node properties SHALL be cleared unless the node is a ContainerNode.
                            if node.node_type != NodeType.ContainerNode:
                                await app['db'].delete_properties(path=target_path_norm, conn=conn)
                                node.remove_properties()
                        else:
                            node = DataNode(path=target_path_norm)
                            await app['db'].insert(node=node, conn=conn)
                            await app['abstract_space'].create_storage_node(node)

                        import_views = app['accepts_views'].get(node.node_type_text, [])
                        if node_view_uri:
                            if node_view_uri not in import_views:
                                raise VOSpaceError(400, f"View Not Supported. "
                                                        f"View {node_view_uri} not supported.")
                        node_type = node.node_type
                    else:
                        if not child_row:
                            raise NodeDoesNotExistError(f"{target_path_norm} not found.")
                        node_type = child_row['type']

            # Can't upload or download data to/from linknode
            # Left out ContainerNode as the specific storage implementation might want to unpack
            # it and create nodes.
            if node_type == NodeType.LinkNode:
                raise VOSpaceError(400, 'Operation Not Supported. No data transfer to a LinkNode.')

            end_points = await _get_storage_endpoints(app,
                                                      space_job_id.space_id,
                                                      space_job_id.job_id,
                                                      node_type,
                                                      target_path_norm,
                                                      prot_uri,
                                                      direction.text)

            xml_transfer = xml_transfer_details(target=target.text,
                                                direction=direction.text,
                                                protocol_endpoints=end_points)

            space_name = app['uri']
            attr_vals = []
            attr_vals.append('<uws:result id="transferDetails" '
                             f'xlink:href="/vospace/transfers/{space_job_id.job_id}'
                             f'/results/transferDetails"/>')
            attr_vals.append('<uws:result id="dataNode" '
                             f'xlink:href="vos://{space_name}!vospace/{target_path_norm}"/>')
            result = f"<uws:results>{''.join(attr_vals)}</uws:results>"

            await update_uws_job(db_pool=db_pool, space_id=space_job_id.space_id,
                                 job_id=space_job_id.job_id, target=target_path_norm,
                                 direction=direction_path_norm, transfer=xml_transfer,
                                 result=result, phase=UWSPhase.Executing)
        else:
            if sync is True:
                raise VOSpaceError(403, "Permission Denied. Move/Copy denied.")

            direction_path = urlparse(direction.text)
            if not direction_path.path:
                raise VOSpaceError(400, "Invalid URI. URI does not exist.")

            direction_path_norm = os.path.normpath(direction_path.path)

            keep_bytes = root.find('vos:keepBytes', NS)
            if keep_bytes is None:
                raise VOSpaceError(500, "Internal Fault. vos:keepBytes does not exist")

            if keep_bytes.text == 'false' or keep_bytes.text == 'False':
                copy_node = False

            elif keep_bytes.text == 'true' or keep_bytes.text == 'True':
                copy_node = True
            else:
                raise VOSpaceError(500, "Unknown keepBytes value.")

            await set_uws_phase_to_executing(db_pool,
                                             space_job_id.space_id,
                                             space_job_id.job_id)

            with suppress(asyncio.CancelledError):
                await asyncio.shield(_move_nodes(app=app,
                                                 space_id=space_job_id.space_id,
                                                 target_path=target_path_norm,
                                                 direction_path=direction_path_norm,
                                                 perform_copy=copy_node))

            # need to shield because we have successfully compeleted
            # a potentially expensive operation
            with suppress(asyncio.CancelledError):
                await asyncio.shield(set_uws_phase_to_completed(db_pool,
                                                                space_job_id.space_id,
                                                                space_job_id.job_id))

    except VOSpaceError as f:
        raise f

    except asyncpg.exceptions.UniqueViolationError:
        raise VOSpaceError(409, f"Duplicate Node. {target_path_norm} already exists.")

    except asyncpg.exceptions.ForeignKeyViolationError:
        raise VOSpaceError(404, f"Node Not Found. {target_path_norm} not found.")

    except BaseException as e:
        import traceback
        traceback.print_exc()
        raise VOSpaceError(500, str(e))


async def _move_nodes(app, space_id, target_path, direction_path, perform_copy):
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
                                          "or path <@ $2 and space_id=$3 order by path asc for update",
                                          target_path_tree,
                                          destination_path_tree,
                                          space_id)
                result_dict = {r['path']: r for r in result}

                target_record = result_dict.get(target_path_tree, None)
                dest_record = result_dict.get(destination_path_tree, None)

                if target_record is None:
                    raise VOSpaceError(404, f"Node Not Found. {target_path} not found.")

                if dest_record is None:
                    raise VOSpaceError(404, f"Node Not Found. {destination_path_tree} not found.")

                if dest_record['type'] != NodeType.ContainerNode:
                    raise VOSpaceError(400, f"Duplicate Node. {direction_path} already exists "
                                            f"and is not a container.")

                if target_record['common'] is True and target_record['type'] == NodeType.ContainerNode:
                    raise VOSpaceError(400, f"Invalid URI. Moving {target_path} -> {direction_path} "
                                            f"is invalid.")

                src = '/'.join(target_path_array)
                dest = f"{'/'.join(direction_path_array)}/"

                if perform_copy:
                    # copy properties
                    prop_results = await conn.fetch("select properties.uri, properties.value, "
                                                    "properties.read_only, properties.space_id, "
                                                    "$2||subpath(node_path, nlevel($1)-1) as concat "
                                                    "from nodes inner join properties on "                    
                                                    "nodes.path = properties.node_path and "
                                                    "nodes.space_id = properties.space_id "
                                                    "where nodes.path <@ $1 and nodes.space_id=$3",
                                                    target_path_tree,
                                                    destination_path_tree,
                                                    space_id)

                    await conn.execute("insert into nodes(name, type, space_id, link, path) ( "
                                       "select name, type, space_id, link, $2||subpath(path, nlevel($1)-1) as concat "
                                       "from nodes where path <@ $1 and space_id=$3)",
                                       target_path_tree,
                                       destination_path_tree,
                                       space_id)

                    user_props_insert = []
                    for prop in prop_results:
                        user_props_insert.append(tuple(prop))

                    await conn.executemany("INSERT INTO properties (uri, value, read_only, space_id, node_path) "
                                           "VALUES ($1, $2, $3, $4, $5)",
                                           user_props_insert)

                    await app['abstract_space'].copy_storage_node(target_record['type'], src,
                                                                  dest_record['type'], dest)

                else:
                    await conn.execute("update nodes set path = $2 || subpath(path, nlevel($1)-1) "
                                       "where path <@ $1 and space_id=$3",
                                       target_path_tree,
                                       destination_path_tree,
                                       space_id)

                    await app['abstract_space'].move_storage_node(target_record['type'], src,
                                                                  dest_record['type'], dest)


    except asyncpg.exceptions.UniqueViolationError as f:
        raise VOSpaceError(409, f"Duplicate Node. {f.detail}")
