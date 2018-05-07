import os
import asyncio
import asyncpg
import lxml.etree as ET

from contextlib import suppress
from urllib.parse import urlparse

from .uws import set_uws_phase_to_executing, set_uws_phase_to_completed, \
    get_uws_job, get_uws_job_conn, create_uws_job, UWSPhase, \
    update_uws_job, set_uws_phase_to_error, set_uws_set_already_in_state, \
    set_uws_phase_to_abort
from .node import NS, NodeType, create_node, NodeTextLookup, \
    delete_properties, get_node
from .exception import VOSpaceError, NodeDoesNotExistError, JobDoesNotExistError, \
    InvalidJobStateError, NodeBusyError, InvalidJobError


def xml_transfer_details(target, direction, protocol_endpoints):
    prot_end = []
    prot = protocol_endpoints['protocol']
    endpoints = protocol_endpoints['endpoint']

    for end in endpoints:
        end_str = f'<vos:protocol uri="{prot}"><vos:endpoint>{end}</vos:endpoint></vos:protocol>'
        prot_end.append(end_str)

    xml = '<?xml version="1.0" encoding="UTF-8"?>' \
          '<vos:transfer xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.1">' \
          f'<vos:target>{target}</vos:target><vos:direction>{direction}</vos:direction>' \
          f'{"".join(prot_end)}</vos:transfer>'

    return xml


async def get_transfer_details(db_pool, job_id):
    job = await get_uws_job(db_pool, job_id)

    if job['phase'] < UWSPhase.Executing:
        raise VOSpaceError(400, f'Job not EXECUTING. Job: {job_id}')

    if not job['transfer']:
        raise VOSpaceError(400, f'No transferDetails for this job. Job: {job_id}')

    return job['transfer']


async def data_request(app, request, func):
    job_id = request.match_info.get('job_id', None)

    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            job = await get_uws_job_conn(conn=conn, job_id=job_id, for_update=True)
            # Can only start a EXECUTING Job
            if job['phase'] != UWSPhase.Executing:
                raise InvalidJobStateError('Invalid Job State')
            # Only run job if pending has not already been called.
            # If we dont do this then pending can be called many times before
            # the running job transitions to execute creating many tasks for the same job.
            if job['already_in_state'] is True:
                raise InvalidJobStateError("Job already executing.")

            fut = app['executor'].execute(_run_transfer_job, job_id,
                                          app, request, job, func)
            with suppress(asyncio.CancelledError):
                await asyncio.shield(set_uws_set_already_in_state(conn, job_id))

    return await fut


async def _run_transfer_job(job_id, app, request, job, func):
    db_pool = app['db_pool']
    results = None
    try:
        results = await _get_node_and_set_to_busy(db_pool, job_id, job)
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await _lock_transfer_node(conn, job_id, job['target'])
                response = await func(app, conn, request, results)
        # if we are here we have completed an expensive operation,
        # threat it as successful regardless of the state of the client
        with suppress(asyncio.CancelledError):
            await asyncio.shield(set_uws_phase_to_completed(db_pool, job_id))
        return response
        # Ignore these errors i.e. don't set the job into error
    except (JobDoesNotExistError, InvalidJobStateError, NodeBusyError, InvalidJobError):
        raise
        # If the node has been deleted at some point then set the job into error
    except NodeDoesNotExistError as e:
        await asyncio.shield(set_uws_phase_to_error(db_pool, job_id, str(e)))
        raise e

    except asyncio.CancelledError:
        await asyncio.shield(set_uws_phase_to_error(db_pool, job_id, "Job Cancelled"))
        raise

    except Exception as f:
        await asyncio.shield(set_uws_phase_to_error(db_pool, job_id, str(f)))
        raise VOSpaceError(500, str(f))

    finally:
        if results and results['direction'] == 'pushToVoSpace':
            await asyncio.shield(_set_transfer_job_to_not_busy(db_pool, results['target']))


async def _get_node_and_set_to_busy(db_pool, job_id, job):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            node_result = await conn.fetchrow("select * from nodes where path=$1 for update",
                                              job['target'])

            if not node_result:
                raise NodeDoesNotExistError(f"Target node for job does not exist. JobID: {job_id}")

            # Want to allow nodes to be appended to containers in parallel.
            # Every other node should be blocked while uploading.
            if node_result['busy'] is True and node_result['type'] != NodeType.ContainerNode:
                raise NodeBusyError(f"Node Busy. Node: {node_result['path']} is busy.")

            # set the node to busy so no other clients can manipulate the data
            # only set busy when we are pushing data to the node.
            if job['direction'] == 'pushToVoSpace':
                await conn.fetchrow("update nodes set busy=true where path=$1", node_result['path'])

            path = list(filter(None, node_result['path'].split('.')))
            path_slash = '/'.join(path)
            dict_results = dict(node_result)
            dict_results['path'] = path_slash
            dict_results.update(job)
            return dict_results


async def _set_transfer_job_to_not_busy(db_pool, path):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            # set the node to not busy so no other clients can manipulate the data
            result = await conn.fetchrow("update nodes set busy=false "
                                         "where path=$1 returning path", path)
            return result


async def _lock_transfer_node(conn, job_id, target):
    # Row lock the node so updates/deletes can not occur for duration of upload/download
    node_result = await conn.fetchrow("select path from nodes where path=$1 for share", target)
    if not node_result:
        raise NodeDoesNotExistError(f"Target node for job does not exist. JobID: {job_id}")


async def modify_transfer_job_phase(app, job_id, uws_cmd):
    if not uws_cmd:
        raise VOSpaceError(400, "Invalid Request. Empty UWS phase input.")

    exec = app['executor']
    phase = uws_cmd.upper()

    if phase == "PHASE=RUN":
        async with app['db_pool'].acquire() as conn:
            async with conn.transaction():
                job = await get_uws_job_conn(conn=conn, job_id=job_id, for_update=True)
                # Can only start a PENDING Job
                if job['phase'] != UWSPhase.Pending:
                    raise InvalidJobStateError('Invalid Job State')
                # Only run job if pending has not already been called.
                # If we dont do this then pending can be called many times before
                # the running job transitions to execute creating many tasks for the same job.
                if job['already_in_state'] is False:
                    exec.execute(run_job, job_id, app, job)
                    with suppress(asyncio.CancelledError):
                        await asyncio.shield(set_uws_set_already_in_state(conn, job_id))

    elif phase == "PHASE=ABORT":
        with suppress(asyncio.CancelledError):
            await asyncio.shield(set_uws_phase_to_abort(app['db_pool'], job_id))
        with suppress(asyncio.CancelledError):
            await asyncio.shield(exec.abort(job_id))
    else:
        raise VOSpaceError(400, f"Invalid Request. Unknown UWS phase input {uws_cmd}")


async def run_job(job_id, app, job):
    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            await perform_transfer_job(app=app, conn=conn, job_id=job_id,
                                       job_xml=job['job_info'], phase=UWSPhase.Executing)


async def create_transfer_job(app, job_xml, phase=UWSPhase.Pending):
    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            job_id = await create_uws_job(app=app, conn=conn, job_info=job_xml, phase=phase)
            # If the phase is Executing its assumes its a sync transfer, so run it now!
            if phase == UWSPhase.Executing:
                await perform_transfer_job(app=app, conn=conn, job_id=job_id,
                                           job_xml=job_xml, phase=phase, sync=True)
            return job_id


async def perform_transfer_job(app, conn, job_id, job_xml, phase, sync=False):
    try:
        try:
            await _perform_transfer_job(app=app, conn=conn, job_id=job_id,
                                         job_xml=job_xml, phase=phase, sync=sync)
        except BaseException as f:
            raise VOSpaceError(500, str(f))
    except VOSpaceError as e:
        await asyncio.shield(set_uws_phase_to_error(app['db_pool'], job_id, e.error))
        raise e


async def _perform_transfer_job(app, conn, job_id, job_xml, phase, sync):
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

            node_view_uri = None
            node_view = root.find('vos:view', NS)
            if node_view is not None:
                node_view_uri = node_view.attrib.get('uri', None)
                if node_view_uri is None:
                    raise VOSpaceError(400, "Invalid Argument. View uri not found.")

            # get the node and its parent and lock it from being updated or deleted
            node_results = await get_node(conn, target_path_norm, app['space_id'])

            if direction.text == 'pushToVoSpace':
                provides_protocols = app['plugin'].get_supported_import_provides_protocols()
                if prot_uri not in provides_protocols:
                    raise VOSpaceError(400, f"Protocol Not Supported. "
                                            f"Protocol {prot_uri} not supported.")

                if node_results:
                    node_type_text = NodeTextLookup[node_results['type']]
                else:
                    node_type_text = NodeTextLookup[NodeType.DataNode]

                import_views = app['plugin'].get_supported_import_accepts_views(target_path_norm,
                                                                                node_type_text)
                if node_view_uri:
                    if node_view_uri not in import_views:
                        raise VOSpaceError(400, f"View Not Supported. View {node_view_uri} not supported.")

                # If there is no Node at the target URI, then the service SHALL
                # create a new Node using the uri and the default xsi:type for the space.
                if not node_results:
                    await create_node(app=app,
                                      conn=conn,
                                      uri_path=target_path_norm,
                                      node_type=node_type_text,
                                      properties=None)
                else:
                    # If a Node already exists at the target URI,
                    # then the data SHALL be imported into the existing Node
                    # and the Node properties SHALL be cleared unless the node is a ContainerNode.
                    if node_results['type'] != NodeType.ContainerNode:
                        await delete_properties(conn, target_path_norm, app['space_id'])

            else:
                if not node_results:
                    raise VOSpaceError(404, f'Node Not Found. {target_path_norm} not found.')

                provides_protocols = app['plugin'].get_supported_export_provides_protocols()
                if prot_uri not in provides_protocols:
                    raise VOSpaceError(400, f"Protocol Not Supported. Protocol {prot_uri} not supported.")

                node_type_text = NodeTextLookup[node_results['type']]

            end_points = app['plugin'].get_protocol_endpoints(uws_job_id=job_id,
                                                              target_path=target_path_norm,
                                                              target_type=node_type_text,
                                                              direction=direction.text,
                                                              protocol=prot_uri,
                                                              view=node_view_uri,
                                                              params=trans_params)

            xml_transfer = xml_transfer_details(target=target.text,
                                                direction=direction.text,
                                                protocol_endpoints=end_points)

            space_name = app['space_name']
            attr_vals = []
            attr_vals.append('<uws:result id="transferDetails" '
                             f'xlink:href="/vospace/transfers/{job_id}/results/transferDetails"/>')
            attr_vals.append('<uws:result id="dataNode" '
                             f'xlink:href="vos://{space_name}!vospace/{target_path_norm}"/>')
            result = f"<uws:results>{''.join(attr_vals)}</uws:results>"

            await update_uws_job(app=app, conn=conn, job_id=job_id, target=target_path_norm,
                                 direction=direction_path_norm, transfer=xml_transfer,
                                 result=result, phase=phase)
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

            await set_uws_phase_to_executing(app['db_pool'], job_id)
            await move_nodes(app=app,
                             target_path=target_path_norm,
                             direction_path=direction_path_norm,
                             perform_copy=copy_node)
            # need to shield because we have successfully compeleted
            # a potentially expensive operation
            with suppress(asyncio.CancelledError):
                await asyncio.shield(set_uws_phase_to_completed(app['db_pool'], job_id))

    except VOSpaceError as f:
        raise f

    except asyncpg.exceptions.UniqueViolationError as f:
        raise VOSpaceError(409, f"Duplicate Node. {target_path_norm} already exists.")

    except asyncpg.exceptions.ForeignKeyViolationError:
        raise VOSpaceError(404, f"Node Not Found. {target_path_norm} not found.")

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise VOSpaceError(500, str(e))


async def move_nodes(app, target_path, direction_path, perform_copy):
    space_id = app['space_id']
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

                    await conn.execute("insert into nodes(name, type, space_id, path) ( "
                                       "select name, type, space_id, $2||subpath(path, nlevel($1)-1) as concat "
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

                else:
                    await conn.execute("update nodes set path = $2 || subpath(path, nlevel($1)-1) "
                                       "where path <@ $1 and space_id=$3",
                                       target_path_tree,
                                       destination_path_tree,
                                       space_id)

    except asyncpg.exceptions.UniqueViolationError as f:
        raise VOSpaceError(409, f"Duplicate Node. {f.detail}")
