import asyncio

from contextlib import suppress
from aiohttp_security import authorized_userid, permits

from pyvospace.core.exception import *
from pyvospace.core.model import *

from .uws import PhaseLookup, UWSPhase
from .transfer import perform_transfer_job
from .database import NodeDatabase


async def get_node_request(request):
    identity = await authorized_userid(request)
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
            node = await request.app['db'].directory(path, conn, identity)

    if detail == 'min':
        node.remove_properties()

    if isinstance(node, DataNode):
        if detail == 'max':
            node.accepts = request.app['abstract_space'].get_accept_views(node)
            node.provides = request.app['abstract_space'].get_provide_views(node)

    if isinstance(node, ContainerNode):
        if limit:
            node.set_nodes(node.nodes[:limit])

    return node


async def delete_node_request(app, request):
    identity = await authorized_userid(request)
    path = request.path.replace('/vospace/nodes', '')
    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            node = await request.app['db'].delete(path, conn)
    with suppress(OSError):
        await app['abstract_space'].delete_storage_node(node)


async def create_node_request(request):
    identity = await authorized_userid(request)
    xml_request = await request.text()
    url_path = request.path.replace('/vospace/nodes', '')
    node = Node.fromstring(xml_request)
    if node.path != Node.uri_to_path(url_path):
        raise InvalidURI("Paths do not match")

    async with request.app['db_pool'].acquire() as conn:
        async with conn.transaction():
            await request.app['db'].insert(node, conn, identity)
            await request.app['abstract_space'].create_storage_node(node)
            node.accepts = request.app['abstract_space'].get_accept_views(node)
    return node


async def set_node_properties_request(request):
    identity = await authorized_userid(request)
    xml_request = await request.text()
    path = request.path.replace('/vospace/nodes', '')
    node = Node.fromstring(xml_request)
    if node.path != Node.uri_to_path(path):
        raise InvalidURI("Paths do not match")

    async with request.app['db_pool'].acquire() as conn:
        async with conn.transaction():
            await request.app['db'].update_properties(node, conn, identity)
            node = await request.app['db'].directory(path, conn, identity=None)
    return node


async def create_transfer_request(request):
    identity = await authorized_userid(request)
    job_xml = await request.text()
    transfer = Transfer.fromstring(job_xml)
    job = await request.app['executor'].create(transfer, UWSPhase.Pending)
    return job


async def sync_transfer_request(request):
    identity = await authorized_userid(request)
    job_xml = await request.text()
    transfer = Transfer.fromstring(job_xml)
    job = await request.app['executor'].create(transfer, UWSPhase.Executing)
    await perform_transfer_job(job, request.app, identity, sync=True)
    return job


async def get_job_request(request):
    identity = await authorized_userid(request)
    job_id = request.match_info.get('job_id', None)
    return await request.app['executor'].get(job_id)


async def get_transfer_details_request(request):
    identity = await authorized_userid(request)
    job_id = request.match_info.get('job_id', None)
    job = await request.app['executor'].get_uws_job(job_id)
    if job['phase'] < UWSPhase.Executing:
        raise InvalidJobStateError('Job not EXECUTING')
    if not job['transfer']:
        raise VOSpaceError(400, 'No transferDetails for this job.')
    return job['transfer']


async def get_job_phase_request(request):
    identity = await authorized_userid(request)
    job_id = request.match_info.get('job_id', None)
    job = await request.app['executor'].get_uws_job_phase(job_id)
    return PhaseLookup[job['phase']]


async def modify_job_request(request):
    identity = await authorized_userid(request)
    job_id = request.match_info.get('job_id', None)
    uws_cmd = await request.text()
    if not uws_cmd:
        raise VOSpaceError(400, "Invalid Request. Empty UWS phase input.")

    phase = uws_cmd.upper()
    if phase == "PHASE=RUN":
        await request.app['executor'].execute(job_id, perform_transfer_job,
                                              request.app, identity, False)

    elif phase == "PHASE=ABORT":
        pass
        '''async with db_pool.acquire() as conn:
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
            await asyncio.shield(exec.abort(UWSKey(space_id, job_id)))'''
    else:
        raise VOSpaceError(400, f"Invalid Request. Unknown UWS phase input {uws_cmd}")

    return job_id
