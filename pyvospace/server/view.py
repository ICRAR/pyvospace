from contextlib import suppress
from aiohttp_security import authorized_userid, permits

from pyvospace.core.exception import VOSpaceError, PermissionDenied, InvalidURI, InvalidJobStateError
from pyvospace.core.model import UWSPhase, UWSPhaseLookup, Node, DataNode, ContainerNode, Transfer

from .transfer import perform_transfer_job
from .database import NodeDatabase


async def get_properties_request(request):
    identity = await authorized_userid(request)
    if identity is None:
        raise PermissionDenied(f'Credentials not found.')
    properties = request.app['abstract_space'].get_properties()
    assert properties
    results = await request.app['db'].get_contains_properties()
    properties.contains = NodeDatabase._resultset_to_properties(results)
    return properties


async def get_node_request(request):
    identity = await authorized_userid(request)
    if identity is None:
        raise PermissionDenied(f'Credentials not found.')
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
    if identity is None:
        raise PermissionDenied(f'Credentials not found.')
    path = request.path.replace('/vospace/nodes', '')
    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            node = await request.app['db'].delete(path, conn, identity)
    with suppress(OSError):
        await app['abstract_space'].delete_storage_node(node)


async def create_node_request(request):
    identity = await authorized_userid(request)
    if identity is None:
        raise PermissionDenied(f'Credentials not found.')
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
    if identity is None:
        raise PermissionDenied(f'Credentials not found.')
    xml_request = await request.text()
    path = request.path.replace('/vospace/nodes', '')
    node = Node.fromstring(xml_request)
    if node.path != Node.uri_to_path(path):
        raise InvalidURI("Paths do not match")

    async with request.app['db_pool'].acquire() as conn:
        async with conn.transaction():
            node = await request.app['db'].update_properties(node, conn, identity)
    return node


async def create_transfer_request(request):
    identity = await authorized_userid(request)
    if identity is None:
        raise PermissionDenied(f'Credentials not found.')
    job_xml = await request.text()
    transfer = Transfer.fromstring(job_xml)
    if not await request.app.permits(identity, 'createTransfer', context=transfer):
        raise PermissionDenied('creating transfer job denied.')
    job = await request.app['executor'].create(transfer, identity, UWSPhase.Pending)
    return job


async def sync_transfer_request(request):
    identity = await authorized_userid(request)
    if identity is None:
        raise PermissionDenied(f'Credentials not found.')
    job_xml = await request.text()
    transfer = Transfer.fromstring(job_xml)
    if not await request.app.permits(identity, 'createTransfer', context=transfer):
        raise PermissionDenied('creating transfer job denied.')
    job = await request.app['executor'].create(transfer, identity, UWSPhase.Executing)
    await perform_transfer_job(job, request.app, identity, sync=True)
    return job


async def get_job_request(request):
    identity = await authorized_userid(request)
    if identity is None:
        raise PermissionDenied(f'Credentials not found.')
    job_id = request.match_info.get('job_id', None)
    job = await request.app['executor'].get(job_id)
    if identity != job.owner:
        raise PermissionDenied(f'{identity} is not the owner of the job.')
    return job


async def get_transfer_details_request(request):
    identity = await authorized_userid(request)
    if identity is None:
        raise PermissionDenied(f'Credentials not found.')
    job_id = request.match_info.get('job_id', None)
    job = await request.app['executor'].get_uws_job(job_id)
    if identity != job['owner']:
        raise PermissionDenied(f'{identity} is not the owner of the job.')
    if job['phase'] < UWSPhase.Executing:
        raise InvalidJobStateError('Job not EXECUTING')
    if not job['transfer']:
        raise VOSpaceError(400, 'No transferDetails for this job.')
    return job['transfer']


async def get_job_phase_request(request):
    identity = await authorized_userid(request)
    if identity is None:
        raise PermissionDenied(f'Credentials not found.')
    job_id = request.match_info.get('job_id', None)
    job = await request.app['executor'].get_uws_job_phase(job_id)
    if identity != job['owner']:
        raise PermissionDenied(f'{identity} is not the owner of the job.')
    return UWSPhaseLookup[job['phase']]


async def modify_job_request(request):
    identity = await authorized_userid(request)
    if identity is None:
        raise PermissionDenied(f'Credentials not found.')
    job_id = request.match_info.get('job_id', None)
    uws_cmd = await request.text()
    if not uws_cmd:
        raise VOSpaceError(400, "Invalid Request. Empty UWS phase input.")

    phase = uws_cmd.upper()
    if phase == "PHASE=RUN":
        await request.app['executor'].execute(job_id, identity, perform_transfer_job,
                                              request.app, identity, False)
    elif phase == "PHASE=ABORT":
        await request.app['executor'].abort(job_id, identity)
    else:
        raise VOSpaceError(400, f"Invalid Request. Unknown UWS phase input {uws_cmd}")

    return job_id
