import os
import io
import asyncio
import aiofiles
import uuid

from aiohttp import web
from concurrent.futures import ProcessPoolExecutor

from pyvospace.server.node import get_transfer_job, set_node_busy, NodeType
from pyvospace.server.exception import VOSpaceError
from pyvospace.server.uws import set_uws_phase, UWSPhase


async def make_dir(path):
    try:
        p = ProcessPoolExecutor()
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(p, os.makedirs, path)

    except FileExistsError as e:
        pass


async def upload_to_node(app, request):
    job_id = request.match_info.get('job_id', None)

    reader = request.content

    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            path_tree = None
            error = None
            phase = UWSPhase.Completed
            try:
                job_results, node_results = await get_transfer_job(conn,
                                                                   job_id)

                path = list(filter(None, node_results['path'].split('.')))
                path_tree = '/'.join(path)

                await set_node_busy(conn, path_tree, True)

                root_dir = app['root_dir']
                file_name = f'{root_dir}/{path_tree}'

                if node_results['type'] == NodeType.ContainerNode:
                    await make_dir(file_name)
                    file_name = f'{root_dir}/{path_tree}/{uuid.uuid4().hex}'

                async with aiofiles.open(file_name, 'wb') as f:
                    while True:
                        buffer = await reader.read(io.DEFAULT_BUFFER_SIZE)
                        if not buffer:
                            break
                        await f.write(buffer)

                # if its a container (rar, zip etc) then
                # unpack it and create nodes if neccessary
                if node_results['type'] == NodeType.ContainerNode:
                    pass

            except VOSpaceError as e:
                error = str(e)
                raise

            except Exception as f:
                error = str(f)
                raise VOSpaceError(500, error)

            finally:
                if path_tree:
                    await set_node_busy(conn, path_tree, False)

                await set_uws_phase(app['db_pool'], job_id, phase, error)
