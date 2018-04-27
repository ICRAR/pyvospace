import os
import io
import asyncio
import aiofiles
import uuid

from pyvospace.server.node import get_transfer_job, set_node_busy, NodeType
from pyvospace.server.exception import VOSpaceError
from pyvospace.server.uws import set_uws_phase_to_error, set_uws_phase_to_completed


async def make_dir(path):
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, os.makedirs, path)
    except FileExistsError as e:
        pass


async def upload_to_node(app, request):
    job_id = request.match_info.get('job_id', None)

    reader = request.content

    async with app['db_pool'].acquire() as conn:
        async with conn.transaction():
            path_tree = None

            try:
                results = await get_transfer_job(conn, job_id)

                path = list(filter(None, results['path'].split('.')))
                path_tree = '/'.join(path)

                await set_node_busy(conn, path_tree, True)

                root_dir = app['root_dir']
                file_name = f'{root_dir}/{path_tree}'

                if results['type'] == NodeType.ContainerNode:
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
                if results['type'] == NodeType.ContainerNode:
                    pass

                await set_uws_phase_to_completed(app['db_pool'], job_id)

            except VOSpaceError as e:
                await set_uws_phase_to_error(app['db_pool'], job_id, str(e))
                raise

            except Exception as f:
                await set_uws_phase_to_error(app['db_pool'], job_id, str(f))
                raise VOSpaceError(500, str(f))

            finally:
                if path_tree:
                    await set_node_busy(conn, path_tree, False)

