import os
import io
import asyncio
import aiohttp
import aiofiles
import uuid

from aiofiles.os import stat
from aiohttp import web

from pyvospace.server.node import NodeType


async def make_dir(path):
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, os.makedirs, path)
    except FileExistsError as e:
        pass


async def _send_file(request, file_name, file_path):
    response = web.StreamResponse()
    file_size = (await stat(file_path)).st_size

    response.headers[aiohttp.hdrs.CONTENT_TYPE] = "application/octet-stream"
    response.headers[aiohttp.hdrs.CONTENT_LENGTH] = str(file_size)
    response.headers[aiohttp.hdrs.CONTENT_DISPOSITION] = f"attachment; filename=\"{file_name}\""

    await response.prepare(request)
    async with aiofiles.open(file_path, mode='rb') as input_file:
        # cannot use sendfile() over an SSL connection
        # defer back to user space copy and send
        # will run this in Gunicorn et al to get maximum utilisation
        while True:
            buff = await input_file.read(65536)
            if not buff:
                break
            await response.write(buff)
    await response.write_eof()
    return response


async def download(app, conn, request, job_details):
    root_dir = app['root_dir']
    path_tree = job_details['path']
    file_path = f'{root_dir}/{path_tree}'

    return await _send_file(request, job_details['name'], file_path)


async def upload(app, conn, request, job_details):
    reader = request.content

    root_dir = app['root_dir']
    path_tree = job_details['path']

    file_name = f'{root_dir}/{path_tree}'
    directory = os.path.dirname(file_name)

    await make_dir(directory)

    if job_details['type'] == NodeType.ContainerNode:
        file_name = f'{root_dir}/{path_tree}/{uuid.uuid4().hex}.zip'

    async with aiofiles.open(file_name, 'wb') as f:
        while True:
            buffer = await reader.read(io.DEFAULT_BUFFER_SIZE)
            if not buffer:
                break
            await f.write(buffer)

    # if its a container (rar, zip etc) then
    # unpack it and create nodes if neccessary
    if job_details['type'] == NodeType.ContainerNode:
        pass

    return web.Response(status=200)

