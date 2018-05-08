import os
import asyncio
import aiohttp
import aiofiles

from aiofiles.os import stat
from aiohttp import web


async def make_dir(path):
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, os.makedirs, path)
    except FileExistsError as e:
        pass


async def remove(path):
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, os.remove, path)
    except FileExistsError as e:
        pass


async def send_file(request, file_name, file_path):
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




