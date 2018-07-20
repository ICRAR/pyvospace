import os
import io
import asyncio
import aiohttp
import aiofiles
import shutil

from aiofiles.os import stat
from aiohttp import web

from pyvospace.server import fuzz


def copytree(src, dst, symlinks=False, ignore=None):
    if not os.path.exists(dst):
        os.makedirs(dst)
        shutil.copystat(src, dst)

    byte_copy = False
    src_id = int(os.stat(src).st_dev >> 8 & 0xff)
    dst_id = int(os.stat(dst).st_dev >> 8 & 0xff)
    if src_id != dst_id:
        byte_copy = True

    lst = os.listdir(src)
    if ignore:
        excl = ignore(src, lst)
        lst = [x for x in lst if x not in excl]
    for item in lst:
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if symlinks and os.path.islink(s):
            if os.path.lexists(d):
                os.remove(d)
            os.symlink(os.readlink(s), d)
            try:
                st = os.lstat(s)
                mode = stat.S_IMODE(st.st_mode)
                os.lchmod(d, mode)
            except:
                pass  # lchmod not available
        elif os.path.isdir(s):
            copytree(s, d, symlinks, ignore)
        else:
            if byte_copy:
                shutil.copy2(s, d)
            else:
                os.rename(s, d)


def _move(src, dst, create_dir=True):
    if create_dir:
        if not os.path.exists(os.path.dirname(dst)):
            os.makedirs(dst)
    shutil.move(src, dst)


async def mkdir(path):
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, os.makedirs, path)
    except FileExistsError as e:
        pass


async def remove(path):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, os.remove, path)


async def move(src, dest):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _move, src, dest)


async def copy(src, dest):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, copytree, src, dest)


async def isfile(path):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, os.path.isfile, path)


async def rmtree(path):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, shutil.rmtree, path)


async def exists(path):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, os.path.exists, path)


async def send_file(request, file_name, file_path):
    response = web.StreamResponse()
    try:
        file_size = (await stat(file_path)).st_size

        response.headers[aiohttp.hdrs.CONTENT_TYPE] = "application/octet-stream"
        response.headers[aiohttp.hdrs.CONTENT_LENGTH] = str(file_size)
        response.headers[aiohttp.hdrs.CONTENT_DISPOSITION] = f"attachment; filename=\"{file_name}\""

        sent = 0
        await response.prepare(request)
        async with aiofiles.open(file_path, mode='rb') as input_file:
            while sent < file_size:
                buff = await input_file.read(io.DEFAULT_BUFFER_SIZE)
                if not buff:
                    raise IOError('file read error')
                await fuzz()
                await response.write(buff)
                sent += len(buff)
        return response
    finally:
        await asyncio.shield(response.write_eof())
