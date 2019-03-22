#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2018
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA

import os
import io
import asyncio
import aiohttp
import aiofiles
import shutil
import tarfile
import urllib

import pdb

from pathlib import Path
from aiofiles.os import stat
from aiohttp import web
from contextlib import suppress

from pyvospace.server import fuzz
from pyvospace.core.model import ContainerNode, StructuredDataNode, Property

class CountedReader:
    """A wrapper class to count the number of bytes being sent from a stream"""
    def __init__(self, content):
        self._content=content
        self._size=0
        self._iter=None

    def __aiter__(self):
        #self._iter=self._content.__aiter__()
        self._iter=self._content.iter_chunked(io.DEFAULT_BUFFER_SIZE)
        return self

    async def __anext__(self):
        buffer=await self._iter.__anext__()
        self._size+=len(buffer)
        return buffer

class ControlledReader:
    """A wrapper class to limit the number of bytes returned from a stream
    to exactly content_length bytes"""

    def __init__(self, content, content_length):
        self._content = content
        self._content_length=content_length
        self._bytes_read = 0
        self._iter = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        # What is the minimum number of bytes to read?
        bytes_to_read = min(io.DEFAULT_BUFFER_SIZE, self._content_length - self._bytes_read)
        if bytes_to_read <= 0:
            raise StopAsyncIteration
        else:
            buffer = await self._content.readexactly(bytes_to_read)
            self._bytes_read+=bytes_to_read
            return buffer

def copytree(src, dst, symlinks=False, ignore=None):
    if not os.path.exists(dst):
        os.makedirs(dst)
        shutil.copystat(src, dst)

    byte_copy = False
    # Checking if on the same volume
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
                try:
                    os.unlink(d)
                except FileNotFoundError:
                    pass
                os.link(s, d)


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


async def statvfs(path):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, os.statvfs, path)


async def lstat(path):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, os.lstat, path)


def sync_touch(path):
    Path(path).touch()


async def touch(path):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, sync_touch, path)

async def send_file(request, file_name, file_path):
    # Send a file to a request
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

async def recv_file_from_ngas(session, hostname, port, filename_ngas, filename_local):

    """Get a single file from NGAS and put it into filename_local"""

    # The URL to contact the NGAS server
    url = f'http://{hostname}:{port}/RETRIEVE'

    # Make up the filename for retrieval from NGAS
    # How can I get the uuid from the database?
    params = {"file_id": filename_ngas}

    # Connect to NGAS
    resp_ngas = await session.get(url, params=params)

    # Rudimentry error checking on the NGAS connection
    if resp_ngas.status != 200:
        raise aiohttp.web.HTTPServerError(reason="Error in connecting to NGAS server")

    # Open the file for writing
    async with aiofiles.open(filename_local, 'wb') as fd:
        # Connect to the NGAS server and download the file
        async for chunk in resp_ngas.content.iter_chunked(io.DEFAULT_BUFFER_SIZE):
            if chunk:
                await fd.write(chunk)


async def send_file_to_ngas(session, hostname, port, filename_ngas, filename_local):

    #pdb.set_trace()

    """Send a single file to an NGAS server"""
    try:

        # Create parameters for the upload
        params = {"filename": filename_ngas,
                  "mime_type": "application/octet-stream"}

        # The URL to contact the NGAS server
        url=f'http://{hostname}:{port}/ARCHIVE'

        # Make sure a the file exists
        if filename_local is None or not os.path.isfile(filename_local):
            raise FileNotFoundError

        # Get the size of the file for content-length
        file_size = (await stat(filename_local)).st_size

        if file_size==0:
            raise ValueError(f"file {filename_local} has 0 size")

        async with aiofiles.open(filename_local, 'rb') as fd:
            # Connect to the NGAS server and upload the file
            resp = await session.post(url, params=params,
                                    data=fd,
                                    headers={"content-length" : str(file_size)})

            if resp.status!=200:
                raise aiohttp.ServerConnectionError("Error received in connecting to NGAS server")

        return(file_size)

    except Exception as e:
        # Do we do anything here?
        raise e

async def send_stream_to_ngas(request: aiohttp.web.Request, session, hostname, port, filename_ngas, logger):

    """If an incoming POST request has the content-length, send a stream direct to NGAS"""
    try:

        # Create parameters for the upload
        params = {"filename": filename_ngas,
                  "mime_type": "application/octet-stream"}

        # The URL to contact the NGAS server
        url="http://"+str(hostname)+":"+str(port)+"/ARCHIVE"

        # Test for content-length
        if 'content-length' not in request.headers:
            raise aiohttp.ServerConnectionError("No content-length in header")

        content_length=int(request.headers['Content-Length'])

        if content_length==0:
            raise ValueError

        # Create a ControlledReader from the content
        reader=ControlledReader(request.content, content_length)

        # Test for proper implementation
        if 'transfer-encoding' in request.headers:
            if request.headers['transfer-encoding']=="chunked":
                raise aiohttp.ServerConnectionError("Error, content length defined but transfer-encoding is chunked")

        # Connect to the NGAS server and upload
        resp = await session.post(url, params=params,
                                    data=reader,
                                    headers={"content-length" : str(content_length)})

        if resp.status!=200:
            raise aiohttp.ServerConnectionError("Error received in connecting to NGAS server")

        return(content_length)

    except Exception as e:
        # Do we do anything here?
        raise e

def path_to_node_tree(directory, root_node_path, owner, group_read, group_write, storage):
    root_node = ContainerNode(root_node_path,
                              owner=owner,
                              group_read=group_read,
                              group_write=group_write)
    root_node.storage = storage

    for root, dirs, files in os.walk(directory):
        dir_name = root
        if dir_name.startswith(directory):
            dir_name = dir_name[len(directory):]
        if dir_name:
            node = ContainerNode(f'{root_node_path}/{dir_name}',
                                 owner=owner,
                                 group_read=group_read,
                                 group_write=group_write)
            node.storage = storage
            root_node.insert_node_into_tree(node)

        for file in files:
            name = f"{root}/{file}"
            file_size = os.path.getsize(name)
            if name.startswith(directory):
                name = name[len(directory):]
            node_name = os.path.normpath(name)

            node = StructuredDataNode(f'{root_node_path}/{node_name}',
                                      owner=owner,
                                      group_read=group_read,
                                      group_write=group_write)
            node.storage = storage
            node.size = file_size
            root_node.insert_node_into_tree(node)
    return root_node


def tar(input, output, arcname):
    with suppress(OSError):
        os.makedirs(os.path.dirname(output))

    with tarfile.open(output, "w") as tar:
        tar.add(input, arcname=arcname)


def untar(tar_name, extract_dir, target, storage):
    with suppress(OSError):
        shutil.rmtree(extract_dir)

    with suppress(OSError):
        os.makedirs(extract_dir)

    with tarfile.open(tar_name) as tar:
        tar.extractall(path=extract_dir)

    return path_to_node_tree(extract_dir, target.path, target.owner,
                               target.group_read, target.group_write, storage)
