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

from pathlib import Path
from aiofiles.os import stat
from aiohttp import web
from contextlib import suppress

from pyvospace.server import fuzz
from pyvospace.core.model import ContainerNode, StructuredDataNode, Property

class ChunkedByteCounter:
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
    # Re-implement this?
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


async def send_stream_to_ngas_rawhttp(request: aiohttp.web.Request,
                                hostname, port, filename_ngas, logger):

    """Send a binary file from a request directly to an NGAS server
    using raw and potentially unsafe http requests"""
    size=0
    try:

        params={"filename": filename_ngas,
                "mime_type":"application/octet-stream"}

        encoded_parms=urllib.parse.urlencode(params)
        ngas_string="ngas_storage"

        # Get the number of bytes in a file
        nbytes=request.content_length
        content_type=request.content_type

        # Open a HTTP connection to the NGAS server in a similar fashion to the
        # Way Curl does it, nothing else seems elegant
        # Make up HTTP Post header
        raw_header= f"POST /ARCHIVE?{encoded_parms} HTTP/1.1\r\n" \
                    f"Host: {hostname}:{port}\r\n" \
                    f"Accept: */*\r\n" \
                    f"User-Agent: {ngas_string}\r\n" \
                    f"Content-Type: {content_type}\r\n" \
                    f"Content-Length: {nbytes}\r\n" \
                    f"Expect: 100-continue\r\n" \
                    f"\r\n"

        # Encode the raw header
        logger.debug(raw_header)
        raw_header=raw_header.encode("utf-8")
        (reader, writer) = await asyncio.open_connection(host=hostname, port=port)

        while True:
            buffer=await request.content.read(io.DEFAULT_BUFFER_SIZE)
            if buffer:
                size+=len(buffer)
                writer.write(buffer)
                await writer.drain()
            else:
                break

        # Not sure why we have to shield write_eof...
        await asyncio.shield(writer.write_eof())
        await writer.drain()

        # Look for ok Message in the first column
        firstline = (await reader.readline()).decode().split(" ")
        if '200' not in firstline:
            # Do we let the client know?
            raise aiohttp.ServerConnectionError("Error received in connecting to NGAS server")
            logger.debug("Error in reply from NGAS server")
        else:
            # Do we do something here to let the client know?
            pass

        # Drain the rest of reader? Not sure if we need to do this
        while True:
            buffer=await reader.read(io.DEFAULT_BUFFER_SIZE)
            if not buffer:
                break

        # Close the connection to NGAS
        writer.close()

    except Exception as e:
        # We can do things, otherwise raise the Exception for now
        raise e

    return(size)

async def send_file_to_ngas(session, hostname, port, filename_ngas, filename_local, logger):

    """Send a single file to an NGAS server"""
    try:

        # Create parameters for the upload
        params = {"filename": filename_ngas,
                  "mime_type": "application/octet-stream"}

        # The URL to contact the NGAS server
        url="http://"+str(hostname)+":"+str(port)+"/ARCHIVE"

        # Get the size of the file for content-length
        file_size = (await stat(filename_local)).st_size

        async with aiofiles.open(filename_local, 'rb') as fd:
            # Connect to the NGAS server and upload the file
            # Should I be using sendfile from Python?
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

    """If a request has the content-length, send a stream direct to NGAS"""
    try:

        # Create parameters for the upload
        params = {"filename": filename_ngas,
                  "mime_type": "application/octet-stream"}

        # The URL to contact the NGAS server
        url="http://"+str(hostname)+":"+str(port)+"/ARCHIVE"

        # Create a bytecounter from the post request content
        bytecounter=ChunkedByteCounter(request.content)

        logger.debug(f"url is {url}")

        # Test for content-length
        if 'content-length' not in request.headers:
            raise aiohttp.ServerConnectionError("No content-length in header")

        # Test for proper implementation
        if 'transfer-encoding' in request.headers:
            if request.headers['transfer-encoding']=="chunked":
                raise aiohttp.ServerConnectionError("Error, content length defined but transfer-encoding is chunked")

        # Connect to the NGAS server and upload
        resp = await session.post(url, params=params,
                                    data=bytecounter,
                                    headers={"content-length" : request.headers['content-length']})

        if resp.status!=200:
            raise aiohttp.ServerConnectionError("Error received in connecting to NGAS server")

        logger.debug(f"bytcounter size is {bytecounter._size}")

        return(bytecounter._size)

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
