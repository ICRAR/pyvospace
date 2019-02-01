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

import io
import os
import uuid
import asyncio
import aiofiles
import aiohttp
import numpy as np

from aiohttp import web
from aiohttp_security import setup as setup_security
from aiohttp_security import SessionIdentityPolicy
from aiohttp_session import setup as setup_session, get_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from contextlib import suppress
from concurrent.futures import ProcessPoolExecutor

from pyvospace.core.model import NodeType, View

# Not sure if I need these
from pyvospace.server.spaces.ngas.utils import mkdir, remove, send_file, move, copy, rmtree, tar, untar
from pyvospace.server.storage import HTTPSpaceStorageServer
from pyvospace.server import fuzz, fuzz01
from pyvospace.server.spaces.ngas.auth import DBUserNodeAuthorizationPolicy
from pyvospace.server.uws import StorageUWSJob

class NGASStorageServer(HTTPSpaceStorageServer):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(cfg_file, *args, **kwargs)

        # What is this for?
        self.secret_key = self.config['Space']['secret_key']
        self.domain = self.config['Space']['domain']

        # Choose an NGAS server, initially at random,
        # there is probably a more elegant implementation
        self.ngas_server_strings=self.config['Space']['ngas_servers']
        server_index=np.random.choice([n for n in range(0,len(self.ngas_servers))],1)
        self.ngas_server_string=self.ngas_servers[server_index]

        # Do I need a root_dir, probably not.
        self.root_dir = self.parameters['root_dir']
        if not self.root_dir:
            # Default way of raising an exception
            raise Exception('root_dir not found.')

        # Do I need a staging directory?
        self.staging_dir = self.parameters['staging_dir']
        if not self.staging_dir:
            raise Exception('staging_dir not found.')

        # Is a pooled server already available?
        self.process_executor = ProcessPoolExecutor(max_workers=32)
        self.on_shutdown.append(self.shutdown)

    async def shutdown(self):
        loop = asyncio.get_event_loop()
        await super().shutdown()
        await loop.run_in_executor(None, self.process_executor.shutdown)

    async def setup(self):
        await super().setup()

        await mkdir(self.root_dir)
        await mkdir(self.staging_dir)

        setup_session(self,
                      EncryptedCookieStorage(
                          secret_key=self.secret_key.encode(),
                          cookie_name='PYVOSPACE_COOKIE',
                          domain=self.domain))

        # Does this need to be different?
        setup_security(self,
                       SessionIdentityPolicy(),
                       DBUserNodeAuthorizationPolicy(self.name, self.db_pool, self.root_dir))

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):
        app = NGASStorageServer(cfg_file, *args, **kwargs)
        await app.setup()
        return app

    async def download(self, job: StorageUWSJob, request: aiohttp.web.Request):
        # Download files from the server, returns a Response
        root_dir = self.root_dir
        path_tree = job.transfer.target.path

        # Get the session of a request? Is this the best way to do it?
        session = await get_session(request)

        # Get the UUID on the transaction
        uuid=job.transfer.target.id

        # Filename to be used with the NGAS object store
        base_name=os.path.basename(path_tree)
        filename_ngas=base_name

        # URL for retrieval from NGAS
        url_ngas=self.ngas_server_string+"/RETRIEVE"

        # Make up the filename for retrieval from NGAS
        # How can I get the uuid from the database?
        params={"file_id" : filename_ngas}

        # I suspect this streams content from the NGAS request to the client
        # In a non-blocking way, but I am not sure
        try:
            # Connect to NGAS
            resp_ngas = await session.get(url_ngas, params=params)
            assert(resp_ngas.status==200), "Couldn't find file on NGAS server"

            # Otherwise create the client
            resp_client=web.StreamResponse()

            # Do we need to do anything here with headers?
            resp_client.headers=resp_ngas.headers

            # Prepare the connection
            await resp_client.prepare(request)

            # Read from source and and write destination in buffers
            async for chunk in resp_ngas.content.iter_chunked(io.DEFAULT_BUFFER_SIZE):
                if chunk:
                    await resp_client.write(chunk)

            # Finish the stream
            await resp_client.write_eof()
            return(resp_client)

        except Exception:
            raise


        # Handling connection errors?

        # root_dir = self.root_dir
        # path_tree = job.transfer.target.path
        # if job.transfer.target.node_type == NodeType.ContainerNode:
        #     # Checking if request wants a tar file
        #     if job.transfer.view != View('ivo://ivoa.net/vospace/core#tar'):
        #         return web.Response(status=400, text=f'Unsupported Container View. '
        #                                              f'View: {job.transfer.view}')
        #
        #     tar_file = f'{self.staging_dir}/{uuid.uuid4()}/{os.path.basename(path_tree)}.tar'
        #     stage_path = f'{self.staging_dir}/{uuid.uuid4()}/{path_tree}'
        #     real_path = f'{self.root_dir}/{path_tree}'
        #     async with job.transaction(exclusive=False):
        #         await copy(real_path, stage_path)
        #
        #     try:
        #         # Tarring up the sources, I might need to ask the database for all
        #         loop = asyncio.get_event_loop()
        #         await loop.run_in_executor(self.process_executor, tar,
        #                                    stage_path, tar_file, os.path.basename(path_tree))
        #         return await send_file(request, os.path.basename(tar_file), tar_file)
        #     finally:
        #         with suppress(Exception):
        #             await asyncio.shield(rmtree(os.path.dirname(tar_file)))
        #         with suppress(Exception):
        #             await asyncio.shield(rmtree(os.path.dirname(stage_path)))
        # else:
        #     file_path = f'{root_dir}/{path_tree}'
        #     return await send_file(request, os.path.basename(path_tree), file_path)

    async def upload(self, job: StorageUWSJob, request: aiohttp.web.Request):
        # Upload files to the NGAS server
        reader=request.content

        # Get the path tree
        path_tree = job.transfer.target.path

        # Get the session of a request? Is this the best way to do it?
        session = await get_session(request)

        # Get the UUID for the transaction
        space_id = job.transfer.target.space_id

        # Make up the URL for the retrieval from NGAS
        url_ngas = self.ngas_server_string + "/ARCHIVE"

        # Create the filename that is to be used with the NGAS object store
        base_name=os.path.basename(path_tree)
        filename_ngas=base_name

        # Make up the filename for upload to NGAS
        params = {"filename": filename_ngas}

        # Stream content from the user to NGAS in a streaming manner
        try:
            resp_ngas= await session.post(url_ngas, params=params)
            size=0
            async for buffer in reader.iter_chunked(io.DEFAULT_BUFFER_SIZE):
                if buffer:
                    size+=len(buffer)
                    await resp_ngas.write(buffer)
            await resp_ngas.write_eof()

            assert(resp_ngas.status==200)

            # Let the client know the transaction was successful
            return web.Response(status=200)

            # Inform the database of new data
            async with job.transaction() as tr:
                node = tr.target # get the target node that is associated with the data
                node.size = size # set the size
                node.storage = self.storage # set the storage back end so it can be found
                await asyncio.shield(fuzz01(2))
                await asyncio.shield(node.save()) # save details to db

        except (asyncio.CancelledError, Exception):
            # Handle a cancellation error
            raise


        # # Stream on the content
        # reader = request.content
        # path_tree = job.transfer.target.path
        # # this UUID is only used for temporary staging file
        # target_id = uuid.uuid4()
        # # Base name of the file
        # base_name = f'{target_id}_{os.path.basename(path_tree)}'
        # # File name without UUID?
        # real_file_name = f'{self.root_dir}/{path_tree}'
        # # File name with UUID
        # stage_file_name = f'{self.staging_dir}/{base_name}'
        # try:
        #     # Read from buffer to temporary file
        #     size = 0
        #     async with aiofiles.open(stage_file_name, 'wb') as f:
        #         while True:
        #             buffer = await reader.read(io.DEFAULT_BUFFER_SIZE)
        #             if not buffer:
        #                 break
        #             await fuzz()
        #             await f.write(buffer)
        #             size += len(buffer)
        #
        #     if job.transfer.target.node_type == NodeType.ContainerNode:
        #         # Check if the upload is a tar file?
        #         # What do I do about directory structures?
        #         if job.transfer.view != View('ivo://ivoa.net/vospace/core#tar'):
        #             return web.Response(status=400, text=f'Unsupported Container View. '
        #                                                  f'View: {job.transfer.view}')
        #         extract_dir = f'{self.staging_dir}/{target_id}/{path_tree}/'
        #         try:
        #
        #             loop = asyncio.get_event_loop()
        #             # Untarring process
        #             root_node = await loop.run_in_executor(self.process_executor,
        #                                                    untar,
        #                                                    stage_file_name,
        #                                                    extract_dir,
        #                                                    job.transfer.target,
        #                                                    self.storage)
        #             async with job.transaction() as tr:
        #                 node = tr.target
        #                 node.size = size
        #                 node.storage = self.storage
        #                 node.nodes = root_node.nodes
        #                 await asyncio.shield(node.save())
        #                 await asyncio.shield(copy(extract_dir, real_file_name))
        #         finally:
        #             with suppress(Exception):
        #                 await asyncio.shield(rmtree(f'{self.staging_dir}/{target_id}'))
        #     else:
        #         async with job.transaction() as tr:
        #             node = tr.target # get the target node that is associated with the data
        #             node.size = size # set the size
        #             node.storage = self.storage # set the storage back end so it can be found
        #             await asyncio.shield(fuzz01(2))
        #             await asyncio.shield(node.save()) # save details to db
        #             await asyncio.shield(move(stage_file_name, real_file_name)) # move in single transaction
        #
        #     # http OK?
        #     return web.Response(status=200)
        #
        # except (asyncio.CancelledError, Exception):
        #     # Handle a cancellation
        #     raise
        # finally:
        #     # Remove stage file name no matter what
        #     with suppress(Exception):
        #         await asyncio.shield(remove(stage_file_name))
