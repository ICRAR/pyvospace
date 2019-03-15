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
import pdb
import traceback
import json

from aiohttp import web
from aiohttp_security import setup as setup_security
from aiohttp_security import SessionIdentityPolicy
from aiohttp_session import setup as setup_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from contextlib import suppress
from concurrent.futures import ProcessPoolExecutor

from pyvospace.core.model import NodeType, View

# Not sure if I need these
from pyvospace.server.spaces.ngas.utils import mkdir, remove, send_file, move, copy, rmtree, tar, untar
from pyvospace.server.spaces.ngas.utils import send_stream_to_ngas, send_file_to_ngas, recv_file_from_ngas
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

        # NGAS parameters section - assumes that NGAS servers (if more than one)
        # all point to the same storage, i.e they are federated
        # Choose an NGAS server, initially at random,
        pdb.set_trace()
        self.ngas_servers=json.loads(self.config['Storage']['ngas_servers'])
        server_index=np.random.choice([n for n in range(0,len(self.ngas_servers))],1)[0]
        self.ngas_server=self.ngas_servers[server_index]

        # Extract hostname and port
        self.ngas_hostname=self.ngas_server["hostname"]
        self.ngas_port=int(self.ngas_server["port"])
        self.ngas_session = aiohttp.ClientSession()

        # Do I need a root_dir, probably not.
        self.root_dir = self.parameters['root_dir']

        #if not self.root_dir:
            # Default way of raising an exception
        #    raise Exception('root_dir not found.')

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
        # Close the NGAS session
        await self.ngas_session.close()

    async def setup(self):
        await super().setup()

        # await mkdir(self.root_dir)
        # Probably don't need to make the root directory

        # We do need a staging directory
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
        """Download files from the NGAS server"""
        root_dir = self.root_dir
        path_tree = job.transfer.target.path

        if job.transfer.target.node_type == NodeType.ContainerNode:

            # Gather the files from NGAS, send to a directory,
            # tar and send to client
            # Should we implement a streaming creation of the tarfile?

            # Make a directory in staging area
            path_tree=job.transfer.target.path
            stage_dir = os.path.normpath(f'{self.staging_dir}/{uuid.uuid4()}')

            # make the stage directory
            await mkdir(stage_dir)

            # Now loop over each current node in NGAS and fetch files
            current_node=job.transfer.target

            # Current node
            if job.transfer.view != View('ivo://ivoa.net/vospace/core#tar'):
                return web.Response(status=400, text=f'Unsupported Container View. '
                                                      f'View: {job.transfer.view}')

            # Now loop over each node and download to file
            for node in current_node.walk(current_node):
                node_path=os.path.normpath('{stage_dir}/{node.path}')
                if node.node_type==NodeType.ContainerNode:
                    # Make a directory in the node path
                    await mkdir(node_path)
                else:
                    # Make a local filename
                    # Make an NGAS filename
                    filename_local=node_path
                    filename_ngas='f{os.path.basename(node_path)}_{node.id}'

                    # Fetch a file from NGAS
                    await recv_file_from_ngas(  self.ngas_session,
                                                self.ngas_hostname,
                                                self.ngas_port,
                                                filename_ngas,
                                                filename_local)

            # Tar file
            tar_file = os.path.normpath(f'{stage_dir}/{uuid.uuid4()}/{os.path.basename(path_tree)}.tar')

            try:
                # Tarring up the sources, I might need to ask the database for all
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(self.process_executor, tar,
                                           stage_path, tar_file, os.path.basename(path_tree))
                return await send_file(request, os.path.basename(tar_file), tar_file)
            finally:
                with suppress(Exception):
                    await asyncio.shield(rmtree(os.path.dirname(tar_file)))
                with suppress(Exception):
                    await asyncio.shield(rmtree(os.path.dirname(stage_dir)))


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



        else:

            # Get the UUID on the transaction from the database
            id=job.transfer.target.id

            # Filename to be used with the NGAS object store
            base_name=os.path.basename(path_tree)
            filename_ngas=f'{base_name}_{id}'

            # URL for retrieval from NGAS
            url_ngas=f'http://{self.ngas_hostname}:{self.ngas_port}/RETRIEVE'

            # Make up the filename for retrieval from NGAS
            # How can I get the uuid from the database?
            params={"file_id" : filename_ngas}

            # Connect to NGAS
            resp_ngas = await self.ngas_session.get(url_ngas, params=params)

            # Rudimentry error checking on the NGAS connection
            if resp_ngas.status!=200:
                raise aiohttp.web.HTTPServerError(reason="Error in connecting to NGAS server")

            # Otherwise create the client
            resp_client=web.StreamResponse()

            # Update the headers
            resp_client.headers.update(resp_ngas.headers)

            # Change the filename?
            resp_client.headers['Content-Disposition']=f'attachment; filename=\"{base_name}\"'

            # Prepare the connection
            await resp_client.prepare(request)

            # Read from source and and write destination in buffers
            async for chunk in resp_ngas.content.iter_chunked(io.DEFAULT_BUFFER_SIZE):
                if chunk:
                    await resp_client.write(chunk)

            # Finish the stream
            await resp_client.write_eof()
            return(resp_client)


    async def upload(self, job: StorageUWSJob, request: aiohttp.web.Request):
        # Upload file/s to the NGAS server

        # Get the path tree
        path_tree = job.transfer.target.path
        # Get the base filename
        base_name = os.path.basename(path_tree)

        # Check for content length in the headers of the incoming request
        # This will inform the user how we respond to the request
        if 'content-length' in request.headers:
            content_length=request.headers['content-length']
        else:
            content_length=None

        if job.transfer.target.node_type == NodeType.ContainerNode:
            #raise NotImplementedError("Container support coming soon")
            # Stream to file
            # Unpack
            # Check if nodes already exist in the tree
            # Walk the tree
            # Upload every file

            # Incoming content
            reader=request.content

            # Temporary file to stage to
            stage_file_name = os.path.join(self.staging_dir, base_name)

            # Temporary UUID for staging the directory
            target_id = uuid.uuid4()


            path_tree = job.transfer.target.path

            try:
                # Read from buffer to temporary file

                size = 0
                async with aiofiles.open(stage_file_name, 'wb') as f:
                    while True:
                        buffer = await reader.read(io.DEFAULT_BUFFER_SIZE)
                        if buffer:
                            await fuzz()
                            await f.write(buffer)
                            size += len(buffer)
                        else:
                            break

                #pdb.set_trace()

                if job.transfer.view != View('ivo://ivoa.net/vospace/core#tar'):
                    return web.Response(status=400, text=f'Unsupported Container View. '
                                                         f'View: {job.transfer.view}')


                # Path to extract to
                extract_dir = os.path.normpath(f'{self.staging_dir}/{target_id}')

                try:

                    loop = asyncio.get_event_loop()

                    # Untarring process
                    root_node = await loop.run_in_executor(self.process_executor,
                                                           untar,
                                                           stage_file_name,
                                                           f'{extract_dir}{path_tree}',
                                                           job.transfer.target,
                                                           self.storage)

                    # Do the walk and upload here
                    # Walk the tree and upload each file to an NGAS flat object store
                    # Keep the ID's of old nodes through checking
                    oldnode=job.transfer.target

                    pdb.set_trace()

                    # Copy old id's across and upload to NGAS

                    # Flatten the tree
                    old_node_paths=[node.path for node in oldnode.walk(oldnode)]
                    old_nodes=[node for node in oldnode.walk(oldnode)]
                    new_nodes=[node for node in root_node.walk(root_node)]

                    # Loop over new nodes
                    for new_node in new_nodes:
                        if new_node.path in old_node_paths:
                            index_old=old_node_paths.index(new_node.path)
                            old_node=old_nodes[index_old]
                            # Copy ID's
                            new_node.id=old_node.id

                        if new_node.node_type != NodeType.ContainerNode:
                            filename_local=os.path.normpath(f'{extract_dir}/{new_node.path}')
                            filename_ngas=f'{os.path.basename(new_node.path)}_{new_node.id}'

                            nbytes_transfer = await send_file_to_ngas(self.ngas_session,
                                                        self.ngas_hostname,
                                                        self.ngas_port,
                                                        filename_ngas,
                                                        filename_local)
                            new_node.size=nbytes_transfer
                        else:
                            # We have a container type, it doesn't contribute to the upload
                            new_node.size=0


                    # Now notify the database
                    async with job.transaction() as tr:
                        pdb.set_trace()
                        node = tr.target
                        node.size = size
                        node.storage = self.storage
                        # This saves all nodes under the root node
                        node.nodes = root_node.nodes
                        await asyncio.shield(node.save())
                        # real_file_name = f'{self.root_dir}/{path_tree}'
                        #await asyncio.shield(copy(extract_dir, real_file_name))
                        # Let the client know the transaction was successful
                        return web.Response(status=200)

                except Exception as e:
                    traceback.print_exception(e, SyntaxError, None)
                    raise e

                finally:
                    with suppress(Exception):
                        await asyncio.shield(rmtree(f'{self.staging_dir}/{target_id}'))

            except Exception as e:
                traceback.print_exception(e, SyntaxError, None)
                raise e


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

        else:

            # Get the UUID for the node
            id = job.transfer.target.id
            ngas_filename=f"{base_name}_{id}"

            if content_length is not None:
                # Content length exists, we can forward the stream straight to the NGAS server
                nbytes_transfer = await send_stream_to_ngas(request, self.ngas_session, self.ngas_hostname,
                                                            self.ngas_port, ngas_filename, self.logger)
            else:
                # Send the stream to a file and upload it

                # Make up a uuid for the staging of a file
                reader=request.content

                # Temporary uuid for the upload of a file
                target_id = uuid.uuid4()
                base_name = f'{target_id}_{os.path.basename(path_tree)}'

                # Temporary file to stage to
                stage_file_name = os.path.join(self.staging_dir, base_name)

                # Need to read in a specific number of bytes?
                async with aiofiles.open(stage_file_name, 'wb') as fd:
                    # We have checked that content-length must exist.
                    while True:
                        buffer = await reader.read(io.DEFAULT_BUFFER_SIZE)
                        if buffer:
                            await fuzz()
                            await fd.write(buffer)
                        else:
                            break

                # Now the file is on disk, send it
                nbytes_transfer = await send_file_to_ngas(self.ngas_session, self.ngas_hostname, self.ngas_port,
                                                        ngas_filename, stage_file_name)

                # Remove the staged file if it exists
                with suppress(Exception):
                    await asyncio.shield(remove(stage_file_name))

            # Inform the database of new data if size
            async with job.transaction() as tr:
                if nbytes_transfer:
                    node = tr.target # get the target node that is associated with the data
                    node.size = nbytes_transfer # set the size
                    node.storage = self.storage # set the storage back end so it can be found
                    await asyncio.shield(fuzz01(2))
                    await asyncio.shield(node.save()) # save details to db

                    # Let the client know the transaction was successful
                    return web.Response(status=200)


