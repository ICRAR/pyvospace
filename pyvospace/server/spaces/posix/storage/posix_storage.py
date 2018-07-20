import io
import os
import uuid
import shutil
import tarfile
import asyncio
import aiofiles

from aiohttp import web
from aiohttp_security import setup as setup_security
from aiohttp_security import SessionIdentityPolicy
from aiohttp_session import setup as setup_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from contextlib import suppress

from pyvospace.core.model import NodeType, Property, View, ContainerNode, StructuredDataNode, Node
from pyvospace.server.spaces.posix.utils import mkdir, remove, send_file, move, copy, rmtree
from pyvospace.server.storage import HTTPSpaceStorageServer
from pyvospace.server import fuzz
from pyvospace.server.spaces.posix.auth import DBUserNodeAuthorizationPolicy


class PosixStorageServer(HTTPSpaceStorageServer):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(cfg_file, *args, **kwargs)

        self.secret_key = self.config['Space']['secret_key']
        self.domain = self.config['Space']['domain']

        self.root_dir = self.parameters['root_dir']
        if not self.root_dir:
            raise Exception('root_dir not found.')

        self.staging_dir = self.parameters['staging_dir']
        if not self.staging_dir:
            raise Exception('staging_dir not found.')

        self.on_shutdown.append(self.shutdown)

    async def shutdown(self):
        await super().shutdown()

    async def setup(self):
        await super().setup()

        await mkdir(self.root_dir)
        await mkdir(self.staging_dir)

        setup_session(self,
                      EncryptedCookieStorage(
                          secret_key=self.secret_key.encode(),
                          cookie_name='PYVOSPACE_COOKIE',
                          domain=self.domain))

        setup_security(self,
                       SessionIdentityPolicy(),
                       DBUserNodeAuthorizationPolicy(self.name, self.db_pool))

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):
        app = PosixStorageServer(cfg_file, *args, **kwargs)
        await app.setup()
        return app

    def untar(self, tar_name, extract_dir, target):
        root_node = ContainerNode(target.path,
                                  owner=target.owner,
                                  group_read=target.group_read,
                                  group_write=target.group_write)

        with suppress(OSError):
            shutil.rmtree(extract_dir)

        with suppress(OSError):
            os.makedirs(extract_dir)

        with tarfile.open(tar_name) as tar:
            tar.extractall(path=extract_dir)

        for root, dirs, files in os.walk(extract_dir):
            dir_name = root
            if dir_name.startswith(extract_dir):
                dir_name = dir_name[len(extract_dir):]
            if dir_name:
                node = ContainerNode(f'{target.path}/{dir_name}',
                                     owner=target.owner,
                                     group_read=target.group_read,
                                     group_write=target.group_write)
                root_node.insert_node_into_tree(node)

            for file in files:
                name = f"{root}/{file}"
                file_size = Property('ivo://ivoa.net/vospace/core#length', str(os.path.getsize(name)))
                if name.startswith(extract_dir):
                    name = name[len(extract_dir):]
                node_name = os.path.normpath(name)

                node = StructuredDataNode(f'{target.path}/{node_name}',
                                          owner=target.owner,
                                          group_read=target.group_read,
                                          group_write=target.group_write,
                                          properties=[file_size])
                root_node.insert_node_into_tree(node)
        return root_node

    async def download(self, job, request):
        root_dir = self.root_dir
        path_tree = job.transfer.target.path
        file_path = f'{root_dir}/{path_tree}'
        #TODO: retrieve tar from a ivo://ivoa.net/vospace/core#tar request
        return await send_file(request, os.path.basename(path_tree), file_path)

    async def upload(self, job, request):
        reader = request.content
        path_tree = job.transfer.target.path
        target_id = uuid.uuid4()
        base_name = f'{target_id}_{os.path.basename(path_tree)}'
        real_file_name = f'{self.root_dir}/{path_tree}'
        stage_file_name = f'{self.staging_dir}/{base_name}'
        try:
            size = 0
            async with aiofiles.open(stage_file_name, 'wb') as f:
                while True:
                    buffer = await reader.read(io.DEFAULT_BUFFER_SIZE)
                    if not buffer:
                        break
                    await fuzz()
                    await f.write(buffer)
                    size += len(buffer)

            if job.transfer.target.node_type == NodeType.ContainerNode:
                if job.transfer.view != View('ivo://ivoa.net/vospace/core#tar'):
                    return web.Response(status=400, text=f'Unsupported Container View. '
                                                         f'View: {job.transfer.view}')
                extract_dir = f'{self.staging_dir}/{target_id}/{path_tree}/'
                try:

                    loop = asyncio.get_event_loop()
                    root_node = await loop.run_in_executor(None, self.untar,
                                                           stage_file_name,
                                                           extract_dir,
                                                           job.transfer.target)
                    async with job.transaction() as tr:
                        node = tr.target
                        node.add_property(Property('ivo://ivoa.net/vospace/core#length', str(size)))
                        node.nodes = root_node.nodes
                        await asyncio.shield(node.save())
                        await asyncio.shield(copy(extract_dir, real_file_name))
                finally:
                    with suppress(Exception):
                        await asyncio.shield(rmtree(f'{self.staging_dir}/{target_id}'))
            else:
                async with job.transaction() as tr:
                    node = tr.target
                    node.add_property(Property('ivo://ivoa.net/vospace/core#length', str(size)))
                    await asyncio.shield(node.save())
                    await asyncio.shield(move(stage_file_name, real_file_name))

            return web.Response(status=200)
        except (asyncio.CancelledError, Exception):
            raise
        finally:
            with suppress(Exception):
                await asyncio.shield(remove(stage_file_name))
