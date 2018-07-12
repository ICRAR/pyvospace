import io
import os
import asyncio
import aiofiles

from aiohttp import web
from aiohttp_security import setup as setup_security
from aiohttp_security import SessionIdentityPolicy
from aiohttp_session import setup as setup_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage

from pyvospace.core.model import NodeType
from pyvospace.server.spaces.posix.utils import mkdir, remove, send_file, move
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

    async def download(self, job, request):
        root_dir = self.root_dir
        path_tree = job.transfer.target.path
        file_path = f'{root_dir}/{path_tree}'
        return await send_file(request, os.path.basename(path_tree), file_path)

    async def upload(self, job, request):
        reader = request.content
        # This implementation wont accept container node data
        if job.transfer.target.node_type == NodeType.ContainerNode:
            return web.Response(status=400, text='Unable to upload data to a container.')

        path_tree = job.job_info.target.path
        object_id = job.transfer.target.object_id
        base_name = f'{object_id}_{os.path.basename(path_tree)}'
        real_file_name = f'{self.root_dir}/{path_tree}'
        stage_file_name = f'{self.staging_dir}/{base_name}'
        try:
            async with aiofiles.open(stage_file_name, 'wb') as f:
                while True:
                    buffer = await reader.read(io.DEFAULT_BUFFER_SIZE)
                    if not buffer:
                        break
                    await fuzz()
                    await f.write(buffer)

            async with job.transaction() as tr:
                await asyncio.shield(move(stage_file_name, real_file_name))

            return web.Response(status=200)
        except (asyncio.CancelledError, Exception):
            await remove(stage_file_name)
            raise
