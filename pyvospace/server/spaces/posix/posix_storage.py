import io
import os
import asyncio
import aiofiles
import configparser
import json
import uuid
import base64

from aiohttp import web
from aiohttp_security import setup as setup_security
from aiohttp_security import SessionIdentityPolicy
from aiohttp_session import setup as setup_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from cryptography import fernet

from pyvospace.server.view import NodeType
from pyvospace.server.spaces.posix.utils import mkdir, remove, send_file, move
from pyvospace.core.exception import *
from pyvospace.server.storage import SpaceStorage

from .auth import DBUserAuthentication, DBUserNodeAuthorizationPolicy


class _PosixStorageServer(web.Application):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = configparser.ConfigParser()
        self.config.read(cfg_file)


class PosixStorageServer(SpaceStorage, _PosixStorageServer):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(cfg_file, *args, **kwargs)

        self.name = self.config.get('Storage', 'name')
        self.host = self.config.get('Storage', 'host')
        self.https = self.config.getboolean('Storage', 'https', fallback=False)
        self.port = self.config.getint('Storage', 'port')
        self.parameters = json.loads(self.config.get('Storage', 'parameters'))
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
        await super(PosixStorageServer, self).shutdown()

    async def setup(self):
        await super(PosixStorageServer, self).setup()

        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.fetchrow("insert into storage (name, host, port, parameters, https) "
                                    "values ($1, $2, $3, $4, $5) on conflict (name, host, port) "
                                    "do update set parameters=$4, https=$5",
                                    self.name, self.host, self.port, json.dumps(self.parameters), self.https)

        await mkdir(self.root_dir)
        await mkdir(self.staging_dir)

        setup_session(self,
                      EncryptedCookieStorage(
                          secret_key=self.secret_key.encode(),
                          cookie_name='PYVOSPACE_COOKIE',
                          domain=self.domain))

        self.authentication = DBUserAuthentication(self.name, self.db_pool)

        setup_security(self,
                       SessionIdentityPolicy(),
                       DBUserNodeAuthorizationPolicy(self.name, self.db_pool))

        self.router.add_put('/vospace/{direction}/{job_id}', self.upload_request)
        self.router.add_get('/vospace/{direction}/{job_id}', self.download_request)

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):
        app = PosixStorageServer(cfg_file, *args, **kwargs)
        await app.setup()
        return app

    async def upload_request(self, request):
        job_id = request.match_info.get('job_id', None)
        return await self.execute(request, job_id, self.upload)

    async def download_request(self, request):
        job_id = request.match_info.get('job_id', None)
        return await self.execute(request, job_id, self.download)

    async def download(self, job, request):
        root_dir = self.root_dir
        path_tree = job.transfer.target.path
        file_path = f'{root_dir}/{path_tree}'
        return await send_file(request, os.path.splitext(path_tree)[0], file_path)

    async def upload(self, job, request):
        reader = request.content
        # This implementation wont accept container node data
        if job.transfer.target.node_type == NodeType.ContainerNode:
            return web.Response(status=400, text='Unable to upload data to a container.')

        path_tree = job.job_info.target.path
        real_file_name = f'{self.root_dir}/{path_tree}'
        stage_file_name = f'{self.staging_dir}/{uuid.uuid1()}'

        try:
            async with aiofiles.open(stage_file_name, 'wb') as f:
                while True:
                    buffer = await reader.read(io.DEFAULT_BUFFER_SIZE)
                    if not buffer:
                        break
                    await f.write(buffer)

            await asyncio.shield(move(stage_file_name, real_file_name))
            return web.Response(status=200)
        except (asyncio.CancelledError, Exception):
            await remove(stage_file_name)
            raise
