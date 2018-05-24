import json
import configparser
import base64
import asyncpg

from contextlib import suppress
from aiohttp_security import setup as setup_security
from aiohttp_security import SessionIdentityPolicy
from aiohttp_session import setup as setup_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from cryptography import fernet

from pyvospace.server.space import SpaceServer
from pyvospace.server.space import AbstractSpace
from pyvospace.core.model import *

from .utils import move, copy, mkdir, remove, rmtree, exists
from .auth import DBUserAuthentication, DBUserNodeAuthorizationPolicy


class PosixSpace(AbstractSpace):
    def __init__(self, cfg_file):
        super().__init__()
        self.config = configparser.ConfigParser()
        self.config.read(cfg_file)
        self.name = self.config['Storage']['name']
        self.storage_parameters = json.loads(self.config['Storage']['parameters'])

        self.root_dir = self.storage_parameters['root_dir']
        if not self.root_dir:
            raise Exception('root_dir not found.')

        self.staging_dir = self.storage_parameters['staging_dir']
        if not self.staging_dir:
            raise Exception('staging_dir not found.')

        self.db_pool = None

    async def setup(self):
        self.db_pool = await asyncpg.create_pool(dsn=self.config['Space']['dsn'])
        await mkdir(self.root_dir)
        await mkdir(self.staging_dir)

    async def shutdown(self):
        pass

    async def move_storage_node(self, src, dest):
        s_path = f"{self.root_dir}/{src.path}"
        d_path = f"{self.root_dir}/{dest.path}"
        await move(s_path, d_path)

    async def copy_storage_node(self, src, dest):
        s_path = f"{self.root_dir}/{src.path}"
        d_path = f"{self.root_dir}/{dest.path}"
        await copy(s_path, d_path)

    async def create_storage_node(self, node: Node):
        m_path = f"{self.root_dir}/{node.path}"
        # Remove what may be left over from a failed xfer.
        # The only way this could happen if the storage server was
        # shutdown forcibly during an upload leaving a partial file.
        with suppress(Exception):
            await remove(m_path)
        if node.node_type == NodeType.ContainerNode:
            await mkdir(m_path)

    async def delete_storage_node(self, node):
        m_path = f"{self.root_dir}/{node.path}"
        if node.node_type == NodeType.ContainerNode:
            # The directory should always exists unless
            # it has been deleted under us.
            await rmtree(m_path)
        else:
            # File may or may not exist as the user may not have upload
            if await exists(m_path):
                await remove(m_path)

    async def set_protocol_transfer(self, job):
        new_protocols = []
        protocols = job.job_info.protocols
        if isinstance(job.job_info, PushToSpace):
            if any(i in [HTTPPut(), HTTPSPut()] for i in protocols) is False:
                raise VOSpaceError(400, "Protocol Not Supported.")

            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    results = await conn.fetch("select * from storage where name=$1", self.name)

            if HTTPPut() in protocols:
                for row in results:
                    if row['https'] is False:
                        endpoint = Endpoint(f'http://{row["host"]}:{row["port"]}/'
                                            f'vospace/{job.job_info.direction}/{job.job_id}')
                        new_protocols.append(HTTPPut(endpoint))

            if HTTPSPut() in protocols:
                for row in results:
                    if row['https'] is True:
                        endpoint = Endpoint(f'https://{row["host"]}:{row["port"]}/'
                                            f'vospace/{job.job_info.direction}/{job.job_id}')
                        new_protocols.append(HTTPPut(endpoint))

        elif isinstance(job.job_info, PullFromSpace):
            if any(i in [HTTPGet(), HTTPSGet()] for i in protocols) is False:
                raise VOSpaceError(400, "Protocol Not Supported.")

            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    results = await conn.fetch("select * from storage where name=$1", self.name)

            if HTTPGet() in protocols:
                for row in results:
                    if row['https'] is False:
                        endpoint = Endpoint(f'http://{row["host"]}:{row["port"]}/'
                                            f'vospace/{job.job_info.direction}/{job.job_id}')
                        new_protocols.append(HTTPPut(endpoint))

            if HTTPSGet() in protocols:
                for row in results:
                    if row['https'] is True:
                        endpoint = Endpoint(f'https://{row["host"]}:{row["port"]}/'
                                            f'vospace/{job.job_info.direction}/{job.job_id}')
                        new_protocols.append(HTTPPut(endpoint))

        if not new_protocols:
            raise VOSpaceError(400, "Protocol Not Supported. No storage found")

        job.transfer.set_protocols(new_protocols)


class PosixSpaceServer(SpaceServer):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(cfg_file, *args, **kwargs)
        self.space = PosixSpace(cfg_file)
        self.authentication = None

    async def setup(self):
        await super().setup(self.space)
        await self.space.setup()

        # secret_key must be 32 url-safe base64-encoded bytes
        fernet_key = fernet.Fernet.generate_key()
        secret_key = base64.urlsafe_b64decode(fernet_key)

        setup_session(self,
                      EncryptedCookieStorage(
                          secret_key=secret_key,
                          cookie_name='PYVOSPACE_COOKIE',
                          domain='localhost'))

        self.authentication = DBUserAuthentication(self['space_name'], self['db_pool'])

        setup_security(self,
                       SessionIdentityPolicy(),
                       DBUserNodeAuthorizationPolicy(self['space_name'], self['db_pool']))

        self.router.add_route('POST', '/login', self.authentication.login, name='login')
        self.router.add_route('GET', '/logout', self.authentication.logout, name='logout')

    async def shutdown(self):
        await super().shutdown()
        await self.space.shutdown()

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):
        app = PosixSpaceServer(cfg_file, *args, **kwargs)
        await app.setup()
        return app
