import json
import configparser
import base64

from typing import List

from aiohttp_security import setup as setup_security
from aiohttp_security import SessionIdentityPolicy
from aiohttp_session import setup as setup_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from cryptography import fernet

from pyvospace.server.space import SpaceServer
from pyvospace.server.node import NodeType
from pyvospace.server.space import AbstractSpace
from pyvospace.core.model import Property, Node

from .utils import move, copy, mkdir, remove, rmtree, exists
from .auth import DBUserAuthentication, DBUserNodeAuthorizationPolicy


class PosixSpace(AbstractSpace):
    def __init__(self, cfg_file):
        super().__init__()

        config = configparser.ConfigParser()
        config.read(cfg_file)

        self.storage_parameters = json.loads(config['Storage']['parameters'])

        self.root_dir = self.storage_parameters['root_dir']
        if not self.root_dir:
            raise Exception('root_dir not found.')

        self.staging_dir = self.storage_parameters['staging_dir']
        if not self.staging_dir:
            raise Exception('staging_dir not found.')

    async def setup(self):
        await mkdir(self.root_dir)
        await mkdir(self.staging_dir)

    async def shutdown(self):
        pass

    async def move_storage_node(self, src_type, src_path, dest_type, dest_path):
        s_path = f"{self.root_dir}/{src_path}"
        d_path = f"{self.root_dir}/{dest_path}"
        await move(s_path, d_path)

    async def copy_storage_node(self, src_type, src_path, dest_type, dest_path):
        s_path = f"{self.root_dir}/{src_path}"
        d_path = f"{self.root_dir}/{dest_path}"
        await copy(s_path, d_path)

    async def create_storage_node(self, parent_node: Node, node: Node) -> List[Property]:
        print(parent_node, node)
        if node.node_type == NodeType.ContainerNode:
            m_path = f"{self.root_dir}/{node.path}"
            await mkdir(m_path)
        return []

    async def delete_storage_node(self, node_type, node_path):
        m_path = f"{self.root_dir}/{node_path}"
        if node_type == NodeType.ContainerNode:
            # The directory should always exists unless
            # it has been deleted under us.
            await rmtree(m_path)
        else:
            # File may or may not exist as the user may not have upload
            if await exists(m_path):
                await remove(m_path)

    async def filter_storage_endpoints(self, storage_list, node_type, node_path, protocol, direction):
        return storage_list


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
