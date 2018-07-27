import json

from contextlib import suppress
from aiohttp_security import setup as setup_security
from aiohttp_security import SessionIdentityPolicy
from aiohttp_session import setup as setup_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from typing import List

from pyvospace.server.space import SpaceServer, AbstractSpace
from pyvospace.core.model import Views, View, Protocols, SecurityMethod, \
    Node, NodeTextLookup, NodeType, Properties, Property, HTTPGet, HTTPPut

from pyvospace.server.spaces.posix.utils import move, copy, mkdir, remove, rmtree, exists
from pyvospace.server.spaces.posix.auth import DBUserAuthentication, DBUserNodeAuthorizationPolicy


ACCEPTS_VIEWS = {
    'vos:Node': [View('ivo://ivoa.net/vospace/core#anyview')],
    'vos:DataNode': [View('ivo://ivoa.net/vospace/core#anyview')],
    'vos:UnstructuredDataNode': [View('ivo://ivoa.net/vospace/core#anyview')],
    'vos:StructuredDataNode': [View('ivo://ivoa.net/vospace/core#anyview')],
    'vos:ContainerNode': [],
    'vos:LinkNode': []
}

PROVIDES_VIEWS = {
    'vos:Node': [View('ivo://ivoa.net/vospace/core#defaultview')],
    'vos:DataNode': [View('ivo://ivoa.net/vospace/core#defaultview')],
    'vos:UnstructuredDataNode': [View('ivo://ivoa.net/vospace/core#defaultview')],
    'vos:StructuredDataNode': [View('ivo://ivoa.net/vospace/core#defaultview')],
    'vos:ContainerNode': [],
    'vos:LinkNode': []
}


class PosixSpaceServer(SpaceServer, AbstractSpace):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(cfg_file, *args, **kwargs)

        self.secret_key = self.config['Space']['secret_key']
        self.domain = self.config['Space']['domain']
        self.name = self.config['Storage']['name']
        self.storage_parameters = json.loads(self.config['Storage']['parameters'])

        self.root_dir = self.storage_parameters['root_dir']
        if not self.root_dir:
            raise Exception('root_dir not found.')

        self.staging_dir = self.storage_parameters['staging_dir']
        if not self.staging_dir:
            raise Exception('staging_dir not found.')

        self.authentication = None

    async def setup_space(self):
        await super().setup(self)

        await mkdir(self.root_dir)
        await mkdir(self.staging_dir)

        setup_session(self,
                      EncryptedCookieStorage(
                          secret_key=self.secret_key.encode(),
                          cookie_name='PYVOSPACE_COOKIE',
                          domain=self.domain))

        self.authentication = DBUserAuthentication(self['space_name'], self['db_pool'])
        setup_security(self,
                       SessionIdentityPolicy(),
                       DBUserNodeAuthorizationPolicy(self['space_name'], self['db_pool']))

        self.router.add_route('POST', '/login', self.authentication.login, name='login')
        self.router.add_route('POST', '/logout', self.authentication.logout, name='logout')

    async def shutdown(self):
        await super().shutdown()

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):
        app = PosixSpaceServer(cfg_file, *args, **kwargs)
        await app.setup_space()
        return app

    def get_properties(self) -> Properties:
        accepts = [Property('ivo://ivoa.net/vospace/core#title', None),
                   Property('ivo://ivoa.net/vospace/core#creator', None),
                   Property('ivo://ivoa.net/vospace/core#subject', None),
                   Property('ivo://ivoa.net/vospace/core#description', None),
                   Property('ivo://ivoa.net/vospace/core#publisher', None),
                   Property('ivo://ivoa.net/vospace/core#contributor', None),
                   Property('ivo://ivoa.net/vospace/core#date', None)]
        provides = []
        return Properties(accepts, provides)

    def get_protocols(self) -> Protocols:
        security_method = SecurityMethod('ivo://ivoa.net/sso#cookie')
        return Protocols(accepts=[], provides=[HTTPGet(security_method=security_method),
                                               HTTPPut(security_method=security_method)])

    def get_views(self) -> Views:
        return Views(accepts=[View('ivo://ivoa.net/vospace/core#anyview')],
                     provides=[View('ivo://ivoa.net/vospace/core#defaultview')])

    def get_accept_views(self, node: Node) -> List[View]:
        return ACCEPTS_VIEWS[NodeTextLookup[node.node_type]]

    def get_provide_views(self, node: Node) -> List[View]:
        return PROVIDES_VIEWS[NodeTextLookup[node.node_type]]

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
