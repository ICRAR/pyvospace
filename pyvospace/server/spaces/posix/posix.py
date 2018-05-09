import json

from pyvospace.server.vospace import VOSpaceServer
from pyvospace.server.node import NodeType

from .utils import move, copy, mkdir, isfile, remove, rmtree, exists


class PosixServer(VOSpaceServer):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(cfg_file, *args, **kwargs)

    async def _setup(self):
        await super()._setup()

        self['parameters'] = json.loads(self['config']['Space']['parameters'])

        self['root_dir'] = self['parameters']['root_dir']
        if not self['root_dir']:
            raise Exception('root_dir not found.')

        self['staging_dir'] = self['parameters']['staging_dir']
        if not self['staging_dir']:
            raise Exception('staging_dir not found.')

        await mkdir(self['root_dir'])
        await mkdir(self['staging_dir'])

    async def shutdown(self):
        await super().shutdown()

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):
        app = PosixServer(cfg_file, *args, **kwargs)
        await app._setup()
        return app

    async def move_storage_node(self, src_type, src_path, dest_type, dest_path):
        s_path = f"{self['root_dir']}/{src_path}"
        d_path = f"{self['root_dir']}/{dest_path}"
        await move(s_path, d_path)

    async def copy_storage_node(self, src_type, src_path, dest_type, dest_path):
        s_path = f"{self['root_dir']}/{src_path}"
        d_path = f"{self['root_dir']}/{dest_path}"
        await copy(s_path, d_path)

    async def create_storage_node(self, node_type, node_path):
        if node_type == NodeType.ContainerNode:
            m_path = f"{self['root_dir']}/{node_path}"
            await mkdir(m_path)

    async def delete_storage_node(self, node_type, node_path):
        m_path = f"{self['root_dir']}/{node_path}"
        if node_type == NodeType.ContainerNode:
            # The directory should always exists unless
            # it has been deleted under us.
            await rmtree(m_path)
        else:
            # File may or may not exist as the user may not have upload
            if await exists(m_path):
                await remove(m_path)
