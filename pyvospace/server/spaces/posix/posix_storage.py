import os
import io
import asyncio
import aiofiles

from aiohttp import web

from pyvospace.server.node import NodeType
from pyvospace.server.storage import StorageServer
from pyvospace.server.spaces.posix.utils import mkdir, remove, send_file


class PosixStorageServer(StorageServer):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(cfg_file, *args, **kwargs)

    async def shutdown(self):
        await super().shutdown()

    async def _setup(self):
        await super()._setup()

        self['root_dir'] = self['space_parameters']['root_dir']
        if not self['root_dir']:
            raise Exception('root_dir not found.')

        self['staging_dir'] = self['space_parameters']['staging_dir']
        if not self['staging_dir']:
            raise Exception('staging_dir not found.')

        await mkdir(self['root_dir'])
        await mkdir(self['staging_dir'])

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):
        app = PosixStorageServer(cfg_file, *args, **kwargs)
        await app._setup()
        return app

    async def download(self, app, conn, request, job_details):
        root_dir = app['root_dir']
        path_tree = job_details['path']
        file_path = f'{root_dir}/{path_tree}'
        return await send_file(request, job_details['name'], file_path)

    async def upload(self, app, conn, request, job_details):
        reader = request.content

        root_dir = app['root_dir']
        path_tree = job_details['path']

        file_name = f'{root_dir}/{path_tree}'
        directory = os.path.dirname(file_name)

        await mkdir(directory)

        if job_details['type'] == NodeType.ContainerNode:
            file_name = f'{root_dir}/{path_tree}/{uuid.uuid4().hex}.zip'

        try:
            async with aiofiles.open(file_name, 'wb') as f:
                while True:
                    buffer = await reader.read(io.DEFAULT_BUFFER_SIZE)
                    if not buffer:
                        break
                    await f.write(buffer)
        except asyncio.CancelledError:
            await remove(file_name)
            raise

        # if its a container (rar, zip etc) then
        # unpack it and create nodes if neccessary
        if job_details['type'] == NodeType.ContainerNode:
            pass

        return web.Response(status=200)
