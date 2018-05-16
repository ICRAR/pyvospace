import io
import asyncio
import aiofiles
import configparser

from aiohttp import web

from pyvospace.server.node import NodeType
from pyvospace.server.storage import AbstractServerStorage
from pyvospace.server.spaces.posix.utils import mkdir, remove, send_file


class PosixStorage(AbstractServerStorage):
    def __init__(self, app, config):
        super().__init__(app, config)

        self.root_dir = self.parameters['root_dir']
        if not self.root_dir:
            raise Exception('root_dir not found.')

        self.staging_dir = self.parameters['staging_dir']
        if not self.staging_dir:
            raise Exception('staging_dir not found.')

    async def setup(self):
        await super().setup()
        await mkdir(self.root_dir)
        await mkdir(self.staging_dir)

    async def download(self, request):
        root_dir = self.root_dir
        path_tree = request.vo_job['path']
        file_path = f'{root_dir}/{path_tree}'
        return await send_file(request, request.vo_job['name'], file_path)

    async def upload(self, request):
        reader = request.content

        # This implementation wont accept container node data
        if request.vo_job['type'] == NodeType.ContainerNode:
            return web.Response(status=400, text='Unable to upload data to a container.')

        path_tree = request.vo_job['path']
        file_name = f'{self.root_dir}/{path_tree}'

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

        return web.Response(status=200)


class PosixStorageServer(web.Application):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config = configparser.ConfigParser()
        config.read(cfg_file)
        self.storage = PosixStorage(self, config)
        self.on_shutdown.append(self.shutdown)

    async def shutdown(self):
        await self.storage.shutdown()

    async def setup(self):
        await self.storage.setup()

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):
        app = PosixStorageServer(cfg_file, *args, **kwargs)
        await app.setup()
        return app
