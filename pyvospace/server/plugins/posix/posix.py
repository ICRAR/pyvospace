import asyncio
import configparser

from pyvospace.server.plugin import VOSpacePluginBase

from aiohttp import web

from .process import AsyncProcess

TAR_VIEW = 'ivo://ivoa.net/vospace/core#tar'
BINARY_VIEW = 'ivo://ivoa.net/vospace/core#binaryview'


PROVIDES_PROTOCOLS = ['ivo://ivoa.net/vospace/core#httpput',
                      'ivo://ivoa.net/vospace/core#httpget']



def create(app):
    return PosixPlugin(app)


class PosixFileServer(web.Application):

    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.router.add_get('/vospace/upload/{name:.*}',
                            self.upload_data)

        self.on_shutdown.append(self.shutdown)

    async def shutdown(self):
        pass

    @classmethod
    def start_server(cls, config, port, address):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        app = PosixFileServer(config)
        web.run_app(app,
                    host=address,
                    port=port,
                    reuse_port=True)

    async def upload_data(self, request):
        return web.Response(status=200,
                            text="hello")


class PosixPlugin(VOSpacePluginBase):

    def __init__(self, app):
        super().__init__()
        self.app = app
        self.server_process = None
        self.server_task = None
        self.port = 8090
        self.address = 'localhost'

    async def setup(self):
        config = self.app['config']

        try:
            self.port = config.getint('PosixFileServer', 'port')
        except configparser.NoSectionError:
            pass

        try:
            self.address = config.get('PosixFileServer', 'address')
        except configparser.NoSectionError:
            pass

        self.server_process = AsyncProcess(name='posix_file_server',
                                           target=PosixFileServer.start_server,
                                           args=(config,
                                                 self.port,
                                                 self.address))

        self.server_task = asyncio.ensure_future(self.server_process.apply())

    async def shutdown(self):
        await self.server_process.terminate()
        await self.server_process.join()

        if self.server_task:
            await self.server_task

    def get_supported_import_views(self, node_type: str) -> list:
        if node_type == 'vos:ContainerNode':
            return [TAR_VIEW]
        else:
            return [BINARY_VIEW]

    def get_accepts_protocols(self) -> list:
        return []

    def get_provides_protocols(self) -> list:
        return PROVIDES_PROTOCOLS

    def get_protocol_endpoints(self,
                               target_path: str,
                               node_type: str,
                               direction: str,
                               protocol: str,
                               view: str,
                               params: list) -> dict:

        protocol_endpoints = {'protocol': protocol,
                              'endpoint': [f'http://{self.address}:{self.port}'
                                           f'/vospace/upload{target_path}']}

        return protocol_endpoints

