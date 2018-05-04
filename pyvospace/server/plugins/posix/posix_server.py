import asyncpg
import asyncio
import configparser

from aiohttp import web

from pyvospace.server.exception import VOSpaceError
from pyvospace.server.transfer import data_request
from pyvospace.server.uws import UWSJobExecutor

from .views import upload, download, make_dir


class PosixFileServer(web.Application):

    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config = configparser.ConfigParser()
        config.read(cfg_file)
        self['config'] = config

        self.router.add_put('/vospace/upload/{job_id}', self.upload_data)
        self.router.add_get('/vospace/download/{job_id}', self.download_data)

        self.on_shutdown.append(self.shutdown)

    async def shutdown(self):
        await self['executor'].close()
        await self['db_pool'].close()

    async def setup(self):
        config = self['config']

        self['root_dir'] = config['PosixPlugin']['root_dir']
        if not self['root_dir']:
            raise Exception('root_dir not found.')

        self['processing_dir'] = config['PosixPlugin']['processing_dir']
        if not self['processing_dir']:
            raise Exception('processing_dir not found.')

        await make_dir(self['root_dir'])
        await make_dir(self['processing_dir'])

        dsn = config['Database']['dsn']
        db_pool = await asyncpg.create_pool(dsn=dsn)
        self['db_pool'] = db_pool

        self['executor'] = UWSJobExecutor()

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):
        app = PosixFileServer(cfg_file, *args, **kwargs)
        await app.setup()
        return app

    async def upload_data(self, request):
        try:
            return await data_request(self, request, upload)

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)
        except asyncio.CancelledError:
            raise
        except Exception as g:
            return web.Response(status=500, text=str(g))

    async def download_data(self, request):
        try:
            return await data_request(self, request, download)

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)
        except asyncio.CancelledError:
            raise
        except Exception as g:
            return web.Response(status=500, text=str(g))