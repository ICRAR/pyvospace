import asyncpg
import configparser

from aiohttp import web

from pyvospace.server.exception import VOSpaceError

from .views import upload_to_node, make_dir


class PosixFileServer(web.Application):

    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config = configparser.ConfigParser()
        config.read(cfg_file)
        self['config'] = config

        self.router.add_put('/vospace/upload/{job_id}',
                            self.upload_data)

        self.on_shutdown.append(self.shutdown)

    async def shutdown(self):
        await self['db_pool'].close()

    async def setup(self):
        config = self['config']

        self['root_dir'] = config['PosixPlugin']['root_dir']
        if not self['root_dir']:
            raise Exception('root_dir not found.')

        await make_dir(self['root_dir'])

        dsn = config['Database']['dsn']
        db_pool = await asyncpg.create_pool(dsn=dsn)
        self['db_pool'] = db_pool

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):
        app = PosixFileServer(cfg_file, *args, **kwargs)
        await app.setup()
        return app

    async def upload_data(self, request):
        try:
            await upload_to_node(self, request)

            return web.Response(status=200)

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)

        except Exception as g:
            import traceback
            traceback.print_exc()
            return web.Response(status=500, text=str(g))

