import asyncpg
import asyncio
import json
import configparser

from aiohttp import web

from .exception import VOSpaceError
from .transfer import data_request
from .uws import UWSJobExecutor
from .space import register_storage


class StorageServer(web.Application):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config = configparser.ConfigParser()
        config.read(cfg_file)
        self['config'] = config

        self.router.add_put('/vospace/pushToVoSpace/{job_id}', self.upload_data)
        self.router.add_get('/vospace/pullFromVoSpace/{job_id}', self.download_data)

        self.on_shutdown.append(self.shutdown)

    async def shutdown(self):
        await self['executor'].close()
        await self['db_pool'].close()

    async def _setup(self):
        config = self['config']

        self['space_host'] = config['Space']['host']
        self['space_port'] = int(config['Space']['port'])
        self['space_name'] = config['Space']['name']
        self['space_uri'] = config['Space']['uri']
        self['space_parameters'] = json.loads(config['Space']['parameters'])

        space_name = self['space_name']
        self['host'] = config[space_name]['host']
        self['port'] = int(config[space_name]['port'])
        self['direction'] = json.loads(config[space_name]['direction'])
        self['db_pool'] = await asyncpg.create_pool(dsn=config['Space']['dsn'])

        space_id = await register_storage(self['db_pool'],
                                          space_name,
                                          self['host'],
                                          self['port'],
                                          json.dumps(self['direction']))

        self['space_id'] = space_id
        self['executor'] = UWSJobExecutor()

    async def download(self, app, conn, request, job_details):
        raise NotImplementedError()

    async def upload(self, app, conn, request, job_details):
        raise NotImplementedError()

    async def upload_data(self, request):
        try:
            return await data_request(self, request, self.upload)

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)
        except asyncio.CancelledError:
            return web.Response(status=500, text="Cancelled")
        except Exception as g:
            return web.Response(status=500, text=str(g))

    async def download_data(self, request):
        try:
            return await data_request(self, request, self.download)

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)
        except asyncio.CancelledError:
            return web.Response(status=500, text="Cancelled")
        except Exception as g:
            return web.Response(status=500, text=str(g))