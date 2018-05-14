import asyncpg
import json
import configparser

from aiohttp import web
from collections import namedtuple

from .exception import VOSpaceError
from .transfer import data_transfer_request
from .uws import UWSJobExecutor


StorageRegister = namedtuple('StorageRegister', 'space_id storage_id')


async def register_storage(db_pool, name, host, port, parameters):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            space_result = await conn.fetchrow("select * from space where name=$1 for update", name)
            if not space_result:
                raise VOSpaceError(404, f'Space not found. Name: {name}')

            storage = await conn.fetchrow("insert into storage (name, host, port, parameters) "
                                          "values ($1, $2, $3, $4) on conflict (name, host, port) "
                                          "do update set parameters=$4 returning id",
                                          name, host, port, parameters)
            return StorageRegister(int(space_result['id']), int(storage['id']))


class SpaceStorageServer(web.Application):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config = configparser.ConfigParser()
        config.read(cfg_file)
        self['config'] = config

        self.router.add_put('/vospace/{direction}/{job_id}', self._upload_data)
        self.router.add_get('/vospace/{direction}/{job_id}', self._download_data)

        self.on_shutdown.append(self.shutdown)

    async def shutdown(self):
        await self['executor'].close()
        await self['db_pool'].close()

    async def _setup(self):
        config = self['config']

        self['storage_name'] = config['Storage']['name']
        self['storage_host'] = config['Storage']['host']
        self['storage_port'] = int(config['Storage']['port'])
        self['storage_parameters'] = json.loads(config['Storage']['parameters'])
        self['db_pool'] = await asyncpg.create_pool(dsn=config['Space']['dsn'])

        result = await register_storage(self['db_pool'],
                                        self['storage_name'],
                                        self['storage_host'],
                                        self['storage_port'],
                                        json.dumps(self['storage_parameters']))

        self['space_id'] = result.space_id
        self['storage_id'] = result.storage_id
        self['executor'] = UWSJobExecutor()

    async def download(self, request):
        raise NotImplementedError()

    async def upload(self, request):
        raise NotImplementedError()

    async def _upload_data(self, request):
        return await data_transfer_request(self, request, self.upload)

    async def _download_data(self, request):
        return await data_transfer_request(self, request, self.download)