import asyncpg
import configparser

from aiohttp import web

from .exception import VOSpaceError
from .node import create_node
from .uws import UWSJobExecutor, create_uws_job, get_uws_job, PhaseLookup
from .transfer import do_transfer

class VOSpaceServer(web.Application):

    def __init__(self, db_pool, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.db_pool = db_pool
        self.executor = UWSJobExecutor()

        self.router.add_put('/vospace/nodes/{name:.*}',
                            self.create_node)
        self.router.add_delete('/vospace/nodes/{name:.*}',
                               self.delete_node)
        self.router.add_post('/vospace/transfers',
                             self.transfer_node)
        self.router.add_get('/vospace/transfers/{job_id}',
                            self.get_transfer_job_phase)
        self.router.add_post('/vospace/transfers/{job_id}/phase',
                             self.transfer_node_job_phase)

    @classmethod
    async def create(cls, config_file, *args, **kwargs):
        config = configparser.ConfigParser()
        config.read(config_file)

        dsn = config['Database']['dsn']
        db_pool = await asyncpg.create_pool(dsn=dsn)

        return VOSpaceServer(db_pool, *args, **kwargs)

    async def create_node(self, request):
        try:
            xml_text = await request.text()
            url_path = request.path.replace('/vospace/nodes', '')

            xml_response = await create_node(self.db_pool,
                                             xml_text,
                                             url_path)

            return web.Response(status=201,
                                content_type='text/xml',
                                text=xml_response)

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)

        except Exception as g:
            return web.Response(status=500, text=str(g))

    async def delete_node(self, request):
        try:
            url_path = request.path.replace('/vospace/nodes', '')
            path_array = list(filter(None, url_path.split('/')))
            path_tree = '.'.join(path_array)

            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    move_tree_result = await conn.fetch("select * from nodes "
                                                        "where path <@ $1",
                                                        path_tree)

                    print(move_tree_result)

            return web.Response(status=200)

        except Exception as e:
            return web.Response(status=500)

    async def transfer_node(self, request):
        try:
            xml_text = await request.text()

            xml_response = await create_uws_job(self.db_pool,
                                                xml_text)

            return web.Response(status=200,
                                content_type='text/xml',
                                text=xml_response)

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            print(e)
            return web.Response(status=500)

    async def get_transfer_job_phase(self, request):
        try:
            job_id = request.match_info.get('job_id', None)

            job = await get_uws_job(self.db_pool, job_id)

            return web.Response(status=200, text=PhaseLookup[job['phase']])

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            import traceback
            traceback.print_exc()
            return web.Response(status=500)

    async def transfer_node_job_phase(self, request):
        try:
            job_id = request.match_info.get('job_id', None)
            uws_cmd = await request.text()

            job = await get_uws_job(self.db_pool, job_id)

            if not uws_cmd:
                return web.Response(status=200, text=PhaseLookup[job['phase']])

            if uws_cmd.upper() != "PHASE=RUN":
                raise VOSpaceError(400, f"Invalid Request. "
                                        f"Unknown UWS phase input {uws_cmd}")

            if job['phase'] > 0:
                raise VOSpaceError(400, f"Invalid Request. "
                                        f"Job not PENDING, can not be RUN.")

            await self.executor.execute(do_transfer, self.db_pool, job)

            return web.HTTPSeeOther(location=f'/vospace/transfers/{job_id}')

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            import traceback
            traceback.print_exc()
            return web.Response(status=500)
