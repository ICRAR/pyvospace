import os
import asyncpg
import configparser

from aiohttp import web
from functools import partial
from pluginbase import PluginBase

from .exception import VOSpaceError
from .node import create_node_request, delete_node, get_node, set_node_properties
from .uws import UWSJobExecutor, create_uws_job, get_uws_job, \
    generate_uws_job_xml, PhaseLookup, UWSPhase
from .transfer import do_transfer
from .plugin import VOSpacePluginBase


class VOSpaceServer(web.Application):

    def __init__(self, db_pool, storage, plugin_source, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self['db_pool'] = db_pool
        self['executor'] = UWSJobExecutor()
        self['storage'] = storage
        self['plugin_source'] = plugin_source

        self.router.add_get('/vospace/nodes/{name:.*}',
                            self.get_node)
        self.router.add_put('/vospace/nodes/{name:.*}',
                            self.create_node)
        self.router.add_post('/vospace/nodes/{name:.*}',
                             self.set_node_properties)
        self.router.add_delete('/vospace/nodes/{name:.*}',
                               self.delete_node)
        self.router.add_post('/vospace/transfers',
                             self.transfer_node)
        self.router.add_get('/vospace/transfers/{job_id}',
                            self.get_transfer_job)
        self.router.add_post('/vospace/transfers/{job_id}/phase',
                             self.change_transfer_job_phase)
        self.router.add_get('/vospace/transfers/{job_id}/phase',
                             self.get_transfer_node_job_phase)
        self.router.add_get('/vospace/transfers/{job_id}/error',
                            self.get_transfer_job)

        self.on_shutdown.append(self.shutdown)

    async def shutdown(self):
        await self['executor'].close()
        await self['db_pool'].close()
        await self['storage'].shutdown()

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):

        config = configparser.ConfigParser()
        config.read(cfg_file)

        dsn = config['Database']['dsn']
        db_pool = await asyncpg.create_pool(dsn=dsn)

        plugin_path = config['StoragePlugin']['path']
        plugin_name = config['StoragePlugin']['name']

        # For easier usage calculate the path relative to here.
        here = os.path.abspath(os.path.dirname(__file__))
        get_path = partial(os.path.join, here)

        plugin_base = PluginBase(package='pyvospace.plugins')
        plugin_source = plugin_base.make_plugin_source(
            searchpath=[get_path('./plugins/'), plugin_path])

        found = False
        for plugin_source_name in plugin_source.list_plugins():
            if plugin_source_name == plugin_name:
                found = True
                break

        if found is False:
            raise VOSpaceError(500, f"Plugin: {plugin_name} not found.")

        plugin = plugin_source.load_plugin(plugin_name)
        storage = plugin.create()

        if not isinstance(storage, VOSpacePluginBase):
            raise ImportError(f"{repr(storage)} is not an "
                              f"instance of VOSpacePluginBase")

        await storage.setup(config=config)

        return VOSpaceServer(db_pool, storage, plugin_source, *args, **kwargs)

    async def set_node_properties(self, request):
        try:
            xml_text = await request.text()
            url_path = request.path.replace('/vospace/nodes', '')
            xml_response = await set_node_properties(self['db_pool'],
                                                     xml_text,
                                                     url_path)
            return web.Response(status=200,
                                content_type='text/xml',
                                text=xml_response)

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)

        except Exception as g:
            return web.Response(status=500, text=str(g))

    async def get_node(self, request):
        try:
            url_path = request.path.replace('/vospace/nodes', '')

            xml_response = await get_node(self['db_pool'],
                                          url_path,
                                          request.query)

            return web.Response(status=200,
                                content_type='text/xml',
                                text=xml_response)

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)

        except Exception as g:
            return web.Response(status=500, text=str(g))

    async def create_node(self, request):
        try:
            xml_text = await request.text()
            url_path = request.path.replace('/vospace/nodes', '')
            xml_response = await create_node_request(self['db_pool'],
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

            await delete_node(self['db_pool'], url_path)

            return web.Response(status=204)

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)

    async def transfer_node(self, request):
        try:
            xml_text = await request.text()
            id = await create_uws_job(self['db_pool'],
                                      xml_text)

            return web.HTTPSeeOther(location=f'/vospace/transfers/{id}')

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)

    async def get_transfer_job(self, request):
        try:
            job_id = request.match_info.get('job_id', None)

            job = await get_uws_job(self['db_pool'], job_id)

            xml = generate_uws_job_xml(job['id'],
                                       job['phase'],
                                       job['destruction'],
                                       job['job_info'],
                                       job['results'],
                                       job['error'])

            return web.Response(status=200,
                                content_type='text/xml',
                                text=xml)

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)

    async def get_transfer_node_job_phase(self, request):
        try:
            job_id = request.match_info.get('job_id', None)

            job = await get_uws_job(self['db_pool'], job_id)

            return web.Response(status=200, text=PhaseLookup[job['phase']])

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)

    async def change_transfer_job_phase(self, request):
        try:
            job_id = request.match_info.get('job_id', None)
            job = await get_uws_job(self['db_pool'], job_id)

            uws_cmd = await request.text()
            if not uws_cmd:
                raise VOSpaceError(400, f"Invalid Request. "
                                        f"Empty UWS phase input.")

            if uws_cmd.upper() != "PHASE=RUN":
                raise VOSpaceError(400, f"Invalid Request. "
                                        f"Unknown UWS phase input {uws_cmd}")

            if job['phase'] != UWSPhase.Pending:
                raise VOSpaceError(400, f"Invalid Request. "
                                        f"Job not PENDING, can not be RUN.")

            await self['executor'].execute(do_transfer, self['db_pool'], job)

            return web.HTTPSeeOther(location=f'/vospace/transfers/{job_id}')

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)
