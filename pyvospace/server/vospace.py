import os
import asyncpg
import configparser

from aiohttp import web
from functools import partial
from pluginbase import PluginBase

from .exception import VOSpaceError
from .node import create_node_request, delete_node, get_node_request, set_node_properties, \
    generate_protocol_response, generate_node_response
from .uws import UWSJobExecutor, get_uws_job, \
    generate_uws_job_xml, PhaseLookup, UWSPhase, InvalidUWSState
from .transfer import create_transfer_job, run_transfer_job, get_transfer_details
from .plugin import VOSpacePluginBase


class VOSpaceServer(web.Application):

    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config = configparser.ConfigParser()
        config.read(cfg_file)
        self['config'] = config

        self.router.add_get('/vospace/protocols',
                            self.get_protocols)
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
        self.router.add_post('/vospace/synctrans',
                             self.sync_transfer_node)
        self.router.add_get('/vospace/transfers/{job_id}',
                            self.get_complete_transfer_job)
        self.router.add_post('/vospace/transfers/{job_id}/phase',
                             self.change_transfer_job_phase)
        self.router.add_get('/vospace/transfers/{job_id}/phase',
                             self.get_transfer_node_job_phase)
        self.router.add_get('/vospace/transfers/{job_id}/error',
                            self.get_complete_transfer_job)
        self.router.add_get('/vospace/transfers/{job_id}/results/transferDetails',
                            self.transfer_details)

        self.on_shutdown.append(self.shutdown)

    async def setup(self):
        self['executor'] = UWSJobExecutor()

        config = self['config']

        dsn = config['Database']['dsn']
        db_pool = await asyncpg.create_pool(dsn=dsn)
        self['db_pool'] = db_pool

        plugin_path = config['Plugin']['path']
        plugin_name = config['Plugin']['name']

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
        plugin_obj = plugin.create(self)

        if not isinstance(plugin_obj, VOSpacePluginBase):
            raise ImportError(f"{repr(plugin_obj)} is not an "
                              f"instance of VOSpacePluginBase")
        # allow the plugin to set itself up before starting server
        await plugin_obj.setup()

        self['space_name'] = plugin_obj.get_space_name()
        self['plugin'] = plugin_obj
        self['plugin_source'] = plugin_source

    async def shutdown(self):
        await self['plugin'].shutdown()
        await self['executor'].close()
        await self['db_pool'].close()

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):
        app = VOSpaceServer(cfg_file, *args, **kwargs)
        await app.setup()
        return app

    async def get_protocols(self, request):
        try:
            accepts = self['plugin'].get_accepts_protocols()

            push_provides = self['plugin'].get_supported_import_provides_protocols()
            pull_provides = self['plugin'].get_supported_export_provides_protocols()
            provides = push_provides + pull_provides

            xml_response = generate_protocol_response(accepts, provides)
            return web.Response(status=200,
                                content_type='text/xml',
                                text=xml_response)

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)

        except Exception as g:
            return web.Response(status=500, text=str(g))

    async def set_node_properties(self, request):
        try:
            xml_text = await request.text()
            url_path = request.path.replace('/vospace/nodes', '')
            xml_response = await set_node_properties(self,
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
            xml_response = await get_node_request(self, url_path, request.query)
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
            response = await create_node_request(self, xml_text, url_path)

            accepts = self['plugin'].get_supported_import_accepts_views(response.node_name,
                                                                        response.node_type_text)

            xml_response = generate_node_response(space_name=self['space_name'],
                                                  node_path=response.node_name,
                                                  node_type=response.node_type_text,
                                                  node_property=response.node_properties,
                                                  node_accepts_views=accepts)

            return web.Response(status=201,
                                content_type='text/xml',
                                text=xml_response)

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)

        except Exception as g:
            #import traceback
            #traceback.print_exc()
            return web.Response(status=500, text=str(g))

    async def delete_node(self, request):
        try:
            url_path = request.path.replace('/vospace/nodes', '')
            await delete_node(self, url_path)
            return web.Response(status=204)

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)

    async def sync_transfer_node(self, request):
        try:
            xml_text = await request.text()
            id = await create_transfer_job(self, xml_text, UWSPhase.Executing)
            return web.HTTPSeeOther(location=f'/vospace/transfers/{id}/results/transferDetails')

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)

    async def transfer_node(self, request):
        try:
            xml_text = await request.text()
            id = await create_transfer_job(self, xml_text)
            return web.HTTPSeeOther(location=f'/vospace/transfers/{id}')

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            import traceback
            traceback.print_exc()
            return web.Response(status=500)

    async def get_complete_transfer_job(self, request):
        try:
            job_id = request.match_info.get('job_id', None)
            job = await get_uws_job(self['db_pool'], job_id)

            xml = generate_uws_job_xml(job['id'],
                                       job['phase'],
                                       job['destruction'],
                                       job['job_info'],
                                       job['result'],
                                       job['error'])

            return web.Response(status=200,
                                content_type='text/xml',
                                text=xml)

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            import traceback
            traceback.print_exc()
            return web.Response(status=500)

    async def transfer_details(self, request):
        try:
            job_id = request.match_info.get('job_id', None)
            xml = await get_transfer_details(self, job_id)
            return web.Response(status=200,
                                content_type='text/xml',
                                text=xml)

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            import traceback
            traceback.print_exc()
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
            uws_cmd = await request.text()
            await run_transfer_job(self, job_id, uws_cmd)
            return web.HTTPSeeOther(location=f'/vospace/transfers/{job_id}')

        except InvalidUWSState:
            return web.HTTPSeeOther(location=f'/vospace/transfers/{job_id}')

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)
