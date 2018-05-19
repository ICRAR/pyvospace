import asyncpg
import configparser
import json

from aiohttp import web
from abc import ABCMeta, abstractmethod
from typing import List


from pyvospace.core.exception import VOSpaceError, InvalidJobStateError
from .node import _create_node_request, delete_node, _get_node_request, _set_node_properties_request
from .uws import UWSJobExecutor, get_uws_job, get_uws_job_phase, \
    generate_uws_job_xml, PhaseLookup, UWSPhase, create_uws_job
from .transfer import modify_transfer_job_phase, get_transfer_details, perform_transfer_job
from .database import NodeDatabase

from pyvospace.core.model import Property, Node


class AbstractSpace(metaclass=ABCMeta):

    @abstractmethod
    async def setup(self):
        raise NotImplementedError()

    @abstractmethod
    async def shutdown(self):
        raise NotImplementedError()

    @abstractmethod
    async def move_storage_node(self, src_type, src_path, dest_type, dest_path):
        raise NotImplementedError()

    @abstractmethod
    async def copy_storage_node(self, src_type, src_path, dest_type, dest_path):
        raise NotImplementedError()

    @abstractmethod
    async def create_storage_node(self, node: Node):
        raise NotImplementedError()

    @abstractmethod
    async def delete_storage_node(self, node_type, node_path):
        raise NotImplementedError()

    @abstractmethod
    async def filter_storage_endpoints(self, storage_list, node_type, node_path, protocol, direction):
        raise NotImplementedError()


class SpaceServer(web.Application):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config = configparser.ConfigParser()
        config.read(cfg_file)
        self.config = config

        self.router.add_get('/vospace/protocols',
                            self._get_protocols)
        self.router.add_get('/vospace/nodes/{name:.*}',
                            self._get_node)
        self.router.add_put('/vospace/nodes/{name:.*}',
                            self._create_node)
        self.router.add_post('/vospace/nodes/{name:.*}',
                             self._set_node_properties)
        self.router.add_delete('/vospace/nodes/{name:.*}',
                               self._delete_node)
        self.router.add_post('/vospace/transfers',
                             self._transfer_node)
        self.router.add_post('/vospace/synctrans',
                             self._sync_transfer_node)
        self.router.add_get('/vospace/transfers/{job_id}',
                            self._get_complete_transfer_job)
        self.router.add_post('/vospace/transfers/{job_id}/phase',
                             self._change_transfer_job_phase)
        self.router.add_get('/vospace/transfers/{job_id}/phase',
                             self._get_transfer_node_job_phase)
        self.router.add_get('/vospace/transfers/{job_id}/error',
                            self._get_complete_transfer_job)
        self.router.add_get('/vospace/transfers/{job_id}/results/transferDetails',
                            self._transfer_details)

        self.on_shutdown.append(self.shutdown)

    async def setup(self, abstract_space):
        self['abstract_space'] = abstract_space
        self['space_host'] = self.config['Space']['host']
        self['space_port'] = int(self.config['Space']['port'])
        self['space_name'] = self.config['Space']['name']
        self['uri'] = self.config['Space']['uri']
        self['parameters'] = json.loads(self.config['Space']['parameters'])
        self['accepts_views'] = json.loads(self.config['Space']['accepts_views'])
        self['provides_views'] = json.loads(self.config['Space']['provides_views'])
        self['accepts_protocols'] = json.loads(self.config['Space']['accepts_protocols'])
        self['provides_protocols'] = json.loads(self.config['Space']['provides_protocols'])
        self['readonly_properties'] = json.loads(self.config['Space']['readonly_properties'])
        db_pool = await asyncpg.create_pool(dsn=self.config['Space']['dsn'])

        space_id = await self._register_space(db_pool,
                                              self['space_name'],
                                              self['space_host'],
                                              self['space_port'],
                                              json.dumps(self['accepts_views']),
                                              json.dumps(self['provides_views']),
                                              json.dumps(self['accepts_protocols']),
                                              json.dumps(self['provides_protocols']),
                                              json.dumps(self['readonly_properties']),
                                              json.dumps(self['parameters']))

        self['db_pool'] = db_pool
        self['space_id'] = space_id
        self['executor'] = UWSJobExecutor()
        self['db'] = NodeDatabase(space_id, db_pool)

    async def _register_space(self, db_pool, name, host, port, accepts_views, provides_views,
                              accepts_protocols, provides_protocols, readonly_properties, parameters):
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.fetchrow("select * from space where host=$1 and port=$2 for update",
                                             host, port)
                if result:
                    # if there is an existing plugin associated with this space
                    # and its not the one specified then raise an error
                    # Don't want to infringe on another space and its data
                    if result['name'] != name:
                        raise VOSpaceError(400, 'Can not start space over an existing '
                                                'space on the same host and port.')

                result = await conn.fetchrow("insert into space (host, port, name, accepts_views, provides_views, "
                                             "accepts_protocols, provides_protocols, readonly_properties, parameters) "
                                             "values ($1, $2, $3, $4, $5, $6, $7, $8, $9) on conflict (host, port) "
                                             "do update set accepts_views=$4, provides_views=$5, accepts_protocols=$6, "
                                             "provides_protocols=$7, readonly_properties=$8, parameters=$9 "
                                             "returning id",
                                             host, port, name, accepts_views, provides_views,
                                             accepts_protocols, provides_protocols, readonly_properties, parameters)
                return int(result['id'])

    async def shutdown(self):
        await self['executor'].close()
        await self['db_pool'].close()

    async def _get_protocols(self, request):
        try:
            accepts = self['accepts_protocols']
            provides = self['provides_protocols']
            xml_response = generate_protocol_response(accepts, provides)
            return web.Response(status=200,
                                content_type='text/xml',
                                text=xml_response)

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)

        except Exception as g:
            return web.Response(status=500, text=str(g))

    async def _set_node_properties(self, request):
        try:
            node = await _set_node_properties_request(request)
            return web.Response(status=200, content_type='text/xml', text=node.tostring())

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)

        except Exception as g:
            #import traceback
            #traceback.print_exc()
            return web.Response(status=500, text=str(g))

    async def _get_node(self, request):
        try:
            node = await _get_node_request(request)

            #import xml.dom.minidom

            #xml = xml.dom.minidom.parseString(node.tostring())
            #print(xml.toprettyxml())

            return web.Response(status=200, content_type='text/xml', text=node.tostring())

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)

        except Exception as g:
            #import traceback
            #traceback.print_exc()
            return web.Response(status=500, text=str(g))

    async def _create_node(self, request):
        try:
            node = await _create_node_request(request)
            return web.Response(status=201, content_type='text/xml', text=node.tostring())

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)

        except Exception as g:
            #import traceback
            #traceback.print_exc()
            return web.Response(status=500, text=str(g))

    async def _delete_node(self, request):
        try:
            url_path = request.path.replace('/vospace/nodes', '')
            await delete_node(self, url_path)
            return web.Response(status=204)

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)

    async def _sync_transfer_node(self, request):
        try:
            job_xml = await request.text()
            space_job_id = await create_uws_job(self['db_pool'], self['space_id'],
                                                job_xml, UWSPhase.Executing)
            await perform_transfer_job(self, space_job_id, job_xml, sync=True)
            return web.HTTPSeeOther(location=f'/vospace/transfers/{space_job_id.job_id}'
                                             f'/results/transferDetails')

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)

    async def _transfer_node(self, request):
        try:
            xml_text = await request.text()
            space_job_id = await create_uws_job(self['db_pool'], self['space_id'],
                                                xml_text, UWSPhase.Pending)
            return web.HTTPSeeOther(location=f'/vospace/transfers/{space_job_id.job_id}')

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)

    async def _get_complete_transfer_job(self, request):
        try:
            job_id = request.match_info.get('job_id', None)
            job = await get_uws_job(self['db_pool'], self['space_id'], job_id)

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
            return web.Response(status=500)

    async def _transfer_details(self, request):
        try:
            job_id = request.match_info.get('job_id', None)
            xml = await get_transfer_details(self['db_pool'], self['space_id'], job_id)
            return web.Response(status=200,
                                content_type='text/xml',
                                text=xml)

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)

    async def _get_transfer_node_job_phase(self, request):
        try:
            job_id = request.match_info.get('job_id', None)
            job = await get_uws_job_phase(self['db_pool'], self['space_id'], job_id)
            return web.Response(status=200, text=PhaseLookup[job['phase']])

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)

    async def _change_transfer_job_phase(self, request):
        try:
            job_id = request.match_info.get('job_id', None)
            uws_cmd = await request.text()
            await modify_transfer_job_phase(self, job_id, uws_cmd)
            return web.HTTPSeeOther(location=f'/vospace/transfers/{job_id}')

        except InvalidJobStateError:
            return web.HTTPSeeOther(location=f'/vospace/transfers/{job_id}')

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)