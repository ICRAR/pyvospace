import asyncpg
import configparser
import json

from aiohttp import web

from .exception import VOSpaceError, InvalidJobStateError
from .node import create_node_request, delete_node, get_node_request, set_node_properties, \
    generate_protocol_response, generate_node_response
from .uws import UWSJobExecutor, get_uws_job, get_uws_job_phase, \
    generate_uws_job_xml, PhaseLookup, UWSPhase
from .transfer import create_transfer_job, modify_transfer_job_phase, get_transfer_details
from .base import VOSpaceBase


async def register_space(db_pool, name, host, port, accepts_views, provides_views,
                         accepts_protocols, provides_protocols, parameters):
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
                                         "accepts_protocols, provides_protocols, parameters) "
                                         "values ($1, $2, $3, $4, $5, $6, $7, $8) on conflict (host, port) "
                                         "do update set accepts_views=$4, provides_views=$5, "
                                         "accepts_protocols=$6, provides_protocols=$7, parameters=$8 "
                                         "returning id",
                                         host, port, name, accepts_views, provides_views,
                                         accepts_protocols, provides_protocols, parameters)
            return int(result['id'])


class SpaceServer(web.Application, VOSpaceBase):

    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config = configparser.ConfigParser()
        config.read(cfg_file)
        self['config'] = config

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

    async def _setup(self):
        config = self['config']
        self['host'] = config['Space']['host']
        self['port'] = int(config['Space']['port'])
        self['name'] = config['Space']['name']
        self['uri'] = config['Space']['uri']
        self['parameters'] = json.loads(config['Space']['parameters'])
        self['accepts_views'] = json.loads(config['Space']['accepts_views'])
        self['provides_views'] = json.loads(config['Space']['provides_views'])
        self['accepts_protocols'] = json.loads(config['Space']['accepts_protocols'])
        self['provides_protocols'] = json.loads(config['Space']['provides_protocols'])
        self['db_pool'] = await asyncpg.create_pool(dsn=config['Space']['dsn'])

        space_id = await register_space(self['db_pool'],
                                        self['name'],
                                        self['host'],
                                        self['port'],
                                        json.dumps(self['accepts_views']),
                                        json.dumps(self['provides_views']),
                                        json.dumps(self['accepts_protocols']),
                                        json.dumps(self['provides_protocols']),
                                        json.dumps(self['parameters']))

        self['space_id'] = space_id
        self['executor'] = UWSJobExecutor()

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

    async def _get_node(self, request):
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

    async def _create_node(self, request):
        try:
            xml_text = await request.text()
            url_path = request.path.replace('/vospace/nodes', '')
            response = await create_node_request(self, xml_text, url_path)

            accepts_views = self['accepts_views'].get(response.node_type_text, [])

            xml_response = generate_node_response(space_name=self['uri'],
                                                  node_path=response.node_name,
                                                  node_type=response.node_type_text,
                                                  node_busy=response.node_busy,
                                                  node_property=response.node_properties,
                                                  node_accepts_views=accepts_views,
                                                  node_target=response.node_target)

            return web.Response(status=201,
                                content_type='text/xml',
                                text=xml_response)

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)

        except Exception as g:
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
            xml_text = await request.text()
            id = await create_transfer_job(self, xml_text, UWSPhase.Executing)
            return web.HTTPSeeOther(location=f'/vospace/transfers/{id}/results/transferDetails')

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)

        except Exception as e:
            return web.Response(status=500)

    async def _transfer_node(self, request):
        try:
            xml_text = await request.text()
            id = await create_transfer_job(self, xml_text)
            return web.HTTPSeeOther(location=f'/vospace/transfers/{id}')

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

    async def get_storage_endpoints(self, conn, space_id, job_id, protocol, direction):
        results = await conn.fetch("select storage.host, storage.port from storage "
                                   "inner join space on space.name=storage.name "
                                   "where space.id=$1", space_id)
        if not results:
            raise VOSpaceError(404, "No storage endpoints found.")

        endpoints = []
        for row in results:
            prot = 'http' if protocol.split('#')[1].startswith('http') else 'https'
            endpoints.append(f'{prot}://{row["host"]}:{row["port"]}/vospace/{direction}/{job_id}')
        return {'protocol': protocol, 'endpoints': endpoints}