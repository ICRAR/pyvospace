import asyncpg
import configparser
import json
import asyncio

from aiohttp import web
from contextlib import suppress
from abc import ABCMeta, abstractmethod

from pyvospace.core.exception import *
from pyvospace.core.model import *

from .view import create_node_request, delete_node_request, \
    get_node_request, set_node_properties_request
from .uws import UWSJobPool, UWSPhase, PhaseLookup
from .transfer import modify_transfer_job_phase, perform_transfer_job
from .database import NodeDatabase


class AbstractSpace(metaclass=ABCMeta):
    @abstractmethod
    def get_protocols(self) -> Protocols:
        raise NotImplementedError()

    @abstractmethod
    async def move_storage_node(self, src: Node, dest: Node):
        raise NotImplementedError()

    @abstractmethod
    async def copy_storage_node(self, src: Node, dest: Node):
        raise NotImplementedError()

    @abstractmethod
    async def create_storage_node(self, node: Node):
        raise NotImplementedError()

    @abstractmethod
    async def delete_storage_node(self, node: Node):
        raise NotImplementedError()

    @abstractmethod
    async def set_protocol_transfer(self, job: UWSJob):
        raise NotImplementedError()


class SpaceServer(web.Application):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config = configparser.ConfigParser()
        config.read(cfg_file)
        self.config = config

        self.router.add_get('/vospace/protocols', self._get_protocols)
        self.router.add_get('/vospace/nodes/{name:.*}', self._get_node)
        self.router.add_put('/vospace/nodes/{name:.*}', self._create_node)
        self.router.add_post('/vospace/nodes/{name:.*}', self._set_node_properties)
        self.router.add_delete('/vospace/nodes/{name:.*}', self._delete_node)
        self.router.add_post('/vospace/transfers', self._create_transfer)
        self.router.add_post('/vospace/synctrans', self._sync_transfer_node)
        self.router.add_get('/vospace/transfers/{job_id}', self._get_complete_transfer_job)
        self.router.add_post('/vospace/transfers/{job_id}/phase', self._change_transfer_job_phase)
        self.router.add_get('/vospace/transfers/{job_id}/phase', self._get_transfer_job_phase)
        self.router.add_get('/vospace/transfers/{job_id}/error', self._get_complete_transfer_job)
        self.router.add_get('/vospace/transfers/{job_id}/results/transferDetails', self._transfer_details)

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
        self['executor'] = UWSJobPool(space_id, db_pool)
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
            protocols = self['abstract_space'].get_protocols()
            return web.Response(status=200, content_type='text/xml', text=protocols.tostring())

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)
        except Exception as g:
            return web.Response(status=500, text=str(g))

    async def _set_node_properties(self, request):
        try:
            with suppress(asyncio.CancelledError):
                node = await asyncio.shield(set_node_properties_request(request))
            return web.Response(status=200, content_type='text/xml', text=node.tostring())

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)
        except Exception as g:
            return web.Response(status=500, text=str(g))

    async def _get_node(self, request):
        try:
            node = await get_node_request(request)
            return web.Response(status=200, content_type='text/xml', text=node.tostring())

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)
        except Exception as g:
            return web.Response(status=500, text=str(g))

    async def _create_node(self, request):
        try:
            with suppress(asyncio.CancelledError):
                node = await asyncio.shield(create_node_request(request))
            return web.Response(status=201, content_type='text/xml', text=node.tostring())

        except VOSpaceError as e:
            return web.Response(status=e.code, text=e.error)
        except Exception as g:
            return web.Response(status=500, text=str(g))

    async def _delete_node(self, request):
        try:
            with suppress(asyncio.CancelledError):
                await asyncio.shield(delete_node_request(self, request))
            return web.Response(status=204)

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)
        except Exception as e:
            return web.Response(status=500)

    async def _sync_transfer_node(self, request):
        try:
            job_xml = await request.text()
            transfer = Transfer.fromstring(job_xml)
            with suppress(asyncio.CancelledError):
                job = await asyncio.shield(self['executor'].create(transfer, UWSPhase.Executing))
            with suppress(asyncio.CancelledError):
                await asyncio.shield(perform_transfer_job(job, self, sync=True))
            return web.HTTPSeeOther(location=f'/vospace/transfers/{job.job_id}'
                                             f'/results/transferDetails')

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)
        except Exception as e:
            return web.Response(status=500)

    async def _create_transfer(self, request):
        try:
            job_xml = await request.text()
            transfer = Transfer.fromstring(job_xml)
            with suppress(asyncio.CancelledError):
                job = await asyncio.shield(self['executor'].create(transfer, UWSPhase.Pending))
            return web.HTTPSeeOther(location=f'/vospace/transfers/{job.job_id}')

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)
        except Exception as e:
            return web.Response(status=500)

    async def _get_complete_transfer_job(self, request):
        try:
            job_id = request.match_info.get('job_id', None)
            job = await self['executor'].get(job_id)
            xml = job.tostring()
            return web.Response(status=200, content_type='text/xml', text=xml)

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)
        except Exception:
            return web.Response(status=500)

    async def _transfer_details(self, request):
        try:
            job_id = request.match_info.get('job_id', None)
            job = await self['executor'].get_uws_job(job_id)
            if job['phase'] < UWSPhase.Executing:
                raise InvalidJobStateError('Job not EXECUTING')
            if not job['transfer']:
                raise VOSpaceError(400, 'No transferDetails for this job.')
            return web.Response(status=200, content_type='text/xml', text=job['transfer'])

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)
        except Exception:
            return web.Response(status=500)

    async def _get_transfer_job_phase(self, request):
        try:
            job_id = request.match_info.get('job_id', None)
            job = await self['executor'].get_uws_job_phase(job_id)
            return web.Response(status=200, text=PhaseLookup[job['phase']])

        except VOSpaceError as f:
            return web.Response(status=f.code, text=f.error)
        except Exception:
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
        except Exception:
            return web.Response(status=500)