from pyvospace.server.plugin import VOSpacePluginBase


TAR_VIEW = 'ivo://ivoa.net/vospace/core#tar'
BINARY_VIEW = 'ivo://ivoa.net/vospace/core#binaryview'


PROVIDES_PROTOCOLS = ['ivo://ivoa.net/vospace/core#httpput',
                      'ivo://ivoa.net/vospace/core#httpget']


def create(app):
    return PosixPlugin(app)


class PosixPlugin(VOSpacePluginBase):

    def __init__(self, app):
        super().__init__()
        self.app = app

        self.host = None
        self.port = None

    async def setup(self):
        config = self.app['config']
        self.port = config.getint('PosixPlugin', 'port')
        self.host = config.get('PosixPlugin', 'host')

    async def shutdown(self):
        pass

    def get_supported_import_views(self, node_type: str) -> list:
        if node_type == 'vos:ContainerNode':
            return [TAR_VIEW]
        else:
            return [BINARY_VIEW]

    def get_accepts_protocols(self) -> list:
        return []

    def get_provides_protocols(self) -> list:
        return PROVIDES_PROTOCOLS

    def get_protocol_endpoints(self,
                               uws_job_id: str,
                               target_path: str,
                               node_type: str,
                               direction: str,
                               protocol: str,
                               view: str,
                               params: list) -> dict:

        protocol_endpoints = {'protocol': protocol,
                              'endpoint': [f'http://{self.host}:{self.port}'
                                           f'/vospace/upload/{uws_job_id}'
                                           f'{target_path}']}

        return protocol_endpoints

