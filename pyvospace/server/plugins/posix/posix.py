from pyvospace.server.plugin import VOSpacePluginBase


TAR_VIEW = ['ivo://ivoa.net/vospace/core#tar']
DEFAULT_VIEW = ['ivo://ivoa.net/vospace/core#defaultview']
ANY_VIEW = ['ivo://ivoa.net/vospace/core#anyview']

IMPORT_PROVIDES = ['ivo://ivoa.net/vospace/core#httpput']
EXPORT_PROVIDES = ['ivo://ivoa.net/vospace/core#httpget']

PROVIDES_PROTOCOLS = IMPORT_PROVIDES + EXPORT_PROVIDES


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

    def get_space_name(self):
        return 'icrar.org'

    def get_accepts_protocols(self) -> list:
        return []

    def get_supported_import_accepts_views(self, node_path: str,
                                           node_type: str) -> list:
        if node_type == 'vos:ContainerNode':
            return TAR_VIEW
        else:
            return ANY_VIEW

    def get_supported_export_provides_views(self, node_path: str,
                                            node_type: str) -> list:
        if node_type == 'vos:ContainerNode':
            return TAR_VIEW
        else:
            return DEFAULT_VIEW

    def get_supported_import_provides_protocols(self) -> list:
        return IMPORT_PROVIDES

    def get_supported_export_provides_protocols(self) -> list:
        return EXPORT_PROVIDES

    def get_protocol_endpoints(self,
                               uws_job_id: str,
                               target_path: str,
                               target_type: str,
                               direction: str,
                               protocol: str,
                               view: str,
                               params: list) -> dict:

        protocol_endpoints = {'protocol': protocol,
                              'endpoint': [f'http://{self.host}:{self.port}'
                                           f'/vospace/upload/{uws_job_id}']}
        return protocol_endpoints
