from pyvospace.server.plugin import VOSpacePluginBase
from pyvospace.server.exception import VOSpaceError

def create():
    return PosixStorage()


PROTOCOLS = ['ivo://ivoa.net/vospace/core#httpput']

class PosixStorage(VOSpacePluginBase):

    def __init__(self):
        super().__init__()

    async def setup(self, config):
        pass

    async def shutdown(self):
        pass

    async def get_import_views(self):
        pass

    async def get_export_views(self):
        pass

    async def get_supported_protocols(self):
        return list(PROTOCOLS)

    async def get_protocol_end_points(self, protocols, path):

        for prot in protocols:
            if prot not in PROTOCOLS:
                raise VOSpaceError(400, f"Protocol Not Supported. {prot}")





