
class VOSpacePluginBase(object):

    async def setup(self):
        raise NotImplementedError()

    async def shutdown(self):
        raise NotImplementedError()

    def get_accepts_protocols(self) -> list:
        raise NotImplementedError()

    def get_provides_protocols(self) -> list:
        raise NotImplementedError()

    def get_supported_import_views(self, node_type: str) -> list:
        raise NotImplementedError()

    def get_protocol_endpoints(self,
                               target_path: str,
                               node_type: str,
                               direction: str,
                               protocol: str,
                               view: str,
                               params: list) -> dict:
        raise NotImplementedError()