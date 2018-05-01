
class VOSpacePluginBase(object):

    async def setup(self):
        raise NotImplementedError()

    async def shutdown(self):
        raise NotImplementedError()

    def get_space_name(self):
        raise NotImplementedError()

    def get_accepts_protocols(self) -> list:
        raise NotImplementedError()

    def get_supported_import_provides_protocols(self) -> list:
        raise NotImplementedError()

    def get_supported_export_provides_protocols(self) -> list:
        raise NotImplementedError()

    def get_supported_import_accepts_views(self, node_path: str,
                                           node_type: str) -> list:
        raise NotImplementedError()

    def get_supported_export_provides_views(self, node_path: str,
                                            node_type: str) -> list:
        raise NotImplementedError()

    def get_protocol_endpoints(self,
                               uws_job_id: str,
                               target: str,
                               target_type: str,
                               direction: str,
                               protocol: str,
                               view: str,
                               params: list) -> dict:
        raise NotImplementedError()