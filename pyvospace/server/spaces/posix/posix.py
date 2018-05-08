from pyvospace.server.vospace import VOSpaceServer


class PosixServer(VOSpaceServer):
    def __init__(self, cfg_file, *args, **kwargs):
        super().__init__(cfg_file, *args, **kwargs)

    async def _setup(self):
        await super()._setup()

    async def shutdown(self):
        await super().shutdown()

    @classmethod
    async def create(cls, cfg_file, *args, **kwargs):
        app = PosixServer(cfg_file, *args, **kwargs)
        await app._setup()
        return app

