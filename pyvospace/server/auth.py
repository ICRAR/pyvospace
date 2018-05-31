from abc import ABCMeta


class SpacePermission(metaclass=ABCMeta):
    async def permits(self, identity, permission, context):
        raise NotImplementedError()