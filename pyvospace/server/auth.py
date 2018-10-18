from abc import ABCMeta


class SpacePermission(metaclass=ABCMeta):
    async def permits(self, identity: object, permission: str, context: object):
        """
        Permission on VOSpace actions.

        :param identity: user object (type defined by implementation)
        :param permission:

            createNode: called on a 6.2.1 createNode request.
            context: tuple(:func:`parent Node<pyvospace.core.model.Node>`, :func:`Node <pyvospace.core.model.Node>`)

            getNode: called on a 6.3.1 setNode request.
            context: :func:`Node <pyvospace.core.model.Node>`

            setNode: called on a 6.3.2 setNode request.
            context: :func:`Node <pyvospace.core.model.Node>`

            moveNode: called on a 6.2.2 moveNode request.
            context: tuple(:func:`src Node <pyvospace.core.model.Node>`, :func:`dest Node <pyvospace.core.model.Node>`)

            copyNode: called on a 6.2.3 copyNode request.
            context: tuple(:func:`src Node <pyvospace.core.model.Node>`, :func:`dest Node <pyvospace.core.model.Node>`)

            deleteNode: called on a 6.2.4 deleteNode request.
            context: :func:`Node <pyvospace.core.model.Node>`

            createTransfer: called when a UWS transfer job is to be created (Push, Pull, Move, Copy)
            context: :func:`Transfer <pyvospace.core.model.Transfer>`

            dataTransfer: called when a UWS push or pull data transfer job is to be created.
            context: :func:`UWSJob <pyvospace.core.model.UWSJob>`

            runJob: called when a UWS job is to be run.
            context: :func:`UWSJob <pyvospace.core.model.UWSJob>`

            abortJob: called when a UWS job is to be aborted.
            context: :func:`UWSJob <pyvospace.core.model.UWSJob>`

        :return: True if action can be performed on behalf of the user; False otherwise.
        """
        raise NotImplementedError()