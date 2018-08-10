class VOSpaceError(Exception):
    def __init__(self, code, error):
        self.code = code
        self.error = error

    def __str__(self):
        return self.error


class InvalidXML(VOSpaceError):
    def __init__(self, error):
        error = f"Invalid URI. {error}"
        super().__init__(400, error)


class InvalidURI(VOSpaceError):
    def __init__(self, error):
        error = f"Invalid URI. {error}"
        super().__init__(400, error)


class InvalidArgument(VOSpaceError):
    def __init__(self, error):
        super().__init__(400, error)


class NodeDoesNotExistError(VOSpaceError):
    def __init__(self, error):
        error = f"Node Not Found. {error}"
        super().__init__(404, error)


class DuplicateNodeError(VOSpaceError):
    def __init__(self, error):
        error = f"Duplicate Node. {error}"
        super().__init__(409, error)


class ContainerDoesNotExistError(VOSpaceError):
    def __init__(self, error):
        error = f"Container Not Found. {error}"
        super().__init__(404, error)


class JobDoesNotExistError(VOSpaceError):
    def __init__(self, error):
        super().__init__(404, error)


class InvalidJobError(VOSpaceError):
    def __init__(self, error):
        super().__init__(400, error)


class InvalidJobStateError(VOSpaceError):
    def __init__(self, error):
        super().__init__(400, error)


class NodeBusyError(VOSpaceError):
    def __init__(self, error):
        error = f"Node Busy. {error}"
        super().__init__(400, error)


class ClosingError(VOSpaceError):
    def __init__(self):
        super().__init__(400, '')


class PermissionDenied(VOSpaceError):
    def __init__(self, error):
        super().__init__(403, error)