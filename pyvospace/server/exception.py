class VOSpaceError(Exception):
    def __init__(self, code, error):
        self.code = code
        self.error = error


class NodeDoesNotExistError(VOSpaceError):
    def __init__(self, error):
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
        super().__init__(400, error)