
class VOSpaceError(Exception):

    def __init__(self, code, error):
        self.code = code
        self.error = error