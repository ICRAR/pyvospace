#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2018
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA

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