#!/usr/bin/env python
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

import json
import stat
import errno
import time
import requests
import argparse

from pyvospace.core.model import *

from http import client
from fuse import FUSE, FuseOSError, Operations
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse


class VOSpaceFS(Operations):
    def __init__(self, host, port, username, password, mountpoint, ssl):
        self.host = host
        self.port = port
        self.conn = {}
        self.mountpoint = mountpoint
        self.ssl = True if ssl == 1 else False
        self.session = requests.session()
        self.uid = os.getuid()
        self.gid = os.getgid()
        url = f"{self._ssl_url()}://{host}:{port}/login"
        with self.session.post(url, auth=HTTPBasicAuth(username, password), verify=True) as r:
            r.raise_for_status()
        cookie_str = self.session.cookies['PYVOSPACE_COOKIE']
        self.cookie_str = f'PYVOSPACE_COOKIE={cookie_str}'

    def _ssl_url(self):
        return "https" if self.ssl else "http"

    # Filesystem methods

    def access(self, path, mode):
        return 0

    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass

    def getattr(self, path, fh=None):
        try:
            url = f'{self._ssl_url()}://{self.host}:{self.port}/vospace/nodes/{path}'
            with self.session.get(url, params={'detail': 'max'}) as r:
                if r.status_code == 403:
                    raise FuseOSError(errno.EACCES)
                if r.status_code == 404:
                    raise FuseOSError(errno.ENOENT)
                r.raise_for_status()
                node = Node.fromstring(r.text)

            node_props = node.properties
            length = node_props.get('ivo://ivoa.net/vospace/core#length')
            ctime = node_props.get('ivo://ivoa.net/vospace/core#ctime')
            mtime = node_props.get('ivo://ivoa.net/vospace/core#mtime')

            if node.node_type == NodeType.ContainerNode:
                link = 2
                mode = 0o755 | stat.S_IFDIR
            else:
                link = 1
                mode = 0o644 | stat.S_IFREG

            a = {'st_atime': float(mtime.value), 'st_ctime': float(ctime.value),
                 'st_gid': self.gid, 'st_mode': mode,
                 'st_mtime': float(mtime.value), 'st_nlink': link,
                 'st_size': int(length.value), 'st_uid': self.uid}
            return a
        except FuseOSError:
            raise
        except:
            raise FuseOSError(errno.EIO)

    def readdir(self, path, fh):
        try:
            url = f'{self._ssl_url()}://{self.host}:{self.port}/vospace/nodes/{path}'
            with self.session.get(url, params={'detail': 'max'}) as r:
                r.raise_for_status()
                node = Node.fromstring(r.text)
            if not isinstance(node, ContainerNode):
                raise FuseOSError(errno.EACCES)
            for n in node.nodes:
                yield os.path.basename(n.path)
        except FuseOSError:
            raise
        except:
            raise FuseOSError(errno.EBUSY)

    def readlink(self, path):
        raise FuseOSError(errno.EACCES)

    def mknod(self, path, mode, dev):
        raise FuseOSError(errno.EACCES)

    def rmdir(self, path):
        try:
            url = f'{self._ssl_url()}://{self.host}:{self.port}/vospace/nodes/{path}'
            with self.session.delete(url) as r:
                if r.status_code == 403:
                    raise FuseOSError(errno.EACCES)
                r.raise_for_status()
        except FuseOSError:
            raise
        except:
            raise FuseOSError(errno.EIO)

    def mkdir(self, path, mode):
        try:
            node = ContainerNode(path)
            url = f'{self._ssl_url()}://{self.host}:{self.port}/vospace/nodes/{node.path}'
            with self.session.put(url, data=node.tostring()) as r:
                if r.status_code == 403:
                    raise FuseOSError(errno.EACCES)
                r.raise_for_status()
        except FuseOSError:
            raise
        except:
            raise FuseOSError(errno.EIO)

    def statfs(self, path):
        try:
            url = f'{self._ssl_url()}://{self.host}:{self.port}/vospace/nodes/{path}'
            with self.session.get(url, params={'detail': 'max'}) as r:
                r.raise_for_status()
                node = Node.fromstring(r.text)

            node_props = node.properties
            attr = node_props.get('ivo://icrar.org/vospace/core#statfs')
            if not attr:
                raise FuseOSError(errno.EIO)
            return json.loads(attr.value)
        except FuseOSError:
            raise
        except:
            raise FuseOSError(errno.EIO)

    def unlink(self, path):
        try:
            url = f'{self._ssl_url()}://{self.host}:{self.port}/vospace/nodes/{path}'
            with self.session.delete(url) as r:
                if r.status_code == 403:
                    raise FuseOSError(errno.EACCES)
                r.raise_for_status()
        except FuseOSError:
            raise
        except:
            raise FuseOSError(errno.EIO)

    def symlink(self, name, target):
        raise FuseOSError(errno.EACCES)

    def rename(self, old, new):
        try:
            mv = Move(Node(old), Node(new))
            url = f'{self._ssl_url()}://{self.host}:{self.port}/vospace/transfers'
            with self.session.post(url, data=mv.tostring()) as r:
                r.raise_for_status()
                job = UWSJob.fromstring(r.text)

            url = f'{self._ssl_url()}://{self.host}:{self.port}/vospace/transfers/{job.job_id}/phase'
            with self.session.post(url, data='PHASE=RUN') as r:
                r.raise_for_status()

            poll_until = ('COMPLETED', 'ERROR')
            while True:
                with self.session.get(url) as r:
                    r.raise_for_status()
                    result = r.text
                if result in poll_until:
                    if result == 'ERROR':
                        raise FuseOSError(errno.EACCES)
                    break
                time.sleep(0.05)
        except FuseOSError:
            raise
        except:
            raise FuseOSError(errno.EIO)

    def link(self, target, name):
        raise FuseOSError(errno.EACCES)

    def utimens(self, path, times=None):
        pass

    # File methods

    def open(self, path, flags):
        node = DataNode(path)
        if flags == os.O_RDONLY:
            transfer = PullFromSpace(node, [HTTPSGet() if self.ssl else HTTPGet()])
        elif flags == os.O_WRONLY:
            transfer = PushToSpace(node, [HTTPSPut() if self.ssl else HTTPPut()])
        elif flags == 32768:
            transfer = PullFromSpace(node, [HTTPSGet() if self.ssl else HTTPGet()])
        elif flags == 32769:
            transfer = PushToSpace(node, [HTTPSPut() if self.ssl else HTTPPut()])
        else:
            raise FuseOSError(errno.EACCES)

        try:
            url = f'{self._ssl_url()}://{self.host}:{self.port}/vospace/synctrans'
            with self.session.post(url, data=transfer.tostring()) as resp:
                if resp.status_code == 403:
                    raise FuseOSError(errno.EACCES)
                resp.raise_for_status()
                response = Transfer.fromstring(resp.text)
                url = response.protocols[0].endpoint.url
        except FuseOSError:
            raise
        except:
            raise FuseOSError(errno.EIO)

        conn = None
        try:
            method = 'PUT'
            pr = urlparse(url)
            if self.ssl:
                conn = client.HTTPSConnection(pr.netloc)
            else:
                conn = client.HTTPConnection(pr.netloc)
            if isinstance(transfer, PullFromSpace):
                method = 'GET'
            conn.putrequest(method, pr.path)
            conn.putheader('Cookie', self.cookie_str)
            if isinstance(transfer, PushToSpace):
                conn.putheader('Content-Type', 'application/octet-stream')
                conn.putheader('Transfer-Encoding', 'chunked')
            conn.endheaders()

            response = None
            if isinstance(transfer, PullFromSpace):
                response = conn.getresponse()
                if response.status == 403:
                    raise FuseOSError(errno.EACCES)
                if response.status != 200:
                    raise FuseOSError(errno.EIO)
        except FuseOSError:
            if conn:
                conn.close()
            raise
        except:
            if conn:
                conn.close()
            raise FuseOSError(errno.EIO)

        self.conn[conn.sock.fileno()] = (conn, transfer, response)
        return conn.sock.fileno()

    def create(self, path, mode, fi=None):
        return self.open(path, os.O_WRONLY)

    def read(self, path, length, offset, fh):
        conn = self.conn.get(fh)
        return conn[2].read(length)

    def write(self, path, buf, offset, fh):
        conn = self.conn.get(fh)
        chunked = []
        chunked.append(f'{len(buf):X}\r\n'.encode('ascii'))
        chunked.append(buf)
        chunked.append(b'\r\n')
        conn[0].sock.sendall(b''.join(chunked))
        return len(buf)

    def release(self, path, fh):
        conn = self.conn.get(fh)
        if not conn:
            return
        if isinstance(conn[1], PushToSpace):
            try:
                conn[0].send(b'0\r\n\r\n')
                conn[0].getresponse()
            except Exception as e:
                pass
        del self.conn[fh]
        conn[0].close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str)
    parser.add_argument("--port", type=int)
    parser.add_argument("--username", type=str)
    parser.add_argument("--password", type=str)
    parser.add_argument("--mountpoint", type=str)
    parser.add_argument("--usessl", type=int, default=0)
    args = parser.parse_args()

    space = VOSpaceFS(args.host, args.port, args.username, args.password, args.mountpoint, args.usessl)
    FUSE(space, args.mountpoint, nothreads=True, foreground=True, allow_other=True)

if __name__ == '__main__':
    main()
