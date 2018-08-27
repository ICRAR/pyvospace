#!/usr/bin/env python


import json
import errno
import time
import requests
from http import client

from pyvospace.core.model import *

from fuse import FUSE, FuseOSError, Operations
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse


class VOSpaceFS(Operations):
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.conn = {}
        self.session = requests.session()
        url = f"http://{host}:{port}/login"
        with self.session.post(url, auth=HTTPBasicAuth(username, password), verify=True) as r:
            r.raise_for_status()

        cookie_str = self.session.cookies['PYVOSPACE_COOKIE']
        self.cookie_str = f'PYVOSPACE_COOKIE={cookie_str}'

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        #print('access', path)
        return 0

    def chmod(self, path, mode):
        #print('chmod', path)
        raise FuseOSError(errno.EACCES)

    def chown(self, path, uid, gid):
        #print('chown', path)
        raise FuseOSError(errno.EACCES)

    def getattr(self, path, fh=None):
        url = f'http://{self.host}:{self.port}/vospace/nodes/{path}'
        with self.session.get(url, params={'detail': 'max'}) as r:
            if r.status_code == 404:
                raise FuseOSError(errno.ENOENT)
            r.raise_for_status()
            node = Node.fromstring(r.text)

        node_props = node.properties
        attr = node_props.get('ivo://icrar.org/vospace/core#getattr')
        if not attr:
            raise FuseOSError(errno.ENOENT)
        return json.loads(attr.value)

    def readdir(self, path, fh):
        try:
            url = f'http://{self.host}:{self.port}/vospace/nodes/{path}'
            with self.session.get(url, params={'detail': 'max'}) as r:
                r.raise_for_status()
                node = Node.fromstring(r.text)
            if not isinstance(node, ContainerNode):
                raise FuseOSError(errno.EACCES)
            for n in node.nodes:
                yield os.path.basename(n.path)
        except Exception as e:
            raise FuseOSError(errno.EBUSY)

    def readlink(self, path):
        raise FuseOSError(errno.EACCES)

    def mknod(self, path, mode, dev):
        raise FuseOSError(errno.EACCES)

    def rmdir(self, path):
        url = f'http://{self.host}:{self.port}/vospace/nodes/{path}'
        with self.session.delete(url) as r:
            r.raise_for_status()

    def mkdir(self, path, mode):
        #print('mkdir', path)
        node = ContainerNode(path)
        url = f'http://{self.host}:{self.port}/vospace/nodes/{node.path}'
        with self.session.put(url, data=node.tostring()) as r:
            r.raise_for_status()

    def statfs(self, path):
        url = f'http://{self.host}:{self.port}/vospace/nodes/{path}'
        with self.session.get(url, params={'detail': 'max'}) as r:
            r.raise_for_status()
            node = Node.fromstring(r.text)

        node_props = node.properties
        attr = node_props.get('ivo://icrar.org/vospace/core#statfs')
        if not attr:
            raise FuseOSError(errno.EIO)
        return json.loads(attr.value)

    def unlink(self, path):
        #print('unlink', path)
        url = f'http://{self.host}:{self.port}/vospace/nodes/{path}'
        with self.session.delete(url) as r:
            r.raise_for_status()

    def symlink(self, name, target):
        raise FuseOSError(errno.EACCES)

    def rename(self, old, new):
        new_name = new[:-len(os.path.basename(old))]
        #print('rename', old, new_name, new)

        mv = Move(Node(old), Node(new_name))
        url = f'http://{self.host}:{self.port}/vospace/transfers'
        with self.session.post(url, data=mv.tostring()) as r:
            #print('TRANSFER', r.status_code)
            r.raise_for_status()
            job = UWSJob.fromstring(r.text)

        url = f'http://{self.host}:{self.port}/vospace/transfers/{job.job_id}/phase'
        with self.session.post(url, data='PHASE=RUN') as r:
            #print('PHASE', r.status_code)
            r.raise_for_status()

        poll_until = ('COMPLETED', 'ERROR')
        while True:
            with self.session.get(url) as r:
                r.raise_for_status()
                result = r.text
            #print('POLL', result)
            if result in poll_until:
                break
            time.sleep(0.05)

    def link(self, target, name):
        raise FuseOSError(errno.EACCES)

    def utimens(self, path, times=None):
        raise FuseOSError(errno.EACCES)

    # File methods
    # ============

    def open(self, path, flags):
        #print('open', path, flags)

        node = DataNode(path)
        if flags == os.O_RDONLY:
            transfer = PullFromSpace(node, [HTTPGet()])
        elif flags == os.O_WRONLY:
            transfer = PushToSpace(node, [HTTPPut()])
        else:
            raise FuseOSError(errno.EACCES)

        try:
            url = f'http://{self.host}:{self.port}/vospace/synctrans'
            with self.session.post(url, data=transfer.tostring()) as resp:
                resp.raise_for_status()
                response = Transfer.fromstring(resp.text)
                url = response.protocols[0].endpoint.url
        except:
            raise FuseOSError(errno.EIO)

        conn = None
        try:
            method = 'PUT'
            pr = urlparse(url)
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
                if response.status != 200:
                    raise FuseOSError(errno.EIO)
        except:
            if conn:
                conn.close()
            raise FuseOSError(errno.EIO)

        self.conn[conn.sock.fileno()] = (conn, transfer, response)
        return conn.sock.fileno()

    def create(self, path, mode, fi=None):
        #print('create', path, mode)
        return self.open(path, os.O_WRONLY)

    def read(self, path, length, offset, fh):
        #print('read', path, fh)
        conn = self.conn.get(fh)
        return conn[2].read(length)

    def write(self, path, buf, offset, fh):
        #print('write', path, fh)
        conn = self.conn.get(fh)
        chunk = f'{len(buf):X}\r\n'.encode('ascii') + buf + b'\r\n'
        conn[0].sock.sendall(chunk)
        return len(buf)

    def release(self, path, fh):
        #print('close')
        conn = self.conn.get(fh)
        if not conn:
            return
        if isinstance(conn[1], PushToSpace):
            try:
                conn[0].send(b'0\r\n\r\n')
                conn[0].getresponse()
            except:
                pass
        del self.conn[fh]
        conn[0].close()


def main():
    FUSE(VOSpaceFS('localhost', 8080, 'test', 'test'), '/tmp/vospace', nothreads=True, foreground=True)


if __name__ == '__main__':
    main()
