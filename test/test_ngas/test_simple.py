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

import unittest
import asyncio
import logging
import hashlib
import io
import filecmp
import pdb

import aiofiles
from aiohttp import web

from pyvospace.core.model import *
from pyvospace.server import set_fuzz, set_busy_fuzz
from pyvospace.server.spaces.ngas.storage.ngas_storage import NGASStorageServer
from test_base import TestBase

class TestPushPull(TestBase):

    def setUp(self):

        logger=logging.getLogger("aiohttp.web")
        logger.setLevel(logging.DEBUG)

        super().setUp()
        self.loop.run_until_complete(self._setup())
        ngas_server = self.loop.run_until_complete(NGASStorageServer.create(self.config_filename, logger=logger))

        self.ngas_runner = web.AppRunner(ngas_server)
        self.loop.run_until_complete(self.ngas_runner.setup())
        self.ngas_site = web.TCPSite(self.ngas_runner, 'localhost', 8083)
        self.loop.run_until_complete(self.ngas_site.start())

    async def _setup(self):
        if not os.path.exists('/tmp/download'):
            os.makedirs('/tmp/download')
        await self.create_file('/tmp/datafile.dat')
        await self.create_tar('/tmp/mytar.tar.gz')

    def tearDown(self):
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/root'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/datanode'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/syncdatanode'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/syncdatanode1.fits'))
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/root/mytar.tar.gz'))
        self.loop.run_until_complete(self.ngas_runner.shutdown())
        self.loop.run_until_complete(self.ngas_runner.cleanup())
        super().tearDown()

    def test_push_pull_simple_chunked(self):
        async def run():
            root_node = ContainerNode('/root')
            await self.create_node(root_node)

            # Make a file with content in it
            test_file="/tmp/datafile.dat"
            test_basename=os.path.basename(test_file)

            test_bytes=1234
            with open(test_file,"wb") as fd:
                fd.write(os.urandom(test_bytes))

            node = DataNode('/root/datafile.dat',
                properties=[Property('ivo://ivoa.net/vospace/core#title', test_basename, True),
                            Property('ivo://ivoa.net/vospace/core#contributor', "dave", True)])
            await self.create_node(node)

            # Push to leaf node
            push = PushToSpace(node, [HTTPPut()], params=[Parameter("ivo://ivoa.net/vospace/core#length", test_bytes)])
            #
            transfer = await self.sync_transfer_node(push)
            put_end = transfer.protocols[0].endpoint.url
            await self.push_to_space(put_end, test_file, expected_status=200)

            # # Pull from leaf node
            pull = PullFromSpace(node, [HTTPGet()])
            transfer = await self.sync_transfer_node(pull)
            pull_end = transfer.protocols[0].endpoint.url
            await self.pull_from_space(pull_end, '/tmp/download/')

            # Do it again
            transfer = await self.sync_transfer_node(push)
            put_end = transfer.protocols[0].endpoint.url
            await self.push_to_space(put_end, test_file, expected_status=200)

            # # Pull from leaf node
            pull = PullFromSpace(node, [HTTPGet()])
            transfer = await self.sync_transfer_node(pull)
            pull_end = transfer.protocols[0].endpoint.url
            await self.pull_from_space(pull_end, '/tmp/download/')
            
            # Compare the two files
            result=filecmp.cmp(test_file, '/tmp/download/'+test_basename)

            self.assertEqual(result, True, msg="Downloaded file not the same as uploaded file")

        self.loop.run_until_complete(run())


    def test_push_pull_simple_with_content_length(self):
        async def run():
            root_node = ContainerNode('/root')
            await self.create_node(root_node)

            # Make a file with content in it
            test_file = "/tmp/datafile.dat"
            test_basename = os.path.basename(test_file)

            test_bytes = 1234
            with open(test_file, "wb") as fd:
                fd.write(os.urandom(test_bytes))

            node = DataNode('/root/datafile.dat',
                            properties=[Property('ivo://ivoa.net/vospace/core#title', test_basename, True),
                                        Property('ivo://ivoa.net/vospace/core#contributor', "dave", True)])
            await self.create_node(node)

            # Push to leaf node
            push = PushToSpace(node, [HTTPPut()],
                               params=[Parameter("ivo://ivoa.net/vospace/core#length", test_bytes)])
            #
            transfer = await self.sync_transfer_node(push)
            put_end = transfer.protocols[0].endpoint.url
            await self.push_to_space_with_content_length(put_end, test_file, expected_status=200)

            # # Pull from leaf node
            pull = PullFromSpace(node, [HTTPGet()])
            transfer = await self.sync_transfer_node(pull)
            pull_end = transfer.protocols[0].endpoint.url
            await self.pull_from_space(pull_end, '/tmp/download/')

            # Compare the two files
            result = filecmp.cmp(test_file, '/tmp/download/' + test_basename)

            self.assertEqual(result, True, msg="Downloaded file not the same as uploaded file")

        self.loop.run_until_complete(run())

    def test_push_to_container(self):
        async def run():

            root_node = ContainerNode('/root')
            await self.create_node(root_node)

            node = DataNode('/root/mytar.tar.gz',
                            properties=[Property('ivo://ivoa.net/vospace/core#title', "mytar.tar.gz", True)])
            await self.create_node(node)

            security_method = SecurityMethod('ivo://ivoa.net/sso#cookie')

            # push tar to node
            container_push = PushToSpace(node, [HTTPPut(security_method=security_method)],
                                         view=View('ivo://ivoa.net/vospace/core#tar'),
                                         params=[Parameter("ivo://ivoa.net/vospace/core#length", 1234)])

            transfer = await self.sync_transfer_node(container_push)
            put_end = transfer.protocols[0].endpoint.url
            await self.push_to_space(put_end, '/tmp/mytar.tar.gz', expected_status=200)

            # push to container node
            container_push = PushToSpace(root_node, [HTTPPut(security_method=security_method)],
                                         view=View('ivo://ivoa.net/vospace/core#tar'),
                                         params=[Parameter("ivo://ivoa.net/vospace/core#length", 1234)])

            #pdb.set_trace()

            transfer = await self.sync_transfer_node(container_push)
            put_end = transfer.protocols[0].endpoint.url
            await self.push_to_space(put_end, '/tmp/mytar.tar.gz', expected_status=200)

            # Do it again
            transfer = await self.sync_transfer_node(container_push)
            put_end = transfer.protocols[0].endpoint.url
            await self.push_to_space(put_end, '/tmp/mytar.tar.gz', expected_status=200)

            pull = PullFromSpace(root_node, [HTTPGet()], view=View('ivo://ivoa.net/vospace/core#tar'))
            transfer = await self.sync_transfer_node(pull)
            pull_end = transfer.protocols[0].endpoint.url
            await self.pull_from_space(pull_end, '/tmp/download/')

            pull = PullFromSpace(node, [HTTPGet()], view=View('ivo://ivoa.net/vospace/core#tar'))
            transfer = await self.sync_transfer_node(pull)
            pull_end = transfer.protocols[0].endpoint.url
            await self.pull_from_space(pull_end, '/tmp/download/')

        self.loop.run_until_complete(run())


if __name__ == '__main__':
    unittest.main()
