import unittest
import asyncio

from aiohttp import web, client_exceptions

from pyvospace.core.model import *
from pyvospace.server import set_fuzz
from pyvospace.server.spaces.posix.posix_storage import PosixStorageServer
from test.test_base import TestBase


class TestPushPull(TestBase):

    def setUp(self):
        super().setUp()
        self.loop.run_until_complete(self._setup())
        posix_server = self.loop.run_until_complete(PosixStorageServer.create(self.config_filename))

        self.posix_runner = web.AppRunner(posix_server)
        self.loop.run_until_complete(self.posix_runner.setup())
        self.posix_site = web.TCPSite(self.posix_runner, 'localhost', 8081)
        self.loop.run_until_complete(self.posix_site.start())

    async def _setup(self):
        await self.create_file('/tmp/datafile.dat')

    def tearDown(self):
        self.loop.run_until_complete(self.delete('http://localhost:8080/vospace/nodes/syncdatanode'))
        self.loop.run_until_complete(self.posix_runner.shutdown())
        self.loop.run_until_complete(self.posix_runner.cleanup())
        super().tearDown()

    def test_push_to_space_sync_push_abort(self):
        async def run():
            node = Node('/syncdatanode')
            push = PushToSpace(node, [HTTPPut()])
            transfer = await self.sync_transfer_node(push, 200)
            put_end = transfer.protocols[0].endpoint.url
            job_id = os.path.basename(put_end)
            set_fuzz(True)

            async def defer_abort(job_id):
                await asyncio.sleep(0.5)
                await self.change_job_state(job_id, state='PHASE=ABORT', expected_status=200)
                await self.poll_job(job_id, poll_until=('ABORTED', 'ERROR'), expected_status='ABORTED')

            tasks = [
                asyncio.ensure_future(self.push_to_space(put_end, '/tmp/datafile.dat', expected_status=400)),
                asyncio.ensure_future(defer_abort(job_id))
            ]

            await asyncio.gather(*tasks)
            set_fuzz(False)
            await self.poll_job(job_id, poll_until=('ABORTED', 'ERROR'), expected_status='ABORTED')

        self.loop.run_until_complete(run())

    def test_push_to_space_sync_pull_abort(self):
        async def run():
            node = Node('/syncdatanode')
            push = PushToSpace(node, [HTTPPut()])
            transfer = await self.sync_transfer_node(push, 200)
            put_end = transfer.protocols[0].endpoint.url
            await self.push_to_space(put_end, '/tmp/datafile.dat', expected_status=200)

            pull = PullFromSpace(node, [HTTPGet()])
            transfer = await self.sync_transfer_node(pull)
            pull_end = transfer.protocols[0].endpoint.url

            job_id = os.path.basename(pull_end)
            set_fuzz(True)

            async def defer_abort(job_id):
                await asyncio.sleep(0.5)
                await self.change_job_state(job_id, state='PHASE=ABORT', expected_status=200)
                await self.poll_job(job_id, poll_until=('ABORTED', 'ERROR'), expected_status='ABORTED')

            tasks = [
                asyncio.ensure_future(self.pull_from_space_defer_error(pull_end, '/tmp/download/')),
                asyncio.ensure_future(defer_abort(job_id))
            ]
            # client recv should fail
            with self.assertRaises(IOError):
                await asyncio.gather(*tasks)

            set_fuzz(False)
            await self.poll_job(job_id, poll_until=('ABORTED', 'ERROR'), expected_status='ABORTED')

        self.loop.run_until_complete(run())

if __name__ == '__main__':
    unittest.main()