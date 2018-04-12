import os
import signal
import asyncio
import aioprocessing


class AsyncProcess(object):

    def __init__(self, target, name, args=(), kwargs={}):
        self._ready = False
        self._queue = aioprocessing.AioQueue()
        self._p = aioprocessing.AioProcess(target=AsyncProcess._run,
                                           name=name,
                                           args=(self._queue, target, *args),
                                           kwargs=kwargs,
                                           daemon=True)

    @classmethod
    def create(self, target, name, args=(), kwargs={}):
        return AsyncProcess(target, name, args, kwargs)

    async def apply(self):
        self._p.start()
        # wait for process to finish init
        self._ready = await self._queue.coro_get()
        # now wait for result from processing
        result = await self._queue.coro_get()
        if isinstance(result, BaseException):
            raise result
        return result

    async def _terminate(self):
        if self._p.is_alive():
            # wait for the process to setup with setsid
            # and sighandler before terminating
            while self._ready is False:
                await asyncio.sleep(0)
            self._p.terminate()

    async def terminate(self):
        await asyncio.shield(self._terminate())

    async def join(self):
        await self._p.coro_join()

    @classmethod
    def handler(cls, signum, frame):
        try:
            # kill process group and all its children
            os.killpg(os.getpgid(os.getpid()), signal.SIGINT)
        except:
            pass

    @classmethod
    def _run(cls, queue, func, *args, **kwargs):
        try:
            # create a new session and group for this process
            os.setsid()
            signal.signal(signal.SIGINT, AsyncProcess.handler)
            # tell calling process init has finished
            queue.put(True)
            # call process function and wait for result
            result = func(*args, **kwargs)
            queue.put(result)
        except BaseException as e:
            queue.put(e)
        finally:
            queue.close()