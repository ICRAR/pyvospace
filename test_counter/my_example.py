import aiohttp
import asyncio
import io

class Wrapper:
    def __init__(self, content):
        self._content = content
        self._counter = 0
        self._iter = None

    def __aiter__(self):
         self._iter = self._content.__aiter__()
         return self

    async def __anext__(self):
         chunk = await self._iter.__anext__()
         self._counter += len(chunk)
         return chunk

class ChunkedByteCounter:
    """A wrapper class to count the number of bytes being sent from a stream"""
    def __init__(self, content):
        self._content=content
        self._size=0
        self._iter=None

    def __aiter__(self):
        #self._iter=self._content.__aiter__()
        self._iter=self._content.iter_chunked(io.DEFAULT_BUFFER_SIZE)
        return self

    async def __anext__(self):
        buffer=await self._iter.__anext__()
        self._size+=len(buffer)
        return buffer


async def main():
    session=aiohttp.ClientSession()
    resp = await session.get('http://python.org')
    wrapper = ChunkedByteCounter(resp.content)
    await session.post('http://httpbin.org/post', data=wrapper)
    print(wrapper._size)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

