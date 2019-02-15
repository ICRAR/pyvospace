import aiohttp
import asyncio

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

async def main():
    session=aiohttp.ClientSession()
    resp = await session.get('http://python.org')
    wrapper = Wrapper(resp.content)
    await session.post('http://httpbin.org/post', data=wrapper)
    print(wrapper._counter)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

