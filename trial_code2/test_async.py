# Function to test NGAS requests
import aiohttp
import asyncio


async def fun1():
    print("fun1 start")
    await(asyncio.sleep(20))
    print("fun1 stop")

async def fun2():
    print("fun2 start")
    await(asyncio.sleep(1))
    print("fun2 stop")

async def main():
    print('hello')
    await fun1()
    await fun2()
    print('world')

loop=asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()


















