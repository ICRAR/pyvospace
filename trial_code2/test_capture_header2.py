# Function to test NGAS requests
import aiohttp
import aiofiles
import asyncio

@asyncio.coroutine
def handle_echo(reader, writer):
    data = yield from reader.read(100)
    message = data.decode()
    addr = writer.get_extra_info('peername')
    print("Received %r from %r" % (message, addr))

    print("Send: %r" % message)
    writer.write(data)
    yield from writer.drain()

    print("Close the client socket")
    writer.close()

async def watchnwrite(reader, writer):
    print("hello?")
    async with aiofiles.open("output.bin","wb") as fd:
        #(reader, writer) = await asyncio.open_connection(host=hostname, port=port)
        while True:
            chunk=await reader.read(1024)
            if chunk:
                await fd.write(chunk)
                await fd.flush()

        writer.close()
    return(None)


loop = asyncio.get_event_loop()
coro = asyncio.start_server(watchnwrite, '127.0.0.1', 8888, loop=loop)
server = loop.run_until_complete(coro)

# Serve requests until Ctrl+C is pressed
print('Serving on {}'.format(server.sockets[0].getsockname()))
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

# Close the server
server.close()
loop.run_until_complete(server.wait_closed())
loop.close()




















