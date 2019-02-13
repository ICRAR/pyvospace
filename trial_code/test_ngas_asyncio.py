# Function to test NGAS requests
import aiohttp
import aiofiles
import asyncio
import urllib
import os

# POST the file and then retrieve it using asynchronous requests

# Upload the data
async def upload(hostname, port, filename_ngas, filename_local):
    # Upload a simple file to an NGAS server
    params={"filename": filename_ngas,
            "file_id" : filename_ngas,
            "mime_type":"application/octet-stream"}

    encoded_parms=urllib.parse.urlencode(params)

    ngas_string="ngas_client"

    # Get the number of bytes in a file
    nbytes=os.stat(filename_local).st_size

    # Open a HTTP connection to the NGAS server
    # Multiline f string for header
    raw_header= f"POST /ARCHIVE?{encoded_parms} HTTP/1.1\r\n" \
                f"Host: {hostname}:{port}\r\n" \
                f"Accept: */*\r\n" \
                f"User-Agent: {ngas_string}\r\n" \
                f"Content-Type: application/octet-stream\r\n" \
                f"Content-Length: {nbytes}\r\n" \
                f"Expect: 100-continue\r\n" \
                f"\r\n"

    raw_header=raw_header.encode("utf-8")
    print(raw_header)

    (reader, writer) = await asyncio.open_connection(host=hostname, port=port)
    async with aiofiles.open(filename_local,"rb") as fd:
        writer.write(raw_header)

        while True:
            buffer=await fd.read(65536)
            if buffer:
                #print(buffer)
                writer.write(buffer)
                await writer.drain()
            else:
                break

        writer.write_eof()
        await writer.drain()
        # Write some headers to the connection

    # Read any response
    print("\nMoving to response:\n")
    while True:
        buffer = await reader.read(1024)
        if buffer:
            print(buffer.decode())
        else:
            break

    #reader.close()
    # Keep the writer open
    writer.close()

async def download(session, filename_ngas, filename_local, ngas_server):
    # Download a simple file from an NGAS server
    url=ngas_server+"/RETRIEVE"
    params = {"file_id": filename_ngas}
    async with aiofiles.open(filename_local, "wb") as fd:
        async with session.get(url, params=params) as resp:
            print(await resp.text())
            async for chunk in resp.content.iter_chunked(1024):
                if chunk:
                    fd.write(chunk)
            print("End of download")


async def main():
    session = aiohttp.ClientSession()
    filename_ngas="file.img"
    filename_local_up="file_up.img"
    filename_local_down="file_down.img"

    ngas_server="http://localhost:7777"

    ngas_hostname="localhost"
    ngas_port=7777

    await upload(ngas_hostname, ngas_port, filename_ngas, filename_local_up)
    #await upload(session, filename_ngas, filename_local_up, ngas_server)
    #await download(session, filename_ngas, filename_local_down, ngas_server)
    await session.close()

loop=asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()




















