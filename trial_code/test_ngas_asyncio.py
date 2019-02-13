# Function to test NGAS requests
import aiohttp
import aiofiles
import asyncio

# POST the file and then retrieve it using asynchronous requests


# Upload the data
async def upload_asyncio(hostname, port, filename_ngas, filename_local):
    # Upload a simple file to an NGAS server
    url=ngas_server+"/ARCHIVE"
    params={"filename": filename_ngas,
            "mime_type":"application/octet-stream"}



    (reader, writer) = await asyncio.open_connection(host=hostname, port=port)


    # Write some headers to the connection


    # Post an upload to the NGAS server, initially with a file, later with an NGAS stream
    #async with aiofiles.open(filename_local, 'rb') as fd:
    #    async with session.post(url, params=params, data={filename_ngas: fd}) as resp:
    #        print(await resp.text())
    #        print("End of upload")

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

    await upload_asyncio(ngas_hostname, ngas_port, filename_ngas, filename_local_up)

    #await upload(session, filename_ngas, filename_local_up, ngas_server)
    await download(session, filename_ngas, filename_local_down, ngas_server)
    await session.close()

loop=asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()




















