import aiohttp
import asyncio
import json
from xml.etree import ElementTree
import os
from datetime import datetime
import json

ngas_hostname="127.0.0.1"
ngas_port=7777
import pdb

async def run():

    ngas_session = aiohttp.ClientSession()

    ngas_filename = "datafile.dat_74f7ec09-f350-40d2-a4f8-3f72d31d5615"
    ngas_filename = "datafile.dat_74f7ec09-f350-40d2-a4f8-3f72d31d5615"

    # Get the list of all files with this file name
    params={"query" : "files_like", "like" : ngas_filename, "format" : "json"}
    url = f'http://{ngas_hostname}:{ngas_port}/QUERY'
    resp = await ngas_session.get(url, params=params)

    lines = await resp.content.read()
    file_entries=json.loads(lines)

    for file in file_entries:

        if ("file_id" in file) and (file["file_id"]==ngas_filename):
            params = {"file_id": file["file_id"], "disk_id": file["disk_id"], "file_version": file["file_version"]}
            url = f'http://{ngas_hostname}:{ngas_port}/CACHEDEL'
            resp = await ngas_session.get(url, params=params)

            if (resp.status!=200):
                #raise an exception
                pass

            print("disk_id: {}, file_id: {}, file_version: {}".format(file["disk_id"], file["file_id"], file["file_version"]))


    pdb.set_trace()

    #params={"file_id" : ngas_filename, "disk_id" : "%", "file_version" : "%"}

    #url = f'http://{ngas_hostname}:{ngas_port}/CACHEDEL'
    #resp = await ngas_session.get(url, params=params)

    #resp = await ngas_session.post(url, params=params)
    #lines=await resp.content.read()

    print(lines)

    await ngas_session.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
loop.close()

