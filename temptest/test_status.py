import aiohttp
import asyncio
import json
from xml.etree import ElementTree
import os
from datetime import datetime

ngas_hostname="127.0.0.1"
ngas_port=7777
import pdb

async def statvfs(path):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, os.statvfs, path)


async def lstat(path):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, os.lstat, path)


def convert_to_epoch_seconds(date_string):
    dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f")
    # Seconds since the epoch
    seconds=str(int((dt - datetime(1970, 1, 1)).total_seconds()))
    return seconds

async def run():

    ngas_session = aiohttp.ClientSession()

    params={"query" : "files_list", "format" : "json"}
    url = f'http://{ngas_hostname}:{ngas_port}/QUERY'

    resp = await ngas_session.get(url, params=params)

    # Get the status on a particular file
    ngas_filename="mytar.tar.gz_0ede81a2-8726-4e0b-b745-6191609b7fb5"
    url = f'http://{ngas_hostname}:{ngas_port}/STATUS'
    params={"file_id" : ngas_filename}

    resp = await ngas_session.get(url, params=params)
    lines=await resp.content.read()
    treelines=ElementTree.fromstring(lines)

    x={t.tag : t for t in treelines.iter()}

    diskstatus=x["DiskStatus"]
    for tag, value in diskstatus.items():
        print(tag,value)

    print()

    filestatus=x["FileStatus"]
    for tag, value in filestatus.items():
        print(tag,value)

    # File size
    st_size=int(filestatus.get('FileSize'))
    # Creation time
    st_ctime=filestatus.get("IngestionDate")
    # Modification time
    st_mtime=filestatus.get('ModificationDate')
    if st_mtime=="":
        st_mtime=st_ctime

    st_ctime=convert_to_epoch_seconds(st_ctime)
    st_mtime=convert_to_epoch_seconds(st_mtime)

    #f_bsize − preferred file system blocksize.
    #f_frsize − fundamental file system blocksize.
    #f_blocks − total number of blocks in the filesystem.
    #f_bfree − total number of free blocks.
    #f_bavail − free blocks available to non - super user.
    #f_files − total number of file nodes.
    #f_ffree − total number of free file nodes.
    #f_favail − free nodes available to non - super user.
    #f_flag − system dependent.
    #f_namemax − maximum file name length.

    # Make up a zero dictionary for now, this information is not correct.
    struct_statvfs_dict = dict((key, 0) for key in ('f_bavail', 'f_bfree', 'f_blocks', 'f_bsize',
                                                    'f_favail', 'f_ffree', 'f_files', 'f_flag',
                                                    'f_frsize', 'f_namemax'))


    temp = os.lstat(os.getcwd())

    print(type(st_size), type(st_mtime))

    pdb.set_trace()

    # So I need
    # st_size
    # st_mtime
    # st_ctime
    # struct_statvfs_dict (f_bavail, f_bfree, f_blocks, f_bsize, f_favail, f_ffree, f_files, f_flag, f_frsize, f_namemax)

    #struct_lstat = await lstat(real_path)
    #struct_statvfs = await statvfs(real_path)
    #struct_statvfs_dict = dict((key, getattr(struct_statvfs, key)) for key in ('f_bavail', 'f_bfree',
    #                                                                           'f_blocks', 'f_bsize',
    #                                                                           'f_favail', 'f_ffree',
    #                                                                           'f_files', 'f_flag',
    #                                                                           'f_frsize', 'f_namemax'))

    #prop_length = Property('ivo://ivoa.net/vospace/core#length', struct_lstat.st_size)
    #prop_btime = Property('ivo://ivoa.net/vospace/core#btime', struct_lstat.st_mtime)
    #prop_ctime = Property('ivo://ivoa.net/vospace/core#ctime', struct_lstat.st_ctime)
    #prop_mtime = Property('ivo://ivoa.net/vospace/core#mtime', struct_lstat.st_mtime)
    #prop_statfs = Property('ivo://icrar.org/vospace/core#statfs', json.dumps(struct_statvfs_dict))

    #pdb.set_trace()

    #for n in subtree:
    #    print(n)
    #print(subtree)

    #print(len(lines))

    #print(len(lines))

    await ngas_session.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
loop.close()

