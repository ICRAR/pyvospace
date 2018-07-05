import os
import asyncio
import argparse
import configparser

from aiohttp import web

from .posix_storage import PosixStorageServer


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--cfg', type=str, action='store')
    args = parser.parse_args()

    if args.cfg:
        cfg_file = args.cfg
    else:
        app_path = os.path.dirname(os.path.realpath(__file__))
        cfg_file = f"{app_path}/cfg/storage.ini"

    config = configparser.ConfigParser()
    config.read(cfg_file)
    host = config['Storage']['host']
    port = config['Storage']['port']

    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(PosixStorageServer.create(cfg_file))
    web.run_app(app, host=host, port=port)


if __name__ == "__main__":
    main()