import os
import asyncio
import argparse
import configparser

from aiohttp import web

from .posix_space import PosixSpaceServer


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--cfg', type=str, action='store')
    args = parser.parse_args()

    if args.cfg:
        cfg_file = args.cfg
    else:
        app_path = os.path.dirname(os.path.realpath(__file__))
        cfg_file = f"{app_path}/cfg/space.ini"

    config = configparser.ConfigParser()
    config.read(cfg_file)
    host = config['Space']['host']
    port = config['Space']['port']

    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(PosixSpaceServer.create(cfg_file))
    web.run_app(app, host=host, port=port)


if __name__ == "__main__":
    main()