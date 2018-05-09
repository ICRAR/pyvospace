import os
import asyncio
import argparse
import configparser

from aiohttp import web

from .space import VOSpaceServer


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--cfg', type=str, action='store')
    args = parser.parse_args()

    if args.cfg:
        cfg_file = args.cfg
    else:
        app_path = os.path.dirname(os.path.realpath(__file__))
        cfg_file = f"{app_path}/cfg/vospace.ini"

    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(VOSpaceServer.create(cfg_file))
    web.run_app(app)


if __name__ == "__main__":
    main()