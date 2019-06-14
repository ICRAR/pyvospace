#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2018
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA

import os
import ssl
import asyncio
import argparse
import configparser

from aiohttp import web

from .ngas_storage import NGASStorageServer

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
    port = config.getint('Storage', 'port')
    use_ssl = config.getint('Storage', 'use_ssl')

    context = None
    if bool(use_ssl):
        cert_file = config['Storage']['cert_file']
        key_file = config['Storage']['key_file']
        context = ssl.SSLContext()
        context.load_cert_chain(certfile=cert_file, keyfile=key_file)

    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(NGASStorageServer.create(cfg_file))
    web.run_app(app, port=port, ssl_context=context)


if __name__ == "__main__":
    main()