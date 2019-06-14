# pyvospace-ngas
NGAS plugin for PyVOSpace, implementation in Python3.

This plugin provides a PyVOSpace implementation that uses an NGAS server as the backing store. The plugin connects to both a PostgreSQL database and one or more federated instances of an NGAS server.

**Requirements**

- Python3.6 or greater.
- Docker support for running a PostgreSQL server. 
- Access to a running NGAS server or servers. To setup an NGAS server please see the [online documentation](https://ngas.readthedocs.io/en/latest/). 
- FUSE filesystem modules (including fusepy).

**Installation**

For installation, including setting up the docker database instance, please see the main ICRAR PyVOSpace [documentation page](README.md).

**Basic configuration**

The file *test_vo_ngas.ini* contains a default configuration, as shown below.

```
[Space]
host = localhost
port = 8082
name = ngas
uri = icrar.org
dsn = postgres://vos_user:vos_user@localhost:5435/vospace
parameters = {}
secret_key = ZlmNyXdQgRhhrC2Wwy-gLZj7Wv6ZtoKH
domain =
use_ssl = 0

[Storage]
name = ngas
host = localhost
port = 8083
parameters = {"staging_dir": "/tmp/ngas/staging/"}
use_ssl = 0
ngas_servers=[{"hostname" : "localhost", "port" : 7777},
    {"hostname" : "localhost", "port" : 7777}]
```

Note the *ngas_servers* line. Multiple NGAS servers may be specified here, and it is assumed that the NGAS servers are synchronised and can work with the same information. At runtime an NGAS server will be chosen at random for both the space and storage servers.

**Running the Space and Storage servers**

'ngas_space --cfg test_vo_ngas.ini'
'ngas_storage --cfg test_vo_ngas.ini'

**Unit tests**

Unit tests are located in test/test_ngas/\*.py and assume an NGAS server is running on localhost at port 7777.

**Common errors**

If you see an error such as,

```
raise aiohttp.web.HTTPServerError(reason=f"File {filename_ngas} not deleted properly from NGAS server"),
```

it means that the NGAS server is not configured with caching enabled and has refused to delete a file. In this instance check if you want the NGAS server to be able to delete files and then enable caching in the NGAS server configuration.
