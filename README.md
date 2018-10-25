# pyvospace
VOSpace implementation in Python3.

**Requirements**

- Python3.6 or greater.
- Docker Support.
- FUSE filesystem modules (including fusepy)

**Quick Start Guide**

Quick start guide to building and running a posix backed vospace.

From the root of the repository, create and start database environment

```
cd pyvospace/server/deploy
docker-compose build
docker-compose up
```

Create Virtual Environment

```
python3 -m venv venv
source venv/bin/activate
```

From the root of the repository, nstall pyvospace modules

```
pip install -U pip setuptools wheel
pip install .
```


Create basic configuration

`vim test_vo.ini`

```
[Space]
host = localhost
port = 8080
name = posix
uri = icrar.org
dsn = postgres://vos_user:vos_user@localhost:5435/vospace
parameters = {}
secret_key = ZlmNyXdQgRhhrC2Wwy-gLZj7Wv6ZtoKH
domain =
use_ssl = 0

[Storage]
name = posix
host = localhost
port = 8081
parameters = {"root_dir": "/tmp/posix/storage/", "staging_dir": "/tmp/posix/staging/"}
use_ssl = 0
```

Confirm deployment

`python -m unittest discover test`

All the tests should pass. Note: Ignore any ConnectionResetError warnings as it's part of a test.

Start vospace metadata server

`posix_space --cfg test_vo.ini`

Start vospace posix storage server

`posix_storage --cfg test_vo.ini`

Install appropriate FUSE libraries for your platform, then install fusepy.
```
pip install fusepy
```

Start FUSE posix client.
```
mkdir /tmp/fuse
python -m pyvospace.client.fuse --host localhost --port 8080 --username test --password test --mountpoint /tmp/fuse/
```

Test FUSE posix client

```
cd /tmp/fuse
mkdir newdir
cd newdir
echo 'hello' >> data
cat data
```