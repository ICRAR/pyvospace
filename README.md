# pyvospace
VOSpace implementation in Python3.

**Requirements**

- Python3.6 or greater.
- Docker Support.
- FUSE filesystem modules (including fusepy)

**Quick Start Guide**

Quick start guide to building and running a posix backed vospace.

Refer to [full documentation](https://pyvospace.readthedocs.io) for more information. 

***Fabric Installation***

pyvospace comes with a fabric based installation mechanism, which supports remote and local automatic installations
on a variety of platforms and in a flexible way. This will install all the dependencies and the
complete environment with a single command line. It requires fabric<2 to be installed in your
local python environment. There are a number of issues with fabric versions smaller than 2 and python3 and
thus the python version running fabric needs to be a python2.7 version. Note that the fabric installation will
still install Python3.6 or Python3.7 for pyvospace into a dedicated virtual environment.

***Installation of fabric***

Make sure that you have a suitable python version on your path and install fabric

```
python -V
>>> Python 2.7.15
pip install fabric<2
```
From the root of the repository execute the fabric installation

```
fab hl.user_deploy -H localhost
```

This will install pyvospace into a dedicated virtual environment into your home directory and test the installation.
The fabric script is very versatile and has many options, including installations on AWS and parallel installations
on multiple hosts.

***Manual Installation***

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
