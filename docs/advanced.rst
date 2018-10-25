Advanced
=====================================

**Design**

This reference implementation is based on the `VOSpace 2.1 specification. <http://www.ivoa.net/documents/VOSpace/>`_

The design separates the metadata and data services into into 2 main HTTP(S) based processes:

* The VOSpace metadata service; and
* The VOSpace storage service.

The reference implementation provides a posix based VOSpace storage service out-of-the-box.
The design allows for custom VOSpace storage services to be implemented by extending a basic set of interfaces.
Please refer to the advanced section for details.

**VOSpace Services**

The VOSpace metadata service is designed to allow it to be a standalone process that implements
the core metadata features of the VOSpace specification. These include the following web operations:

* Service Metadata (Section 6.1)
* Creating and manipulating data nodes (Section 6.2)
* Accessing metadata (Section 6.3)

The transfer of data (Section 6.4) to and from a VOSpace node is the responsibility of the
VOSpace storage process(es). The following transfer actions are supported:

* pushToVoSpace; and
* pullFromVoSpace.


**Custom VOService**

The base package implements a basic posix backend. If the developer wished to implement a custom storage backend follow the recipe below.

Note: that the storage classes in this packages only supports the HTTP and HTTP(S) protocols.
Other protocols can be implemented by following the recipe found in :py:class:`pyvospace.server.storage.HTTPSpaceStorageServer`

1. Implement interface :py:class:`pyvospace.server.space.AbstractSpace`
    * This implementation must be passed to :py:func:`pyvospace.server.space.SpaceServer.setup` for the VOSpace metadata service.

2. To control permissions on the space extend :py:class:`pyvospace.server.space.SpaceServer` and override :py:class:`pyvospace.server.space.SpacePermission.permits`.

Note: The authentication and authorisation mechanisms are outside the scope of the implementation but it does implement
the hooks to the `aiohttp_security <https://aiohttp-security.readthedocs.io/en/latest/>`_ library which is can be leveraged.
Refer to posix example for more details.

3. Implement the storage interface :py:class:`pyvospace.server.storage.HTTPSpaceStorageServer`

Note that when implementing the upload funtionality it's important to first upload the data to a staging area
then copy or move data to its final location in a single transaction. In this way the system can ensure consistency in a concurrent environment::

    async def upload(self, job: StorageUWSJob, request: aiohttp.web.Request):

        # upload data to staging area first
        ...

        # copy data to final location and update node details in db in single transaction.
        # good practise to asyncio.shield save() and move() to ensure they are not aborted.

        async with job.transaction() as tr:
            node = tr.target # get the target node that is associated with the data
            node.size = size # set the size
            node.storage = self.storage # set the storage back end so it can be found
            await asyncio.shield(node.save()) # save details to db
            await asyncio.shield(move(stage_file_name, real_file_name)) # move file from staging to final location in the space in single transaction


4. Create configuration file for the Space.


**[Space]**

Ref by :py:class:`pyvospace.server.space.SpaceServer`

    * host: host of space metadata service.
    * port: port of space metadata service.
    * name: name of the space (unique).
    * uri: uri base for the space.
    * dsn: connection string to the database.
    * parameters: any customs parameters defined by the implementor of the space (json string)
    * use_ssl: use https (1: yes, 0: no)
    * cert_file: SSL certificate file.
    * key_file = SSL key file.

**[Storage]**

Ref by :py:class:`pyvospace.server.storage.HTTPSpaceStorageServer`

    * host: host of space storage.
    * port: port of space storage.
    * name: name of the space (unique).
    * parameters: any customs parameters defined by the implementor of the space (json string)
    * use_ssl: use https (1: yes, 0: no)
    * cert_file: SSL certificate file.
    * key_file = SSL key file.

Configuration Example::

   [Space]
   host = localhost
   port = 8080
   name = posix
   uri = icrar.org
   dsn = postgres://vos_user:vos_user@localhost:5435/vospace
   parameters = {}
   use_ssl = 0

   [Storage]
   name = posix
   host = localhost
   port = 8081
   parameters = {"root_dir": "/tmp/posix/storage/", "staging_dir": "/tmp/posix/staging/"}
   use_ssl = 0


5. Start each service.

VOSpace metadata service::

        abstract_space = MySpecificImpl()
        app = SpaceServer(<path to config>)
        await app.setup(abstract_space)


VOSpace storage service::

        app = MyHTTPSpaceStorageServer(<path to config>)
        await app.setup()
