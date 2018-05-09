from .exception import VOSpaceError


async def register_space(db_pool, name, host, port, accepts_views, provides_views,
                         accepts_protocols, provides_protocols, parameters):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            result = await conn.fetchrow("select * from space where host=$1 and port=$2 for update",
                               host, port)
            if not result:
                result = await conn.fetchrow("insert into space (host, port, name, accepts_views, provides_views, "
                                             "accepts_protocols, provides_protocols, params) "
                                             "values ($1, $2, $3, $4, $5, $6, $7, $8) returning id",
                                             host, port, name, accepts_views, provides_views,
                                             accepts_protocols, provides_protocols, parameters)
                return int(result['id'])
            # if there is an existing plugin associated with this space
            # and its not the one specified then raise an error
            # Don't want to infringe on another space and its data
            if result['name'] != name:
                raise VOSpaceError(400, 'Can not start space over an existing '
                                        'space on the same host and port.')
            return int(result['id'])


async def register_storage(db_pool, name, host, port, direction):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            result = await conn.fetchrow("select * from space where name=$1 for update", name)
            if not result:
                raise VOSpaceError(404, f'Space not found. Name: {name}')
            await conn.fetchrow("insert into storage (name, host, port, direction) "
                                "values ($1, $2, $3, $4) on conflict (name, host, port) "
                                "do update set direction=$4",
                                name, host, port, direction)
            return int(result['id'])


async def get_storage_endpoints(conn, space_id, job_id, protocol, direction):
    results = await conn.fetch("select storage.host, storage.port from storage "
                               "inner join space on space.name=storage.name "
                               "where space.id=$1 and storage.direction ? $2",
                               space_id, direction)
    if not results:
        raise VOSpaceError(404, "No storage endpoints found.")

    endpoints = []
    for row in results:
        prot = 'http' if protocol.split('#')[1].startswith('http') else 'https'
        endpoints.append(f'{prot}://{row["host"]}:{row["port"]}/vospace/{direction}/{job_id}')
    return {'protocol': protocol, 'endpoints': endpoints}