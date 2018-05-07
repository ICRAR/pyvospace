from .exception import VOSpaceError


async def update_space(db_pool, host, port, plugin_name):
    if not host:
        raise VOSpaceError(400, "host not defined")

    if not port:
        raise VOSpaceError(400, "port not defined")

    if not plugin_name:
        raise VOSpaceError(400, "plugin name not defined")

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            result = await _get_space(conn, host, port)
            if not result:
                return await _create_space(conn, host, port, plugin_name)
            # if there is an exisiting plugin associated with this space
            # and its not the one specified then raise an error
            # Don't want to infringe on another space and its data
            if result['plugin_name'] != plugin_name:
                raise VOSpaceError

            return int(result['id'])


async def _get_space(conn, host, port):
    return await conn.fetchrow("select * from space where host=$1 and port=$2 for update",
                               host, port)


async def _create_space(conn, host, port, plugin_name):
    return await conn.fetchrow("insert into space (host, port, plugin_name)"
                               "values ($1, $2, $3) returning id",
                               host, port, plugin_name)
