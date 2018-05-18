from aiohttp import helpers, web
from aiohttp_security.abc import AbstractAuthorizationPolicy
from aiohttp_security import remember, forget, authorized_userid, permits
from passlib.hash import pbkdf2_sha256


class DBUserNodeAuthorizationPolicy(AbstractAuthorizationPolicy):

    def __init__(self, space_name, db_pool):
        super().__init__()
        self.space_name = space_name
        self.db_pool = db_pool

    async def authorized_userid(self, identity):
        async with self.db_pool.acquire() as conn:
            results = await conn.fetchrow("select username from users "
                                          "where username=$1 and space_name=$2",
                                          identity, self.space_name)
        if results:
            return results['username']
        return None

    def _any_value_in_lists(self, a, b):
        return any(i in a for i in b)

    async def permits(self, identity, permission, context=None):
        async with self.db_pool.acquire() as conn:
            user_groups = await conn.fetchrow("select groupread, groupwrite from users "
                                              "where username=$1 and space_name=$2",
                                              identity, self.space_name)
            if not user_groups:
                raise web.HTTPForbidden('user not found')

        if context['owner'] == identity:
            return True

        if permission in ('setNode', 'createNode', 'pushToVoSpace', 'moveNode', 'copyNode'):
            return self._any_value_in_lists(user_groups['groupwrite'], context['groupwrite'])

        elif permission in ('getNode', 'pullFromVoSpace'):
            return self._any_value_in_lists(user_groups['groupread'], context['groupread'])

        else:
            raise web.HTTPForbidden('unkown permission')


class DBUserAuthentication(object):

    def __init__(self, space_name, db_pool):
        self.db_pool = db_pool
        self.space_name = space_name

    async def get_user(self, identity):

        async with self.db_pool.acquire() as conn:
            return await conn.fetchrow("select * from users where username=$1 and space_name=$2",
                                       identity, self.space_name)

    async def check_credentials(self, username, password):
        user = await self.get_user(username)
        if not user:
            return None

        #if pbkdf2_sha256.verify(password, user['password']):
        if password == user['password']:
            return username
        return None

    async def login(self, request):
        try:
            auth = helpers.BasicAuth.decode(request.headers['Authorization'])
        except:
            return web.HTTPForbidden()

        try:
            user = await self.check_credentials(auth.login, auth.password)
            if not user:
                return web.HTTPForbidden()

            response = web.Response()
            await remember(request, response, user)
            return response

        except web.HTTPForbidden:
            raise
        except Exception:
            import traceback
            traceback.print_exc()
            raise web.HTTPInternalServerError()

    async def logout(self, request):
        try:
            response = web.Response()
            await forget(request, response)
            return response

        except Exception as e:
            raise web.HTTPInternalServerError()
