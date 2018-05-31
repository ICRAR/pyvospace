import asyncpg

from pyvospace.core.exception import VOSpaceError, InvalidURI, NodeDoesNotExistError, PermissionDenied, \
    ContainerDoesNotExistError, DuplicateNodeError
from pyvospace.core.model import Node, DataNode, UnstructuredDataNode, StructuredDataNode, LinkNode, ContainerNode, \
    NodeType, Property, DeleteProperty, NodeTextLookup


class NodeDatabase(object):
    def __init__(self, space_id, db_pool, permission):
        self.space_id = space_id
        self.db_pool = db_pool
        self.permission = permission

    @classmethod
    def path_to_ltree(cls, path):
        path_array = list(filter(None, path.split('/')))
        if len(path_array) == 0:
            raise InvalidURI("Path is empty")
        return '.'.join(path_array)

    @classmethod
    def _resultset_to_node(cls, root_node_rows, root_properties_row):
        if root_node_rows[0] is None:
            return None
        node = NodeDatabase._create_node(root_node_rows[0])
        node.owner = root_node_rows[0]['owner']
        node.group_read = root_node_rows[0]['groupread']
        node.group_write = root_node_rows[0]['groupwrite']
        root_node_rows.pop(0)
        child_nodes = [NodeDatabase._create_node(node_row) for node_row in root_node_rows]
        if len(child_nodes) > 0:
            assert node.node_type == NodeType.ContainerNode
            node.set_nodes(child_nodes)
        node.set_properties(NodeDatabase._resultset_to_properties(root_properties_row))
        return node

    @classmethod
    def _resultset_to_properties(cls, results):
        properties = []
        for result in results:
            dic = dict(result)
            properties.append(Property(dic['uri'], dic['value'], dic['read_only']))
        return properties

    @classmethod
    def _create_node(cls, node_row):
        path = node_row['path'].replace('.', '/')
        node_type = node_row['type']

        if node_type == NodeType.Node:
            return Node(path)
        elif node_type == NodeType.DataNode:
            return DataNode(path, busy=node_row['busy'])
        elif node_type == NodeType.StructuredDataNode:
            return StructuredDataNode(path, busy=node_row['busy'])
        elif node_type == NodeType.UnstructuredDataNode:
            return UnstructuredDataNode(path, busy=node_row['busy'])
        elif node_type == NodeType.ContainerNode:
            return ContainerNode(path, busy=node_row['busy'])
        elif node_type == NodeType.LinkNode:
            return LinkNode(path, uri_target=node_row['link'])
        else:
            raise VOSpaceError(500, "Unknown type")

    async def _get_node_and_parent(self, path, conn):
        path_list = list(filter(None, path.split('/')))
        if len(path_list) == 0:
            raise VOSpaceError(400, "Invalid URI. Path is empty")

        path_parent = path_list[:-1]
        path_parent_tree = '.'.join(path_parent)
        path_tree = '.'.join(path_list)

        # share lock both node and parent, important so we
        # dont have a dead lock with move/copy/create
        result = await conn.fetch("select * from nodes where path=$1 or path=$2 "
                                  "and space_id=$3 order by path asc for update",
                                  path_tree, path_parent_tree, self.space_id)
        result_len = len(result)
        if result_len == 1:
            # If its just one result and we have a parent node
            # it can not be the child node. It has to be just the parent!
            if path_parent_tree:
                assert result[0]['path'] == path_parent_tree
                return result[0], None
            # If the only result is the node then return it.
            # This assumes its a root node.
            return None, result[0]
        elif result_len == 2:
            # If there are 2 results the first is the parent
            # the second is the node in question.
            return result[0], result[1]
        return None, None

    async def directory(self, path, conn, identity=None):
        path_tree = NodeDatabase.path_to_ltree(path)

        results = await conn.fetch("select * from nodes where path <@ $1 and "
                                   "nlevel(path)-nlevel($1)<=1 and space_id=$2 "
                                   "order by path asc",
                                   path_tree, self.space_id)
        if len(results) == 0:
            raise NodeDoesNotExistError(f"{path} not found.")

        properties = await conn.fetch("select * from properties "
                                      "where node_path=$1 and space_id=$2",
                                      results[0]['path'], self.space_id)

        node = self._resultset_to_node(results, properties)
        if not await self.permission.permits(identity, 'getNode', context=node):
            raise PermissionDenied('getNode denied.')
        return node

    async def insert(self, node, conn, identity):
        try:
            # We can not have a target unless its a link node
            target = None
            if isinstance(node, LinkNode):
                if node.target is None:
                    raise VOSpaceError(400, f"Type Not Supported. "
                                            f"{NodeTextLookup[node.node_type]} does not have a target.")
                target = node.target

            path = list(filter(None, node.path.split('/')))
            if len(path) == 0:
                raise InvalidURI("Path is empty")

            path_parent = path[:-1]
            node_name = path[-1]
            path_tree = '.'.join(path)

            # get parent node and check if its valid to add node to it
            parent_row, child_row = await self._get_node_and_parent(node.path, conn)
            # if the parent is not found but its expected to exist
            if not parent_row and len(path_parent) > 0:
                raise ContainerDoesNotExistError(f"{'/'.join(path_parent)} not found.")

            if parent_row:
                if parent_row['type'] == NodeType.LinkNode:
                    raise VOSpaceError(400, f"Link Found. {parent_row['name']} found in path.")

                if parent_row['type'] != NodeType.ContainerNode:
                    raise ContainerDoesNotExistError(f"{parent_row['name']} is not a container.")

            parent_node = NodeDatabase._resultset_to_node([parent_row], [])
            if not await self.permission.permits(identity, 'createNode', context=(parent_node, node)):
                raise PermissionDenied('createNode denied.')

            await conn.fetchrow("INSERT INTO nodes (type, name, path, owner, groupread, groupwrite, space_id, link) "
                                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                                node.node_type, node_name, path_tree, identity,
                                node.group_read, node.group_write, self.space_id, target)

            node_properties = []
            for prop in node.properties:
                if prop.persist:
                    node_properties.append(prop.tolist()+[path_tree, self.space_id])

            if node_properties:
                await conn.executemany("INSERT INTO properties (uri, value, read_only, node_path, space_id) "
                                       "VALUES ($1, $2, $3, $4, $5)", node_properties)
            return parent_row, child_row
        except asyncpg.exceptions.UniqueViolationError as f:
            raise DuplicateNodeError(f"{node_name} already exists.")

    async def update_properties(self, node, conn, identity):
        node_path_tree = NodeDatabase.path_to_ltree(node.path)

        results = await conn.fetchrow("select * from nodes where path=$1 "
                                      "and type=$2 and space_id=$3 for update",
                                      node_path_tree, node.node_type, self.space_id)
        if not results:
            raise NodeDoesNotExistError(f"{node.path} not found.")

        node.owner = results['owner']
        node.group_read = results['groupread']
        node.group_write = results['groupwrite']
        if not await self.permission.permits(identity, 'setNode', context=node):
            raise PermissionDenied('setNode denied.')

        pass_through_properties = []
        node_props_delete = []
        node_props_insert = []
        for prop in node.properties:
            if isinstance(prop, DeleteProperty):
                node_props_delete.append(prop.uri)
            else:
                if prop.persist:
                    node_props_insert.append([prop.uri, prop.value, False, node_path_tree, self.space_id])
                else:
                    pass_through_properties.append(prop)

        await conn.execute("update nodes set groupread=$1, groupwrite=$2 where path=$3 and space_id=$4",
                           node.group_read, node.group_write, node_path_tree, self.space_id)

        # if a property already exists then update it
        await conn.executemany("INSERT INTO properties (uri, value, read_only, node_path, space_id) "
                               "VALUES ($1, $2, $3, $4, $5) on conflict (uri, node_path, space_id) "
                               "do update set value=$2 where properties.value!=$2",
                               node_props_insert)

        await conn.execute("DELETE FROM properties WHERE uri=any($1::text[]) "
                           "AND node_path=$2 and space_id=$3",
                           node_props_delete, node_path_tree, self.space_id)

        node_properties = await conn.fetch("select * from properties "
                                           "where node_path=$1 and space_id=$2",
                                           node_path_tree, self.space_id)
        node.set_properties(NodeDatabase._resultset_to_properties(node_properties)+pass_through_properties)
        return node

    async def delete(self, path, conn, identity):
        path_tree = NodeDatabase.path_to_ltree(path)
        results = await conn.fetch("with delete_rows as "
                                   "(delete from nodes where path <@ $1 and space_id=$2 returning *) "
                                   "select * from delete_rows order by delete_rows.path asc;",
                                   path_tree, self.space_id)
        if not results:
            raise NodeDoesNotExistError(f"{path} not found.")
        node = NodeDatabase._resultset_to_node([results[0]], [])
        if not await self.permission.permits(identity, 'deleteNode', context=node):
            raise PermissionDenied('deleteNode denied.')
        return node

    async def delete_properties(self, path, conn):
        path_tree = NodeDatabase.path_to_ltree(path)
        await conn.execute("delete from properties where node_path=$1 and space_id=$2",
                           path_tree, self.space_id)

    async def get_contains_properties(self):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                return await conn.fetch("select distinct(uri), value, read_only from properties where space_id=$1",
                                        self.space_id)