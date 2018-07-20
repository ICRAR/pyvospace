import os
import asyncpg
import base64

from collections import OrderedDict

from pyvospace.core.exception import VOSpaceError, InvalidURI, NodeDoesNotExistError, PermissionDenied, \
    ContainerDoesNotExistError, DuplicateNodeError
from pyvospace.core.model import Node, DataNode, UnstructuredDataNode, StructuredDataNode, LinkNode, ContainerNode, \
    NodeType, Property, DeleteProperty, NodeTextLookup


class NodeDatabase(object):
    def __init__(self, space_id, db_pool, permission):
        self.space_id = space_id
        self.permission = permission
        self.db_pool = db_pool

    @classmethod
    def ltree_to_path(cls, ltree_path):
        path_array = list(filter(None, ltree_path.split('.')))
        path_array_result = [base64.b16decode(i).decode() for i in path_array]
        path = '/'.join(path_array_result)
        return path.replace('%2E', '.')

    @classmethod
    def path_to_ltree(cls, path, as_array=False):
        path_array = list(filter(None, path.replace('.', '%2E').split('/')))
        if not path_array:
            raise InvalidURI("Path is empty")
        path_array_result = [base64.b16encode(i.encode()).decode('ascii') for i in path_array]
        if as_array:
            return path_array_result
        else:
            return '.'.join(path_array_result)

    @classmethod
    def resultset_to_node_tree(cls, results, prop_results=None):
        prop_dict = {}
        if prop_results:
            for result in prop_results:
                prop_dict.setdefault(result['node_path'], []).append(
                    Property(result['uri'], result['value'], result['read_only']))

        root = NodeDatabase._create_node(results.pop(0))
        if results:
            if not isinstance(root, ContainerNode):
                raise InvalidURI(f'{root} is not a container')
        for result in results:
            node = NodeDatabase._create_node(result)
            props = prop_dict.get(result['path'], [])
            node.set_properties(props)
            root.insert_node_into_tree(node)
        return root

    @classmethod
    def _resultset_to_node(cls, root_node_rows, root_properties_row):
        if root_node_rows[0] is None:
            return None
        node = NodeDatabase._create_node(root_node_rows[0])
        root_node_rows.pop(0)
        child_nodes = [NodeDatabase._create_node(node_row) for node_row in root_node_rows]
        if len(child_nodes) > 0:
            assert node.node_type == NodeType.ContainerNode
            node.nodes = child_nodes
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
        path = NodeDatabase.ltree_to_path(node_row['path'])
        node_type = node_row['type']
        id = node_row['id']
        if node_type == NodeType.Node:
            node = Node(path, id=id)
        elif node_type == NodeType.DataNode:
            node = DataNode(path, busy=node_row['busy'], id=id)
        elif node_type == NodeType.StructuredDataNode:
            node = StructuredDataNode(path, busy=node_row['busy'], id=id)
        elif node_type == NodeType.UnstructuredDataNode:
            node = UnstructuredDataNode(path, busy=node_row['busy'], id=id)
        elif node_type == NodeType.ContainerNode:
            node = ContainerNode(path, busy=node_row['busy'], id=id)
        elif node_type == NodeType.LinkNode:
            node = LinkNode(path, uri_target=node_row['link'], id=id)
        else:
            raise VOSpaceError(500, "Unknown type")

        node.owner = node_row['owner']
        node.group_read = node_row['groupread']
        node.group_write = node_row['groupwrite']
        node.path_modified = node_row['path_modified']
        return node

    async def _get_node_and_parent(self, path, conn):
        path_list = NodeDatabase.path_to_ltree(path, as_array=True)
        path_parent = path_list[:-1]
        path_parent_tree = '.'.join(path_parent)
        path_tree = '.'.join(path_list)

        try:
            # share lock both node and parent, important so we
            # dont have a dead lock with move/copy/create
            result = await conn.fetch("select * from nodes where path=$1 or path=$2 "
                                      "and space_id=$3 order by path asc for update",
                                      path_tree, path_parent_tree, self.space_id)
        except asyncpg.exceptions.PostgresSyntaxError:
            raise InvalidURI(f"{path} contains invalid characters.")

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

            path_list = NodeDatabase.path_to_ltree(node.path, as_array=True)
            path_parent = path_list[:-1]
            node_name = os.path.basename(node.path)
            path_tree = '.'.join(path_list)
            # get parent node and check if its valid to add node to it
            parent_row, child_row = await self._get_node_and_parent(node.path, conn)
            # if the parent is not found but its expected to exist
            if not parent_row and len(path_parent) > 0:
                raise ContainerDoesNotExistError(f"{NodeDatabase.ltree_to_path(''.join(path_parent))} not found.")

            if parent_row:
                if parent_row['type'] == NodeType.LinkNode:
                    raise VOSpaceError(400, f"Link Found. {parent_row['name']} found in path.")

                if parent_row['type'] != NodeType.ContainerNode:
                    raise ContainerDoesNotExistError(f"{parent_row['name']} is not a container.")

            parent_node = NodeDatabase._resultset_to_node([parent_row], [])
            if not await self.permission.permits(identity, 'createNode', context=(parent_node, node)):
                raise PermissionDenied('createNode denied.')

            await conn.fetchrow("insert into nodes (type, name, path, owner, "
                                "groupread, groupwrite, space_id, link) "
                                "values ($1, $2, $3, $4, $5, $6, $7, $8)",
                                node.node_type, node_name, path_tree, identity,
                                node.group_read, node.group_write, self.space_id, target)
            node_properties = []
            for prop in node.properties:
                if prop.persist:
                    node_properties.append(prop.tolist()+[path_tree, self.space_id])

            if node_properties:
                await conn.executemany("insert into properties (uri, value, read_only, node_path, space_id) "
                                       "values ($1, $2, $3, $4, $5)", node_properties)
            return parent_row, child_row
        except asyncpg.exceptions.UniqueViolationError as f:
            raise DuplicateNodeError(f"{node.path} already exists.")
        except asyncpg.exceptions.PostgresSyntaxError as e:
            raise InvalidURI(f"{node.path} contains invalid characters.")

    async def insert_tree(self, root, conn):
        assert isinstance(root, ContainerNode)
        nodes = [node for node in Node.walk(root)]
        assert len(nodes) > 0
        # we only want to update its properties
        nodes.pop(0)
        nodes.sort()
        await self.update_properties(root, conn, root.owner, check_identity=False)

        if nodes:
            node_insert = []
            for node in nodes:
                path = NodeDatabase.path_to_ltree(node.path)
                node_row = [node.node_type, node.name, path, node.owner,
                            node.group_read, node.group_write, node.id, self.space_id]
                if isinstance(node, LinkNode):
                    node_row.append(node.target)
                else:
                    node_row.append(None)
                node_insert.append(node_row)

            await conn.executemany("INSERT INTO nodes (type, name, path, owner, groupread, groupwrite, "
                                   "id, space_id, link) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) "
                                   "on conflict (path, space_id) do nothing",
                                   node_insert)

            node_properties = []
            for node in nodes:
                for prop in node.properties:
                    prop_list = prop.tolist() + [NodeDatabase.path_to_ltree(node.path), self.space_id]
                    node_properties.append(prop_list)

            if node_properties:
                await conn.executemany("INSERT INTO properties (uri, value, read_only, node_path, space_id) "
                                       "VALUES ($1, $2, $3, $4, $5) on conflict (uri, node_path, space_id) "
                                       "do update set value=$2 where properties.value!=$2",
                                       node_properties)

    async def update_properties(self, node, conn, identity, check_identity=True):
        node_path_tree = NodeDatabase.path_to_ltree(node.path)

        results = await conn.fetchrow("select * from nodes where path=$1 "
                                      "and type=$2 and space_id=$3 for update",
                                      node_path_tree, node.node_type, self.space_id)
        if not results:
            raise NodeDoesNotExistError(f"{node.path} not found.")

        node.id = results['id']
        node.owner = results['owner']
        node.group_read = results['groupread']
        node.group_write = results['groupwrite']
        if check_identity:
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
        await conn.executemany("insert into properties (uri, value, read_only, node_path, space_id) "
                               "values ($1, $2, $3, $4, $5) on conflict (uri, node_path, space_id) "
                               "do update set value=$2 where properties.value!=$2",
                               node_props_insert)

        await conn.execute("delete from properties where uri=any($1::text[]) "
                           "and node_path=$2 and space_id=$3",
                           node_props_delete, node_path_tree, self.space_id)

        node_properties = await conn.fetch("select * from properties "
                                           "where node_path=$1 and space_id=$2",
                                           node_path_tree, self.space_id)
        node.set_properties(NodeDatabase._resultset_to_properties(node_properties) + pass_through_properties)
        return node

    async def delete(self, path, conn, identity):
        path_tree = NodeDatabase.path_to_ltree(path)
        results = await conn.fetch("with delete_rows as "
                                   "(delete from nodes where path <@ $1 and space_id=$2 returning *) "
                                   "select *, nlevel(delete_rows.path) from delete_rows "
                                   "order by nlevel(delete_rows.path) asc;",
                                   path_tree, self.space_id)
        if not results:
            raise NodeDoesNotExistError(f"{path} not found.")

        node = NodeDatabase.resultset_to_node_tree(results)

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