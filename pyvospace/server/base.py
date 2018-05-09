class VOSpaceBase(object):
    async def move_storage_node(self, src_type, src_path, dest_type, dest_path):
        raise NotImplementedError()

    async def copy_storage_node(self, src_type, src_path, dest_type, dest_path):
        raise NotImplementedError()

    async def create_storage_node(self, node_type, node_path):
        raise NotImplementedError()

    async def delete_storage_node(self, node_type, node_path):
        raise NotImplementedError()

    async def get_storage_endpoints(self, conn, space_id, job_id, protocol, direction):
        raise NotImplementedError()