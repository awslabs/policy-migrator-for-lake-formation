from .tree_node import TreeNode
import logging

logger = logging.getLogger(__name__)

class S3Tree:
    '''
    A tree that stores S3 paths. Each bucket and "directory" in S3 are stored as nodes
    so that we can easily navigate the tree.
    '''

    def __init__(self):
        # We only have one slash becasue our tree is delimited by a '/'
        self._root = TreeNode("s3:/")

    def add_path(self, s3_path : str, value = None):
        '''
        Adds a path to the tree and returns the last node in the tree.
        If the path already exists, it returns the existing node.
        '''
        current_node, s3_paths = self._sanitize_s3_path(s3_path)

        while len(s3_paths) > 0:
            next_node = current_node >> s3_paths[0]
            if next_node is None:
                next_node = TreeNode(s3_paths[0], current_node)
                current_node.add_child(next_node)

            current_node = next_node
            s3_paths.pop(0)

        if value is not None:
            current_node.add_value(value)

        return current_node

    def get_last_node_from_path(self, s3_path : str) -> TreeNode:
        '''
        Gets the last valid node in the tree from the given s3 path. 
        If the path doesn't exist, it returns the last node that exists in the tree.
        '''
        current_node, s3_paths = self._sanitize_s3_path(s3_path=s3_path, create_bucket_if_not_exists = False)
        logger.debug(f"get_last_node_from_path: s3_path: {s3_path} current_node = {current_node} s3_paths = {s3_paths}")

        if current_node is None:
            return None

        while len(s3_paths) > 0:
            next_value = s3_paths.pop(0)
            next_node = current_node >> next_value
            if next_node is None:
                return current_node
            current_node = next_node

        return current_node

    def get_node_from_path(self, s3_path : str) -> TreeNode | None:
        '''
        Gets the node that is represented by the path. If the path does not exist, it will return None
        '''
        current_node, s3_paths = self._sanitize_s3_path(s3_path=s3_path, create_bucket_if_not_exists = False)
        logger.debug(f"get_node_from_path: s3_path: {s3_path} current_node = {current_node} s3_paths = {s3_paths}")

        if current_node is None:
            return None

        while len(s3_paths) > 0:
            next_value = s3_paths.pop(0)
            next_node = current_node >> next_value
            if next_node is None:
                return None
            current_node = next_node

        return current_node

    def get_all_subtree_values_from_path(self, s3_path : str) -> list[str]:
        try:
            pathNode = self.get_node_from_path(s3_path)
            if pathNode is None:
                logger.debug(f"PathNode for s3_path {s3_path} doesn't exist. Ignoring.")
                return []
            return self.__get_all_values_from_node(pathNode)
        except Exception as e:
            logger.error(f"Error getting all values from path {s3_path}: {e}")
            raise e

    def __get_all_values_from_node(self, node) -> list[str]:
        values = []
        for child in node.get_children():
            values.extend(self.__get_all_values_from_node(child))
        values.extend(node.get_values())
        return values

    def _sanitize_s3_path(self, s3_path : str, create_bucket_if_not_exists = True) -> tuple[str, str]:
        if s3_path is None:
            return None, None

        if s3_path.startswith("s3://"):
            s3_path = s3_path[5:]

        if s3_path == "":
            return self._root, []

        if s3_path.endswith("/"):
            s3_path = s3_path[:-1]
        elif "/" in s3_path:
            s3_path = s3_path[:s3_path.rindex('/')]

        s3_paths = s3_path.split("/")

        bucket = s3_paths.pop(0)
        bucket_node = self._root >> bucket
        if create_bucket_if_not_exists and bucket_node is None:
            bucket_node = TreeNode(bucket)
            self._root.add_child(bucket_node)

        return bucket_node, s3_paths
