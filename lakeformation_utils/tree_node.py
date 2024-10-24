

class TreeNode:
    '''
    Class that represents a node in a tree.
    '''

    def __init__(self, path_val : any, parent_node = None):
        self._parent_node = parent_node
        self._path_val = path_val
        self._children = {}
        self._values = set()

    def add_child(self, val : any):
        if isinstance(val, TreeNode):
            val._parent_node = self
            self._children[val._path_val] = val
        else:
            node = TreeNode(val, self)
            self._children[val] = node

    def add_value(self, value):
        self._values.add(value)

    def has_value(self, value) -> bool:
        return value in self._values

    def has_values(self) -> bool:
        return len(self._values) > 0

    def get_values(self) -> set[str]:
        return self._values

    def get_children(self):
        return self._children.values()

    def get_path_to_self(self) -> str:
        return self._get_path_value()

    def _get_path_value(self):
        if self._parent_node is None:
            return self._path_val
        return self._parent_node._get_path_value() + "/" + self._path_val

    def __str__(self):
        return f"TreeNode(path_val={self._path_val}, values={[node for node in self._values]})"

    def __rshift__(self, other):
        ''' Right shift operator that will traverse its children and return its child. None otherwise. '''
        if other in self._children:
            return self._children[other]
        return None
