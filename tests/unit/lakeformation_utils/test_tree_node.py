import unittest

from lakeformation_utils.tree_node import TreeNode


class TestTreeNode(unittest.TestCase):
    """Tests for TreeNode operations."""

    def test_add_child_as_tree_node(self):
        parent = TreeNode("root")
        child = TreeNode("child1")
        parent.add_child(child)
        self.assertEqual(parent >> "child1", child)

    def test_add_child_as_string(self):
        parent = TreeNode("root")
        parent.add_child("child1")
        result = parent >> "child1"
        self.assertIsNotNone(result)

    def test_rshift_returns_none_for_missing_child(self):
        node = TreeNode("root")
        self.assertIsNone(node >> "nonexistent")

    def test_add_and_check_values(self):
        node = TreeNode("root")
        node.add_value("v1")
        node.add_value("v2")
        self.assertTrue(node.has_value("v1"))
        self.assertTrue(node.has_values())
        self.assertFalse(node.has_value("v3"))
        self.assertSetEqual(node.get_values(), {"v1", "v2"})

    def test_has_values_empty(self):
        node = TreeNode("root")
        self.assertFalse(node.has_values())

    def test_get_children(self):
        parent = TreeNode("root")
        parent.add_child("a")
        parent.add_child("b")
        children = list(parent.get_children())
        self.assertEqual(len(children), 2)

    def test_get_path_to_self_root(self):
        node = TreeNode("root")
        self.assertEqual(node.get_path_to_self(), "root")

    def test_get_path_to_self_nested(self):
        root = TreeNode("s3:/")
        child = TreeNode("bucket", root)
        root.add_child(child)
        grandchild = TreeNode("path", child)
        child.add_child(grandchild)
        self.assertEqual(grandchild.get_path_to_self(), "s3://bucket/path")

    def test_str_representation(self):
        node = TreeNode("test")
        node.add_value("val")
        s = str(node)
        self.assertIn("test", s)
        self.assertIn("val", s)


if __name__ == '__main__':
    unittest.main()
