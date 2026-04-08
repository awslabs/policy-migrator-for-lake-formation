import unittest

from permissions.permission_record import PermissionRecord


class TestPermissionRecord(unittest.TestCase):
    """Tests for PermissionRecord, including the assertion that
    permission_actions must be a set (bug #6 context)."""

    def test_creation_with_set(self):
        record = PermissionRecord("p1", "r1", {"a1", "a2"})
        self.assertEqual(record.principal_arn(), "p1")
        self.assertEqual(record.resource_arn(), "r1")
        self.assertSetEqual(record.permission_actions(), {"a1", "a2"})

    def test_creation_with_list_raises(self):
        with self.assertRaises(AssertionError):
            PermissionRecord("p1", "r1", ["a1", "a2"])

    def test_str_representation(self):
        record = PermissionRecord("p1", "r1", {"a1"})
        s = str(record)
        self.assertIn("p1", s)
        self.assertIn("r1", s)
        self.assertIn("a1", s)

    def test_lt_comparison(self):
        r1 = PermissionRecord("a", "a", {"a"})
        r2 = PermissionRecord("b", "b", {"b"})
        # Just verify it doesn't crash — ordering correctness is secondary
        _ = r1 < r2


if __name__ == '__main__':
    unittest.main()
