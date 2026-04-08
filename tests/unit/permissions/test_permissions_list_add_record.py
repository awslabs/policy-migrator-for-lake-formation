import unittest

from permissions.permissions_list import PermissionsList
from permissions.permission_record import PermissionRecord


class TestPermissionsListAddRecord(unittest.TestCase):
    """Tests for the add_permission_record method — specifically the bug where
    only the first action was ever checked and the method returned early."""

    def test_add_permission_record_adds_all_actions(self):
        pl = PermissionsList()
        record = PermissionRecord("p1", "r1", {"a1", "a2", "a3"})
        result = pl.add_permission_record(record)

        self.assertTrue(result)
        self.assertSetEqual(pl.get_permission_actions("p1", "r1"), {"a1", "a2", "a3"})

    def test_add_permission_record_merges_new_actions(self):
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord("p1", "r1", {"a1", "a2"}))
        result = pl.add_permission_record(PermissionRecord("p1", "r1", {"a2", "a3"}))

        self.assertTrue(result)
        self.assertSetEqual(pl.get_permission_actions("p1", "r1"), {"a1", "a2", "a3"})

    def test_add_permission_record_returns_false_when_all_exist(self):
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord("p1", "r1", {"a1", "a2"}))
        result = pl.add_permission_record(PermissionRecord("p1", "r1", {"a1", "a2"}))

        self.assertFalse(result)
        self.assertSetEqual(pl.get_permission_actions("p1", "r1"), {"a1", "a2"})

    def test_add_permission_record_increments_count_once_per_resource(self):
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord("p1", "r1", {"a1"}))
        pl.add_permission_record(PermissionRecord("p1", "r1", {"a2"}))
        pl.add_permission_record(PermissionRecord("p1", "r2", {"a1"}))

        self.assertEqual(pl.get_permissions_count(), 2)

    def test_add_permission_record_single_action(self):
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord("p1", "r1", {"a1"}))

        self.assertSetEqual(pl.get_permission_actions("p1", "r1"), {"a1"})

    def test_add_permissions_from_list_merges_correctly(self):
        pl1 = PermissionsList()
        pl1.add_permission_record(PermissionRecord("p1", "r1", {"a1"}))

        pl2 = PermissionsList()
        pl2.add_permission_record(PermissionRecord("p1", "r1", {"a2"}))
        pl2.add_permission_record(PermissionRecord("p1", "r2", {"a3"}))

        pl1.add_permissions_from_list(pl2)

        self.assertSetEqual(pl1.get_permission_actions("p1", "r1"), {"a1", "a2"})
        self.assertSetEqual(pl1.get_permission_actions("p1", "r2"), {"a3"})
        self.assertEqual(pl1.get_permissions_count(), 2)


if __name__ == '__main__':
    unittest.main()
