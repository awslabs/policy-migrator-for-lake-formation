import unittest

from permissions.permissions_list import PermissionsList
from permissions.permission_record import PermissionRecord

class TestPermissionsList(unittest.TestCase):
    def test_permissions_list_adding_permissions(self):
        permissionsList = PermissionsList()
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable")

        self.assertSetEqual(permissionsList.get_permission_actions("principal1", "resource1"), {"glue:GetTable"})

        permissionsList.add_permission("principal1", "resource1", "glue:GetTable2")

        self.assertSetEqual(permissionsList.get_permission_actions("principal1", "resource1"), {"glue:GetTable", "glue:GetTable2"})

        permissionsList.add_permission("principal2", "resource1", "glue:GetTable")
        permissionsList.add_permission("principal2", "resource1", "glue:GetTable2")
        permissionsList.add_permission("principal2", "resource1", "glue:GetTable3")

        self.assertSetEqual(permissionsList.get_permission_actions("principal2", "resource1"), {"glue:GetTable", "glue:GetTable2", "glue:GetTable3"})

    def test_permissions_adding_duplicate_permission(self):
        permissionsList = PermissionsList()
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable")
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable")

        self.assertSetEqual(permissionsList.get_permission_actions("principal1", "resource1"), {"glue:GetTable"})

    def test_permissions_list_get_permission_actions(self):
        permissionsList = PermissionsList()
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable")
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable2")
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable3")
        self.assertSetEqual(permissionsList.get_permission_actions("principal1", "resource1"), {"glue:GetTable", "glue:GetTable2", "glue:GetTable3"})

    def test_permissions_list_delete_permission(self):
        permissionsList = PermissionsList()
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable")
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable2")
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable3")

        permissionsList.add_permission("principal2", "resource1", "glue:GetTable")
        permissionsList.add_permission("principal2", "resource1", "glue:GetTable2")
        permissionsList.add_permission("principal2", "resource1", "glue:GetTable3")

        permissionsList.add_permission("principal3", "resource1", "glue:GetTable")

        self.assertSetEqual(permissionsList.get_permission_actions("principal1", "resource1"), {"glue:GetTable", "glue:GetTable2", "glue:GetTable3"})

        permissionsList.delete_permission("principal1", "resource1")
        self.assertEqual(permissionsList.get_permission_actions("principal1", "resource1"), None)

    def test_permissions_list_delete_permission_not_existing(self):
        permissionsList = PermissionsList()
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable")
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable2")

        permissionsList.delete_permission("principal1", "resource2")
        #shouldn't throw an exception

    def test_permissions_list_remove_permissions(self):
        permissionsList = PermissionsList()
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable")
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable2")
        permissionsList.add_permission("principal1", "resource1", "glue:GetTable3")

        permissionsList.add_permission("principal2", "resource1", "glue:GetTable")
        permissionsList.add_permission("principal2", "resource1", "glue:GetTable2")
        permissionsList.add_permission("principal2", "resource1", "glue:GetTable3")

        permissionsList.add_permission("principal3", "resource1", "glue:GetTable")

        permissionsToRemove = PermissionsList()
        #test subset of permissions
        permissionsToRemove.add_permission("principal1", "resource1", "glue:GetTable")
        permissionsToRemove.add_permission("principal1", "resource1", "glue:GetTable2")

        #test all permissions for user
        permissionsToRemove.add_permission("principal2", "resource1", "glue:GetTable")
        permissionsToRemove.add_permission("principal2", "resource1", "glue:GetTable2")
        permissionsToRemove.add_permission("principal2", "resource1", "glue:GetTable3")

        #test all permissions and one that doesn't exist
        permissionsToRemove.add_permission("principal3", "resource1", "glue:GetTable")
        permissionsToRemove.add_permission("principal3", "resource1", "glue:GetTable2")

        #test for prinicpal that doesn't exist
        permissionsToRemove.add_permission("principal4", "resource1", "glue:GetTable")

        permissionsList.remove_permissions(permissionsToRemove)

        self.assertListEqual(permissionsList.get_principal_arns(), ["principal1"])

        permissionsForPrincipal1 = list(permissionsList.get_permissions_for_principal("principal1"))
        self.assertEqual(len(permissionsForPrincipal1), 1)
        self.assertSetEqual(permissionsForPrincipal1[0].permission_actions(), {"glue:GetTable3"})

        permissionsForPrincipal2 = list(permissionsList.get_permissions_for_principal("principal2"))
        self.assertListEqual(permissionsForPrincipal2, [])

        permissionsForPrincipal3 = list(permissionsList.get_permissions_for_principal("principal3"))
        self.assertListEqual(permissionsForPrincipal3, [])

    def test_permissions_list_iteration(self):
        permissionsList = PermissionsList()

        permissionRecords = set()

        permissionRecords.add(PermissionRecord("principal1", "resource1", {"glue:GetTable", "glue:GetTable2", "glue:GetTable3"}))
        permissionRecords.add(PermissionRecord("principal2", "resource2", {"glue:GetTable", "glue:GetTable2", "glue:GetTable3"}))
        permissionRecords.add(PermissionRecord("principal2", "resource3", {"glue:GetTable", "glue:GetTable3"}))

        for permissionRecord in permissionRecords:
            permissionsList.add_permission_record(permissionRecord)

        permissionRecordsFromList = list(iter(permissionsList))
        self.assertTrue(len(permissionRecordsFromList) == 3)

if __name__ == '__main__':
    unittest.main()
