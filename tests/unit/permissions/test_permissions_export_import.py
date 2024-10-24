import os
import unittest

from permissions.permissions_exporter import PermissionsImportExport
from permissions.permissions_list import PermissionsList
from tests.unit.helpers.permissions_list_test_helpers import PermissionsListTestHelper

# pylint: disable=protected-access
class TestPermissionsExportImport(unittest.TestCase):
    '''
        Tests PermissionsExportImport class.
    '''

    @staticmethod
    def tearDownClass():
        #Delete test file if exists
        try:
            os.remove("test.csv")
        except:
            print("File failed to be removed.")

    def test_import_export(self):
        iePerms = PermissionsImportExport(args = {})

        permissionsList = PermissionsListTestHelper.create_permissions_list_with_glue_permissions_one_database_three_tables()

        iePerms._export_permissions(permissionsList, "test.csv")
        newPermsList = iePerms._import_permissions("test.csv")

        self.assertEqual(permissionsList.get_permissions().sort(), newPermsList.get_permissions().sort())

    def test_empty_import_export(self):
        iePerms = PermissionsImportExport(args = {})
        permissionsList = PermissionsList()
        iePerms._export_permissions(permissionsList, "test.csv")
        newPermsList = iePerms._import_permissions("test.csv")
        self.assertEqual(permissionsList.get_permissions().sort(), newPermsList.get_permissions().sort())

if __name__ == '__main__':
    unittest.main()