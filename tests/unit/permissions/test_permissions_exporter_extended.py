import os
import unittest

from permissions.permissions_exporter import PermissionsImportExport
from permissions.permissions_list import PermissionsList
from permissions.permission_record import PermissionRecord


class TestPermissionsImportExportEnabled(unittest.TestCase):
    """Tests for PermissionsImportExport with enabled=True covering the public API."""

    _TEST_DIR = "test_output"

    def setUp(self):
        os.makedirs(self._TEST_DIR, exist_ok=True)
        self._args = {
            "import_export": {
                "export_policy_readers_filename": f"{self._TEST_DIR}/policy_readers.csv",
                "import_policy_readers": "false",
                "export_filtered_permissions_filename": f"{self._TEST_DIR}/filtered.csv",
                "import_filtered_permissions": "false",
                "export_post_processed_permissions_filename": f"{self._TEST_DIR}/post_processed.csv",
                "import_post_processed_permissions": "false",
                "export_lf_permissions_filename": f"{self._TEST_DIR}/lf_perms.csv",
                "import_lf_permissions": "false",
            }
        }

    def tearDown(self):
        for f in os.listdir(self._TEST_DIR):
            os.remove(os.path.join(self._TEST_DIR, f))
        os.rmdir(self._TEST_DIR)

    def _make_permissions(self):
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord("p1", "r1", {"a1", "a2"}))
        pl.add_permission_record(PermissionRecord("p2", "r2", {"a3"}))
        return pl

    def test_createImportExport_with_section(self):
        ie = PermissionsImportExport.createImportExport(self._args)
        self.assertTrue(ie._enabled)

    def test_createImportExport_without_section(self):
        ie = PermissionsImportExport.createImportExport({})
        self.assertFalse(ie._enabled)

    def test_export_and_import_policy_readers(self):
        ie = PermissionsImportExport(self._args, True)
        pl = self._make_permissions()
        ie.export_policy_readers_output(pl)
        self.assertTrue(os.path.exists(f"{self._TEST_DIR}/policy_readers.csv"))

        # Import disabled by default
        self.assertIsNone(ie.import_policy_readers_input())

    def test_export_and_import_filtered_permissions(self):
        ie = PermissionsImportExport(self._args, True)
        pl = self._make_permissions()
        ie.export_filtered_permissions_output(pl)
        self.assertTrue(os.path.exists(f"{self._TEST_DIR}/filtered.csv"))
        self.assertIsNone(ie.import_filtered_permissions_input())

    def test_export_and_import_post_processed_permissions(self):
        ie = PermissionsImportExport(self._args, True)
        pl = self._make_permissions()
        ie.export_post_processed_permissions_output(pl)
        self.assertTrue(os.path.exists(f"{self._TEST_DIR}/post_processed.csv"))
        self.assertIsNone(ie.import_post_processed_permissions_input())

    def test_export_and_import_lf_permissions(self):
        ie = PermissionsImportExport(self._args, True)
        pl = self._make_permissions()
        ie.export_lf_permissions_output(pl)
        self.assertTrue(os.path.exists(f"{self._TEST_DIR}/lf_perms.csv"))
        self.assertIsNone(ie.import_lf_permissions_input())

    def test_import_with_enabled_flag_roundtrips(self):
        """Enable import, export then import, verify data matches."""
        args = {
            "import_export": {
                "export_policy_readers_filename": f"{self._TEST_DIR}/rt.csv",
                "import_policy_readers": "true",
                "export_filtered_permissions_filename": f"{self._TEST_DIR}/rt2.csv",
                "import_filtered_permissions": "false",
                "export_post_processed_permissions_filename": f"{self._TEST_DIR}/rt3.csv",
                "import_post_processed_permissions": "false",
                "export_lf_permissions_filename": f"{self._TEST_DIR}/rt4.csv",
                "import_lf_permissions": "false",
            }
        }
        ie = PermissionsImportExport(args, True)
        pl = self._make_permissions()
        ie.export_policy_readers_output(pl)

        imported = ie.import_policy_readers_input()
        self.assertIsNotNone(imported)
        self.assertEqual(imported.get_permissions_count(), 2)

    def test_import_nonexistent_file_returns_none(self):
        ie = PermissionsImportExport(self._args, True)
        result = ie._import_permissions(f"{self._TEST_DIR}/nonexistent.csv")
        self.assertIsNone(result)

    def test_disabled_export_does_nothing(self):
        ie = PermissionsImportExport({}, False)
        pl = self._make_permissions()
        # These should not raise or create files
        ie.export_policy_readers_output(pl)
        ie.export_filtered_permissions_output(pl)
        ie.export_post_processed_permissions_output(pl)
        ie.export_lf_permissions_output(pl)

    def test_disabled_import_returns_none(self):
        ie = PermissionsImportExport({}, False)
        self.assertIsNone(ie.import_policy_readers_input())
        self.assertIsNone(ie.import_filtered_permissions_input())
        self.assertIsNone(ie.import_post_processed_permissions_input())
        self.assertIsNone(ie.import_lf_permissions_input())


if __name__ == '__main__':
    unittest.main()
