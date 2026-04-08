import unittest

from config.application_configuration import ApplicationConfiguration
from permissions.permissions_list import PermissionsList
from permissions.permission_record import PermissionRecord
from policy_filters.datazone_role_filter import FilterDataZoneRoles


class TestFilterDataZoneRoles(unittest.TestCase):
    """Tests for FilterDataZoneRoles policy filter."""

    def _make_filter(self):
        app_config = ApplicationConfiguration(args={})
        return FilterDataZoneRoles(app_config, {})

    def test_filters_datazone_role(self):
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/datazone-MyRole",
            "arn:aws:glue:us-east-1:123456789012:table/db/tbl",
            {"glue:GetTable"}
        ))

        f = self._make_filter()
        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 1)
        self.assertEqual(f.get_number_filtered(), 1)

    def test_keeps_non_datazone_role(self):
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/MyAnalyticsRole",
            "arn:aws:glue:us-east-1:123456789012:table/db/tbl",
            {"glue:GetTable"}
        ))

        f = self._make_filter()
        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 0)
        self.assertEqual(f.get_number_filtered(), 0)

    def test_filters_only_datazone_roles_in_mixed_list(self):
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/datazone-ProjectRole",
            "arn:aws:glue:us-east-1:123456789012:table/db/tbl1",
            {"glue:GetTable"}
        ))
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/MyRole",
            "arn:aws:glue:us-east-1:123456789012:table/db/tbl2",
            {"glue:GetTable"}
        ))
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/datazone-AnotherRole",
            "arn:aws:glue:us-east-1:123456789012:table/db/tbl3",
            {"glue:UpdateTable"}
        ))

        f = self._make_filter()
        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 2)
        self.assertEqual(f.get_number_filtered(), 2)

    def test_keeps_user_principals(self):
        """IAM users should never be filtered, even if username starts with datazone-."""
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:user/datazone-admin",
            "arn:aws:glue:us-east-1:123456789012:table/db/tbl",
            {"glue:GetTable"}
        ))

        f = self._make_filter()
        filtered = f.filter_policies(pl)

        # Not a role ARN, so _extract_role_name returns None -> not filtered
        self.assertEqual(filtered.get_permissions_count(), 0)

    def test_filters_datazone_role_with_path(self):
        """Roles with a path like /path/datazone-X should still be filtered."""
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/custom-path/datazone-ProjectRole",
            "arn:aws:glue:us-east-1:123456789012:table/db/tbl",
            {"glue:GetTable"}
        ))

        f = self._make_filter()
        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 1)

    def test_does_not_filter_role_containing_datazone_mid_name(self):
        """A role like 'my-datazone-role' should NOT be filtered — prefix must match."""
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/my-datazone-role",
            "arn:aws:glue:us-east-1:123456789012:table/db/tbl",
            {"glue:GetTable"}
        ))

        f = self._make_filter()
        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 0)

    def test_case_sensitive_prefix(self):
        """'DataZone-' (capital D) should NOT be filtered — prefix is case-sensitive."""
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/DataZone-MyRole",
            "arn:aws:glue:us-east-1:123456789012:table/db/tbl",
            {"glue:GetTable"}
        ))

        f = self._make_filter()
        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 0)

    def test_empty_permissions_list(self):
        pl = PermissionsList()

        f = self._make_filter()
        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 0)
        self.assertEqual(f.get_number_filtered(), 0)

    def test_multiple_permissions_same_datazone_role(self):
        """Multiple resources for the same datazone role should all be filtered."""
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/datazone-ProjectRole",
            "arn:aws:glue:us-east-1:123456789012:table/db/tbl1",
            {"glue:GetTable"}
        ))
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/datazone-ProjectRole",
            "arn:aws:glue:us-east-1:123456789012:table/db/tbl2",
            {"glue:UpdateTable"}
        ))

        f = self._make_filter()
        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 2)

    def test_classmethod_metadata(self):
        self.assertEqual(FilterDataZoneRoles.get_name(), "FilterDataZoneRoles")
        self.assertEqual(FilterDataZoneRoles.get_config_section(), "policy_filter_datazone_roles")
        self.assertEqual(FilterDataZoneRoles.get_required_configuration(), {})


if __name__ == '__main__':
    unittest.main()
