import unittest

from config.application_configuration import ApplicationConfiguration
from permissions.permissions_list import PermissionsList
from permissions.permission_record import PermissionRecord
from policy_filters.iam_filter_principals_by_list import IamFilterPrincipalsByList

ROLE_A = "arn:aws:iam::123456789012:role/RoleA"
ROLE_B = "arn:aws:iam::123456789012:role/RoleB"
ROLE_C = "arn:aws:iam::123456789012:role/RoleC"
USER_X = "arn:aws:iam::123456789012:user/UserX"
RESOURCE = "arn:aws:glue:us-east-1:123456789012:table/db/tbl"


def _make_filter(conf):
    app_config = ApplicationConfiguration(args={})
    return IamFilterPrincipalsByList(app_config, conf)


def _make_permissions(*principals):
    pl = PermissionsList()
    for i, principal in enumerate(principals):
        pl.add_permission_record(PermissionRecord(
            principal, f"{RESOURCE}{i}", {"glue:GetTable"}
        ))
    return pl


class TestIamFilterPrincipalsByListInclude(unittest.TestCase):
    """Tests for include_list mode."""

    def test_include_keeps_listed_principals(self):
        f = _make_filter({"include_list": f"{ROLE_A}, {ROLE_B}"})
        pl = _make_permissions(ROLE_A, ROLE_B, ROLE_C)

        filtered = f.filter_policies(pl)

        # ROLE_C should be filtered out
        self.assertEqual(filtered.get_permissions_count(), 1)
        self.assertEqual(f.get_number_filtered(), 1)

    def test_include_filters_all_when_none_match(self):
        f = _make_filter({"include_list": ROLE_A})
        pl = _make_permissions(ROLE_B, ROLE_C)

        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 2)

    def test_include_keeps_all_when_all_match(self):
        f = _make_filter({"include_list": f"{ROLE_A}, {ROLE_B}"})
        pl = _make_permissions(ROLE_A, ROLE_B)

        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 0)

    def test_include_single_principal(self):
        f = _make_filter({"include_list": ROLE_A})
        pl = _make_permissions(ROLE_A, ROLE_B)

        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 1)


class TestIamFilterPrincipalsByListExclude(unittest.TestCase):
    """Tests for exclude_list mode."""

    def test_exclude_filters_listed_principals(self):
        f = _make_filter({"exclude_list": ROLE_C})
        pl = _make_permissions(ROLE_A, ROLE_B, ROLE_C)

        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 1)
        self.assertEqual(f.get_number_filtered(), 1)

    def test_exclude_keeps_all_when_none_match(self):
        f = _make_filter({"exclude_list": "arn:aws:iam::123456789012:role/NonExistent"})
        pl = _make_permissions(ROLE_A, ROLE_B)

        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 0)

    def test_exclude_filters_all_when_all_match(self):
        f = _make_filter({"exclude_list": f"{ROLE_A}, {ROLE_B}"})
        pl = _make_permissions(ROLE_A, ROLE_B)

        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 2)

    def test_exclude_multiple_principals(self):
        f = _make_filter({"exclude_list": f"{ROLE_A}, {ROLE_C}"})
        pl = _make_permissions(ROLE_A, ROLE_B, ROLE_C, USER_X)

        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 2)
        self.assertEqual(f.get_number_filtered(), 2)


class TestIamFilterPrincipalsByListEdgeCases(unittest.TestCase):
    """Tests for edge cases and validation."""

    def test_both_lists_raises_error(self):
        with self.assertRaises(ValueError):
            _make_filter({"include_list": ROLE_A, "exclude_list": ROLE_B})

    def test_neither_list_does_nothing(self):
        f = _make_filter({})
        pl = _make_permissions(ROLE_A, ROLE_B)

        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 0)
        self.assertEqual(f.get_number_filtered(), 0)

    def test_empty_permissions_list(self):
        f = _make_filter({"include_list": ROLE_A})
        pl = PermissionsList()

        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 0)

    def test_whitespace_in_list_is_trimmed(self):
        f = _make_filter({"include_list": f"  {ROLE_A}  ,  {ROLE_B}  "})
        pl = _make_permissions(ROLE_A, ROLE_B, ROLE_C)

        filtered = f.filter_policies(pl)

        # Only ROLE_C filtered
        self.assertEqual(filtered.get_permissions_count(), 1)

    def test_newline_separated_list(self):
        f = _make_filter({"include_list": f"\n{ROLE_A}\n{ROLE_B}\n"})
        pl = _make_permissions(ROLE_A, ROLE_B, ROLE_C)

        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 1)

    def test_mixed_comma_and_newline_list(self):
        f = _make_filter({"exclude_list": f"{ROLE_A},\n{ROLE_B}\n{ROLE_C}"})
        pl = _make_permissions(ROLE_A, ROLE_B, ROLE_C, USER_X)

        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 3)
        self.assertEqual(f.get_number_filtered(), 3)

    def test_multiple_resources_same_excluded_principal(self):
        f = _make_filter({"exclude_list": ROLE_A})
        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(ROLE_A, f"{RESOURCE}1", {"glue:GetTable"}))
        pl.add_permission_record(PermissionRecord(ROLE_A, f"{RESOURCE}2", {"glue:UpdateTable"}))
        pl.add_permission_record(PermissionRecord(ROLE_B, f"{RESOURCE}3", {"glue:GetTable"}))

        filtered = f.filter_policies(pl)

        self.assertEqual(filtered.get_permissions_count(), 2)
        self.assertEqual(f.get_number_filtered(), 2)

    def test_classmethod_metadata(self):
        self.assertEqual(IamFilterPrincipalsByList.get_name(), "IamFilterPrincipalsByList")
        self.assertEqual(IamFilterPrincipalsByList.get_config_section(), "policy_filter_principals_by_list")
        self.assertIn("include_list", IamFilterPrincipalsByList.get_required_configuration())
        self.assertIn("exclude_list", IamFilterPrincipalsByList.get_required_configuration())


if __name__ == '__main__':
    unittest.main()
