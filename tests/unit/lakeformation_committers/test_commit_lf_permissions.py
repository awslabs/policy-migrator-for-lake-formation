import unittest
from unittest.mock import Mock

from lakeformation_committers.commit_lake_formation_permissions import LakeFormationPermissionsCommitter
from permissions.permissions_list import PermissionsList
from permissions.permission_record import PermissionRecord


class TestLakeFormationPermissionsCommitter(unittest.TestCase):
    """Tests for bug #2 — the committer was calling non-existent methods
    like get_resource_arn(), get_principal_arn(), get_permissions() on
    PermissionRecord instead of resource_arn(), principal_arn(),
    permission_actions()."""

    def test_commit_catalog_permission(self):
        mock_session = Mock()
        mock_lf_client = Mock()
        mock_session.client.return_value = mock_lf_client

        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/myrole",
            "arn:aws:glue:us-east-1:123456789012:catalog",
            {"CREATE_DATABASE"},
        ))

        committer = LakeFormationPermissionsCommitter(mock_session)
        committer.commit_lakeformation_permissions(pl)

        mock_lf_client.grant_permissions.assert_called_once_with(
            Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789012:role/myrole'},
            Resource={'Catalog': {}},
            Permissions=['CREATE_DATABASE'],
        )

    def test_commit_database_permission(self):
        mock_session = Mock()
        mock_lf_client = Mock()
        mock_session.client.return_value = mock_lf_client

        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/myrole",
            "arn:aws:glue:us-east-1:123456789012:database/mydb",
            {"DESCRIBE"},
        ))

        committer = LakeFormationPermissionsCommitter(mock_session)
        committer.commit_lakeformation_permissions(pl)

        mock_lf_client.grant_permissions.assert_called_once_with(
            Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789012:role/myrole'},
            Resource={'Database': {'CatalogId': '123456789012', 'Name': 'mydb'}},
            Permissions=['DESCRIBE'],
        )

    def test_commit_table_permission(self):
        mock_session = Mock()
        mock_lf_client = Mock()
        mock_session.client.return_value = mock_lf_client

        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/myrole",
            "arn:aws:glue:us-east-1:123456789012:table/mydb/mytable",
            {"SELECT", "INSERT"},
        ))

        committer = LakeFormationPermissionsCommitter(mock_session)
        committer.commit_lakeformation_permissions(pl)

        mock_lf_client.grant_permissions.assert_called_once()
        call_kwargs = mock_lf_client.grant_permissions.call_args[1]
        self.assertEqual(call_kwargs['Principal']['DataLakePrincipalIdentifier'], 'arn:aws:iam::123456789012:role/myrole')
        self.assertEqual(call_kwargs['Resource']['Table']['CatalogId'], '123456789012')
        self.assertEqual(call_kwargs['Resource']['Table']['DatabaseName'], 'mydb')
        self.assertEqual(call_kwargs['Resource']['Table']['Name'], 'mytable')
        self.assertSetEqual(set(call_kwargs['Permissions']), {"SELECT", "INSERT"})

    def test_commit_skips_unknown_resource(self):
        mock_session = Mock()
        mock_lf_client = Mock()
        mock_session.client.return_value = mock_lf_client

        pl = PermissionsList()
        pl.add_permission_record(PermissionRecord(
            "arn:aws:iam::123456789012:role/myrole",
            "arn:aws:ec2:us-east-1:123456789012:instance/i-12345",
            {"DESCRIBE"},
        ))

        committer = LakeFormationPermissionsCommitter(mock_session)
        committer.commit_lakeformation_permissions(pl)

        mock_lf_client.grant_permissions.assert_not_called()


if __name__ == '__main__':
    unittest.main()
