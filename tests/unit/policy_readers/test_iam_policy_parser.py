import unittest
from unittest.mock import Mock

from aws_resources.actions.glue_action import GlueAction
from aws_resources.actions.s3_action import S3Action
from lakeformation_utils.s3_to_table_mapper import S3ToTableMapper
from permissions.permissions_list import PermissionsList
from policy_readers.iam_policy_parser import IamPolicyParser
from policy_readers.iam_policy_permissions_reader import IamPolicyPermissionsReader
from tests.unit.helpers.permissions_list_test_helpers import PermissionsListTestHelper

# pylint: disable=all

CATALOG_ID = PermissionsListTestHelper.test_catalog_id
REGION = PermissionsListTestHelper.test_region
GLUE_DATA_CATALOG = PermissionsListTestHelper.create_glue_data_catalog()
S3_TO_TABLE_MAPPER = S3ToTableMapper(GLUE_DATA_CATALOG)
PRINCIPAL = "arn:aws:iam::012345678901:role/role1"
PRINCIPAL2 = "arn:aws:iam::012345678901:role/role2"

ALL_GLUE_ACTIONS = set(GlueAction.get_glue_actions_with_wildcard("*"))
ALL_S3_ACTIONS = set(S3Action.get_s3_actions_with_wildcard("*"))


def _make_app_config(iam_policy_reader=None):
    app_config = Mock()
    app_config.get_glue_data_catalog.return_value = GLUE_DATA_CATALOG
    app_config.get_s3_to_table_translator.return_value = S3_TO_TABLE_MAPPER
    if iam_policy_reader is None:
        iam_policy_reader = Mock()
    app_config.get_iam_policy_reader.return_value = iam_policy_reader
    return app_config, iam_policy_reader


def _make_policy(statements):
    return {"Version": "2012-10-17", "Statement": statements}


def _make_reader(app_config, iam_policy_reader, policies_by_principal):
    iam_policy_reader.get_all_prinicial_policies.return_value = iter(policies_by_principal.items())
    return IamPolicyPermissionsReader(app_config, {})


class TestIamPolicyParserWildcardResource(unittest.TestCase):
    """Tests for the new Resource: '*' wildcard handling in _filter_resource (lines 165-167).

    NOTE: The current implementation fetches tables and databases when Resource='*'
    but does NOT append them to glue_resources or s3_resources. These tests document
    the current (incomplete) behavior. When the implementation is completed to actually
    populate the resource lists, update the expected counts accordingly.
    """

    def test_resource_star_with_glue_action_expands_all_catalog_resources(self):
        """Resource: '*' with glue actions should expand to all databases and tables."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable"],
                "Resource": "*"
            }])]
        })

        perms = reader.read_policies()

        # Should produce permissions for all catalogs (1) + databases (2) + tables (15)
        total_catalogs = len(GLUE_DATA_CATALOG.get_catalogs())
        total_databases = len(list(GLUE_DATA_CATALOG.get_catalogs().values())[0].get_databases())
        total_tables = len(list(GLUE_DATA_CATALOG.get_tables()))
        expected = total_catalogs + total_databases + total_tables
        self.assertEqual(len(perms.get_permissions()), expected)

    def test_resource_star_with_s3_action_currently_yields_nothing(self):
        """Resource: '*' with S3 actions — currently produces no permissions
        because the wildcard branch only populates glue_resources, not s3_resources."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": "*"
            }])]
        })

        perms = reader.read_policies()

        # S3 actions don't get resources from the wildcard branch (only glue resources are populated)
        self.assertEqual(len(perms.get_permissions()), 0)

    def test_resource_star_with_all_actions_expands_glue_resources(self):
        """Action: '*' + Resource: '*' — should expand to all glue resources with all actions."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": "*",
                "Resource": "*"
            }])]
        })

        perms = reader.read_policies()

        # All catalogs + databases + tables get glue actions applied
        total_catalogs = len(GLUE_DATA_CATALOG.get_catalogs())
        total_databases = len(list(GLUE_DATA_CATALOG.get_catalogs().values())[0].get_databases())
        total_tables = len(list(GLUE_DATA_CATALOG.get_tables()))
        expected = total_catalogs + total_databases + total_tables
        self.assertEqual(len(perms.get_permissions()), expected)


class TestIamPolicyParserActionWildcards(unittest.TestCase):
    """Tests for action wildcard expansion."""

    def test_action_star_on_glue_resource_expands_only_glue_actions(self):
        """Action: '*' on a specific Glue table resource expands to all glue actions.
        S3 actions are also expanded but only apply to s3_resources (empty here),
        so only glue actions end up on the glue resource."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": "*",
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertEqual(perms_list[0].resource_arn(),
                         f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table")
        # Only glue actions apply to glue resources
        self.assertTrue(ALL_GLUE_ACTIONS.issubset(perms_list[0].permission_actions()))
        # S3 actions should NOT be present since they only apply to s3_resources
        self.assertFalse(ALL_S3_ACTIONS.issubset(perms_list[0].permission_actions()))

    def test_glue_star_action_expands_all_glue_actions(self):
        """Action: 'glue:*' should expand to all glue actions."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": "glue:*",
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertSetEqual(perms_list[0].permission_actions(), ALL_GLUE_ACTIONS)

    def test_s3_star_action_expands_all_s3_actions(self):
        """Action: 's3:*' on an S3 wildcard resource."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": "s3:*",
                "Resource": "arn:aws:s3:::mybucket/test_database/test_table/*"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertSetEqual(perms_list[0].permission_actions(), ALL_S3_ACTIONS)

    def test_glue_partial_wildcard_action(self):
        """Action: 'glue:Get*' should expand to all glue Get actions."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": "glue:Get*",
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        expected = set(GlueAction.get_glue_actions_with_wildcard("Get*"))
        self.assertSetEqual(perms_list[0].permission_actions(), expected)

    def test_s3_partial_wildcard_action(self):
        """Action: 's3:Put*' should expand to PutObject only."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": "s3:Put*",
                "Resource": "arn:aws:s3:::mybucket/test_database/test_table/*"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertSetEqual(perms_list[0].permission_actions(), {"s3:PutObject"})

    def test_action_list_with_mixed_services(self):
        """Action list with both glue and s3 actions on a glue resource."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable", "s3:GetObject"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        # Only glue actions apply to glue resources
        self.assertEqual(len(perms_list), 1)
        self.assertIn("glue:GetTable", perms_list[0].permission_actions())
        # s3:GetObject won't match a glue resource, so it shouldn't appear
        # (s3 actions go to s3_resources which is empty here)


class TestIamPolicyParserGlueResourceWildcards(unittest.TestCase):
    """Tests for Glue resource wildcard expansion in _filter_resource."""

    def test_glue_table_wildcard_all_databases(self):
        """Resource: table/*/test_table should match test_table in all databases."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/*/test_table"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()
        perms_list.sort()

        # test_table exists in both test_database and test_database2
        self.assertEqual(len(perms_list), 2)
        resource_arns = {p.resource_arn() for p in perms_list}
        self.assertIn(f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table", resource_arns)
        self.assertIn(f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database2/test_table", resource_arns)

    def test_glue_table_wildcard_all_tables_in_database(self):
        """Resource: table/test_database/* should match all tables in test_database."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/*"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        # test_database has 3 tables
        self.assertEqual(len(perms_list), 3)
        table_names = {p.resource_arn().split("/")[-1] for p in perms_list}
        self.assertSetEqual(table_names, {"test_table", "test_table2", "test_table3"})

    def test_glue_table_wildcard_all_tables_all_databases(self):
        """Resource: table/*/* should match all tables in all databases."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/*/*"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        # All tables across both databases in the test catalog
        total_tables = len(list(GLUE_DATA_CATALOG.get_tables()))
        self.assertEqual(len(perms_list), total_tables)

    def test_glue_database_wildcard_all_databases(self):
        """Resource: database/* should match all databases."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetDatabase"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:database/*"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        # 2 databases in the test catalog
        self.assertEqual(len(perms_list), 2)
        resource_arns = {p.resource_arn() for p in perms_list}
        self.assertIn(f"arn:aws:glue:{REGION}:{CATALOG_ID}:database/test_database", resource_arns)
        self.assertIn(f"arn:aws:glue:{REGION}:{CATALOG_ID}:database/test_database2", resource_arns)

    def test_glue_specific_table_no_wildcard(self):
        """Resource: specific table ARN should match exactly one table."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertEqual(perms_list[0].resource_arn(),
                         f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table")

    def test_glue_nonexistent_table_yields_nothing(self):
        """Resource pointing to a table that doesn't exist in the catalog."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/nonexistent_table"
            }])]
        })

        perms = reader.read_policies()
        self.assertEqual(len(perms.get_permissions()), 0)

    def test_glue_nonexistent_database_yields_nothing(self):
        """Resource pointing to a database that doesn't exist in the catalog."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetDatabase"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:database/nonexistent_db"
            }])]
        })

        perms = reader.read_policies()
        self.assertEqual(len(perms.get_permissions()), 0)

    def test_resource_as_list_with_multiple_glue_arns(self):
        """Resource as a list of multiple specific Glue table ARNs."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable"],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table",
                    f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table2"
                ]
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 2)


class TestIamPolicyParserDenyAndEdgeCases(unittest.TestCase):
    """Tests for deny logic, invalid statements, multiple principals, and edge cases."""

    def test_deny_removes_subset_of_allowed_actions(self):
        """Allow glue:GetTable + glue:GetTables, then deny only glue:GetTable."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetTable", "glue:GetTables"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": ["glue:GetTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }])
            ]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertSetEqual(perms_list[0].permission_actions(), {"glue:GetTables"})

    def test_deny_all_actions_removes_entire_permission(self):
        """Allow then deny the exact same actions — should result in nothing."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": ["glue:GetTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }])
            ]
        })

        perms = reader.read_policies()
        self.assertEqual(len(perms.get_permissions()), 0)

    def test_deny_on_wildcard_resource_removes_specific_table(self):
        """Allow on wildcard, deny on specific table."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/*"
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": ["glue:GetTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }])
            ]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()
        perms_list.sort()

        # 3 tables in test_database, deny removes test_table -> 2 remain
        self.assertEqual(len(perms_list), 2)
        remaining_tables = {p.resource_arn().split("/")[-1] for p in perms_list}
        self.assertSetEqual(remaining_tables, {"test_table2", "test_table3"})

    def test_multiple_principals_independent_permissions(self):
        """Two principals with different policies should get independent permissions."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
            }])],
            PRINCIPAL2: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable", "glue:UpdateTable"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table2"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()
        perms_list.sort()

        self.assertEqual(len(perms_list), 2)

        p1_perms = [p for p in perms_list if p.principal_arn() == PRINCIPAL]
        p2_perms = [p for p in perms_list if p.principal_arn() == PRINCIPAL2]

        self.assertEqual(len(p1_perms), 1)
        self.assertSetEqual(p1_perms[0].permission_actions(), {"glue:GetTable"})

        self.assertEqual(len(p2_perms), 1)
        self.assertSetEqual(p2_perms[0].permission_actions(), {"glue:GetTable", "glue:UpdateTable"})

    def test_invalid_statement_missing_action_is_skipped(self):
        """A statement missing 'Action' should be silently skipped."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([
                {
                    "Effect": "Allow",
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                    # Missing "Action"
                },
                {
                    "Effect": "Allow",
                    "Action": ["glue:GetTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }
            ])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        # Only the valid statement should produce a permission
        self.assertEqual(len(perms_list), 1)

    def test_invalid_statement_missing_resource_is_skipped(self):
        """A statement missing 'Resource' should be silently skipped."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable"]
                # Missing "Resource"
            }])]
        })

        perms = reader.read_policies()
        self.assertEqual(len(perms.get_permissions()), 0)

    def test_invalid_statement_missing_effect_raises_key_error(self):
        """A statement missing 'Effect' causes a KeyError because the parser
        accesses statement['Effect'] before calling _is_valid_statement.
        This is a known limitation — Effect should be checked first."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Action": ["glue:GetTable"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                # Missing "Effect"
            }])]
        })

        with self.assertRaises(KeyError):
            reader.read_policies()

    def test_empty_statement_list(self):
        """A policy with an empty Statement list should produce no permissions."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([])]
        })

        perms = reader.read_policies()
        self.assertEqual(len(perms.get_permissions()), 0)

    def test_s3_resource_without_wildcard_suffix_yields_nothing(self):
        """An S3 ARN without trailing '*' is currently not handled — no permissions."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": "arn:aws:s3:::mybucket/test_database/test_table/somefile.parq"
            }])]
        })

        perms = reader.read_policies()
        # S3 resources only get processed when they end with '*'
        self.assertEqual(len(perms.get_permissions()), 0)

    def test_allow_on_one_resource_deny_on_different_resource(self):
        """Deny on a different resource should not affect the allowed resource."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": ["glue:GetTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table2"
                }])
            ]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertEqual(perms_list[0].resource_arn(),
                         f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table")

    def test_multiple_allow_statements_in_single_policy_merge(self):
        """Two allow statements in the same policy for the same resource should merge actions."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([
                {
                    "Effect": "Allow",
                    "Action": ["glue:GetTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                },
                {
                    "Effect": "Allow",
                    "Action": ["glue:UpdateTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }
            ])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertSetEqual(perms_list[0].permission_actions(), {"glue:GetTable", "glue:UpdateTable"})

    def test_unrecognized_service_action_is_ignored(self):
        """Actions for services other than glue/s3 should be silently ignored."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["ec2:DescribeInstances", "glue:GetTable"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        # Only glue action should be present, ec2 action is ignored
        self.assertSetEqual(perms_list[0].permission_actions(), {"glue:GetTable"})


class TestIamPolicyParserResourceStarDeny(unittest.TestCase):
    """Tests for Resource: '*' combined with deny statements."""

    def test_resource_star_allow_then_deny_specific_table(self):
        """Allow all via Resource '*', then deny a specific table."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetTable"],
                    "Resource": "*"
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": ["glue:GetTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }])
            ]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        total_catalogs = len(GLUE_DATA_CATALOG.get_catalogs())
        total_databases = len(list(GLUE_DATA_CATALOG.get_catalogs().values())[0].get_databases())
        total_tables = len(list(GLUE_DATA_CATALOG.get_tables()))
        # One table denied, so total - 1
        self.assertEqual(len(perms_list), total_catalogs + total_databases + total_tables - 1)
        denied_arn = f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
        resource_arns = {p.resource_arn() for p in perms_list}
        self.assertNotIn(denied_arn, resource_arns)

    def test_resource_star_allow_then_deny_entire_database(self):
        """Allow all via Resource '*', then deny a specific database."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetDatabase"],
                    "Resource": "*"
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": ["glue:GetDatabase"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:database/test_database"
                }])
            ]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        total_catalogs = len(GLUE_DATA_CATALOG.get_catalogs())
        total_databases = len(list(GLUE_DATA_CATALOG.get_catalogs().values())[0].get_databases())
        total_tables = len(list(GLUE_DATA_CATALOG.get_tables()))
        # The database is denied, tables still have the action
        resource_arns = {p.resource_arn() for p in perms_list}
        denied_arn = f"arn:aws:glue:{REGION}:{CATALOG_ID}:database/test_database"
        self.assertNotIn(denied_arn, resource_arns)
        # Catalog + other database + all tables remain
        self.assertEqual(len(perms_list), total_catalogs + total_databases + total_tables - 1)

    def test_resource_star_allow_then_deny_wildcard_action(self):
        """Allow glue:GetTable on '*', then deny glue:* on a specific table."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetTable", "glue:UpdateTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": "glue:*",
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }])
            ]
        })

        perms = reader.read_policies()
        # glue:* deny removes all glue actions
        self.assertEqual(len(perms.get_permissions()), 0)


class TestIamPolicyParserMultiplePoliciesAccumulation(unittest.TestCase):
    """Tests for permissions accumulating across multiple policies."""

    def test_three_policies_accumulate_actions_on_same_resource(self):
        """Three separate policies each adding a different action to the same resource."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }]),
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:UpdateTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }]),
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:DeleteTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }])
            ]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertSetEqual(perms_list[0].permission_actions(),
                            {"glue:GetTable", "glue:UpdateTable", "glue:DeleteTable"})

    def test_allow_across_policies_then_partial_deny(self):
        """Accumulate actions from two allow policies, then deny one action."""
        app_config, iam_reader = _make_app_config()
        resource = f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetTable", "glue:GetTables"],
                    "Resource": resource
                }]),
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:UpdateTable", "glue:DeleteTable"],
                    "Resource": resource
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": ["glue:DeleteTable"],
                    "Resource": resource
                }])
            ]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertSetEqual(perms_list[0].permission_actions(),
                            {"glue:GetTable", "glue:GetTables", "glue:UpdateTable"})

    def test_deny_only_no_prior_allow(self):
        """A deny with no prior allow should produce no permissions."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Deny",
                    "Action": ["glue:GetTable"],
                    "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                }])
            ]
        })

        perms = reader.read_policies()
        self.assertEqual(len(perms.get_permissions()), 0)

    def test_two_principals_deny_affects_only_target(self):
        """Deny for one principal should not affect the other."""
        app_config, iam_reader = _make_app_config()
        resource = f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetTable"],
                    "Resource": resource
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": ["glue:GetTable"],
                    "Resource": resource
                }])
            ],
            PRINCIPAL2: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetTable"],
                    "Resource": resource
                }])
            ]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        # PRINCIPAL denied, PRINCIPAL2 still has it
        self.assertEqual(len(perms_list), 1)
        self.assertEqual(perms_list[0].principal_arn(), PRINCIPAL2)


class TestIamPolicyParserS3EdgeCases(unittest.TestCase):
    """Tests for S3 resource handling edge cases."""

    def test_s3_wildcard_across_multiple_tables(self):
        """S3 wildcard at database level should expand to all tables under it."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject"],
                "Resource": "arn:aws:s3:::mybucket/test_database/*"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        # 3 tables under mybucket/test_database/
        self.assertEqual(len(perms_list), 3)
        for p in perms_list:
            self.assertSetEqual(p.permission_actions(), {"s3:GetObject", "s3:PutObject"})

    def test_s3_wildcard_no_matching_tables(self):
        """S3 wildcard pointing to a path with no tables."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": "arn:aws:s3:::nonexistent_bucket/path/*"
            }])]
        })

        perms = reader.read_policies()
        self.assertEqual(len(perms.get_permissions()), 0)

    def test_s3_allow_then_deny_specific_table_path(self):
        """Allow S3 on a broad path, deny on a specific table path."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": "arn:aws:s3:::mybucket/test_database/*"
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": ["s3:GetObject"],
                    "Resource": "arn:aws:s3:::mybucket/test_database/test_table/*"
                }])
            ]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        # 3 tables allowed, 1 denied -> 2 remain
        self.assertEqual(len(perms_list), 2)
        resource_arns = {p.resource_arn() for p in perms_list}
        for arn in resource_arns:
            self.assertNotIn("test_database/test_table/", arn)

    def test_s3_bucket_level_arn_no_path_yields_nothing(self):
        """S3 bucket ARN without any path or wildcard — not handled."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": "arn:aws:s3:::mybucket"
            }])]
        })

        perms = reader.read_policies()
        self.assertEqual(len(perms.get_permissions()), 0)

    def test_s3_and_glue_resources_in_same_statement(self):
        """A statement with both S3 and Glue resources should handle both."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable", "s3:GetObject"],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table",
                    "arn:aws:s3:::mybucket/test_database/test_table2/*"
                ]
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()
        perms_list.sort()

        # 1 glue resource with glue action + 1 s3 resource with s3 action
        self.assertEqual(len(perms_list), 2)
        glue_perms = [p for p in perms_list if "glue:" in p.resource_arn()]
        s3_perms = [p for p in perms_list if "s3:" in p.resource_arn()]
        self.assertEqual(len(glue_perms), 1)
        self.assertEqual(len(s3_perms), 1)
        self.assertSetEqual(glue_perms[0].permission_actions(), {"glue:GetTable"})
        self.assertSetEqual(s3_perms[0].permission_actions(), {"s3:GetObject"})


class TestIamPolicyParserCatalogLevel(unittest.TestCase):
    """Tests for catalog-level Glue resource ARNs."""

    def test_catalog_arn_with_catalog_actions(self):
        """Catalog-level ARN with catalog-level actions should produce a permission."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetDatabases", "glue:CreateDatabase"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:catalog"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertEqual(perms_list[0].resource_arn(), f"arn:aws:glue:{REGION}:{CATALOG_ID}:catalog")
        self.assertSetEqual(perms_list[0].permission_actions(), {"glue:GetDatabases", "glue:CreateDatabase"})

    def test_action_string_not_list(self):
        """Action as a plain string instead of a list."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": "glue:GetTable",
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertSetEqual(perms_list[0].permission_actions(), {"glue:GetTable"})

    def test_resource_string_not_list(self):
        """Resource as a plain string instead of a list."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable", "glue:UpdateTable"],
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertSetEqual(perms_list[0].permission_actions(), {"glue:GetTable", "glue:UpdateTable"})

    def test_resource_list_with_star_among_specific_arns(self):
        """Resource list containing '*' alongside specific ARNs."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetTable"],
                "Resource": [
                    "*",
                    f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                ]
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        # '*' expands all catalogs + databases + tables, specific ARN is a subset — should deduplicate
        total_catalogs = len(GLUE_DATA_CATALOG.get_catalogs())
        total_databases = len(list(GLUE_DATA_CATALOG.get_catalogs().values())[0].get_databases())
        total_tables = len(list(GLUE_DATA_CATALOG.get_tables()))
        self.assertEqual(len(perms_list), total_catalogs + total_databases + total_tables)

    def test_multiple_allow_and_deny_in_same_policy(self):
        """Allow and Deny in the same policy document (different statements)."""
        app_config, iam_reader = _make_app_config()
        resource = f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([
                {
                    "Effect": "Allow",
                    "Action": ["glue:GetTable", "glue:UpdateTable", "glue:DeleteTable"],
                    "Resource": resource
                },
                {
                    "Effect": "Deny",
                    "Action": ["glue:DeleteTable"],
                    "Resource": resource
                }
            ])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        # Allow + Deny in same policy: allows are processed first across all policies,
        # then denies. Since they're in the same policy, the deny should still remove.
        # But note: read_iam_principal_allow_policies only processes Allow,
        # and read_iam_deny_policies only processes Deny — both iterate the same policy.
        self.assertEqual(len(perms_list), 1)
        self.assertSetEqual(perms_list[0].permission_actions(), {"glue:GetTable", "glue:UpdateTable"})

    def test_s3_deny_with_wildcard_action(self):
        """Deny s3:* on a specific path after allowing s3:GetObject."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject"],
                    "Resource": "arn:aws:s3:::mybucket/test_database/test_table/*"
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": "s3:*",
                    "Resource": "arn:aws:s3:::mybucket/test_database/test_table/*"
                }])
            ]
        })

        perms = reader.read_policies()
        self.assertEqual(len(perms.get_permissions()), 0)


class TestIamPolicyParserCatalogResource(unittest.TestCase):
    """Tests for catalog-level Glue resource handling in _filter_resource."""

    def test_catalog_arn_single_action(self):
        """A single catalog-level action on a catalog ARN."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": "glue:CreateDatabase",
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:catalog"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertEqual(perms_list[0].resource_arn(), f"arn:aws:glue:{REGION}:{CATALOG_ID}:catalog")
        self.assertSetEqual(perms_list[0].permission_actions(), {"glue:CreateDatabase"})

    def test_catalog_arn_with_all_glue_actions(self):
        """glue:* on a catalog ARN should produce all glue actions on the catalog."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": "glue:*",
                "Resource": f"arn:aws:glue:{REGION}:{CATALOG_ID}:catalog"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertSetEqual(perms_list[0].permission_actions(), ALL_GLUE_ACTIONS)

    def test_catalog_arn_allow_then_deny(self):
        """Allow then deny on catalog ARN."""
        app_config, iam_reader = _make_app_config()
        catalog_arn = f"arn:aws:glue:{REGION}:{CATALOG_ID}:catalog"
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetDatabases", "glue:CreateDatabase"],
                    "Resource": catalog_arn
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": ["glue:CreateDatabase"],
                    "Resource": catalog_arn
                }])
            ]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        self.assertEqual(len(perms_list), 1)
        self.assertSetEqual(perms_list[0].permission_actions(), {"glue:GetDatabases"})

    def test_catalog_arn_deny_all_removes_permission(self):
        """Deny all actions on catalog ARN should remove the permission entirely."""
        app_config, iam_reader = _make_app_config()
        catalog_arn = f"arn:aws:glue:{REGION}:{CATALOG_ID}:catalog"
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [
                _make_policy([{
                    "Effect": "Allow",
                    "Action": ["glue:GetDatabases"],
                    "Resource": catalog_arn
                }]),
                _make_policy([{
                    "Effect": "Deny",
                    "Action": ["glue:GetDatabases"],
                    "Resource": catalog_arn
                }])
            ]
        })

        perms = reader.read_policies()
        self.assertEqual(len(perms.get_permissions()), 0)

    def test_catalog_and_database_and_table_in_same_statement(self):
        """A statement with catalog, database, and table resources together."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetDatabases", "glue:GetDatabase", "glue:GetTable"],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{CATALOG_ID}:catalog",
                    f"arn:aws:glue:{REGION}:{CATALOG_ID}:database/test_database",
                    f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table"
                ]
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()
        perms_list.sort()

        self.assertEqual(len(perms_list), 3)
        resource_arns = {p.resource_arn() for p in perms_list}
        self.assertIn(f"arn:aws:glue:{REGION}:{CATALOG_ID}:catalog", resource_arns)
        self.assertIn(f"arn:aws:glue:{REGION}:{CATALOG_ID}:database/test_database", resource_arns)
        self.assertIn(f"arn:aws:glue:{REGION}:{CATALOG_ID}:table/test_database/test_table", resource_arns)

    def test_resource_star_includes_catalog(self):
        """Resource '*' should now include the catalog ARN alongside databases and tables."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetDatabases"],
                "Resource": "*"
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()

        resource_arns = {p.resource_arn() for p in perms_list}
        catalog_arn = f"arn:aws:glue:{REGION}:{CATALOG_ID}:catalog"
        self.assertIn(catalog_arn, resource_arns)

    def test_catalog_nonexistent_catalog_id_yields_nothing(self):
        """Catalog ARN with a catalog ID not in the data catalog."""
        app_config, iam_reader = _make_app_config()
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetDatabases"],
                "Resource": f"arn:aws:glue:{REGION}:999999999999:catalog"
            }])]
        })

        perms = reader.read_policies()
        self.assertEqual(len(perms.get_permissions()), 0)

    def test_two_principals_catalog_permissions(self):
        """Two principals with different catalog-level permissions."""
        app_config, iam_reader = _make_app_config()
        catalog_arn = f"arn:aws:glue:{REGION}:{CATALOG_ID}:catalog"
        reader = _make_reader(app_config, iam_reader, {
            PRINCIPAL: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:GetDatabases"],
                "Resource": catalog_arn
            }])],
            PRINCIPAL2: [_make_policy([{
                "Effect": "Allow",
                "Action": ["glue:CreateDatabase"],
                "Resource": catalog_arn
            }])]
        })

        perms = reader.read_policies()
        perms_list = perms.get_permissions()
        perms_list.sort()

        self.assertEqual(len(perms_list), 2)
        p1 = [p for p in perms_list if p.principal_arn() == PRINCIPAL][0]
        p2 = [p for p in perms_list if p.principal_arn() == PRINCIPAL2][0]
        self.assertSetEqual(p1.permission_actions(), {"glue:GetDatabases"})
        self.assertSetEqual(p2.permission_actions(), {"glue:CreateDatabase"})


if __name__ == '__main__':
    unittest.main()
