import unittest
from unittest.mock import Mock, MagicMock, patch

from aws_resources.readers.iam_policy_reader import IamPolicyReader


def _mock_paginator(pages):
    """Create a mock paginator that yields the given pages."""
    paginator = Mock()
    paginator.paginate.return_value = pages
    return paginator


POLICY_DOC = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}


class TestIamPolicyReader(unittest.TestCase):
    """Tests for IamPolicyReader."""

    def _make_reader(self, iam_client):
        session = Mock()
        session.client.return_value = iam_client
        return IamPolicyReader(session)

    def _setup_iam_client(self, roles=None, users=None):
        iam_client = Mock()

        role_pages = [{"Roles": roles or []}]
        user_pages = [{"Users": users or []}]

        def get_paginator(operation):
            paginators = {
                "list_roles": _mock_paginator(role_pages),
                "list_users": _mock_paginator(user_pages),
                "list_attached_role_policies": _mock_paginator([{"AttachedPolicies": []}]),
                "list_role_policies": _mock_paginator([{"PolicyNames": []}]),
                "list_user_policies": _mock_paginator([{"PolicyNames": []}]),
                "list_attached_user_policies": _mock_paginator([{"AttachedPolicies": []}]),
                "list_groups_for_user": _mock_paginator([{"Groups": []}]),
                "list_group_policies": _mock_paginator([{"PolicyNames": []}]),
            }
            return paginators.get(operation, _mock_paginator([]))

        iam_client.get_paginator.side_effect = get_paginator
        return iam_client

    def test_empty_account_no_roles_no_users(self):
        iam_client = self._setup_iam_client()
        reader = self._make_reader(iam_client)

        users, roles = reader.get_all_principal_arns()
        self.assertEqual(users, [])
        self.assertEqual(roles, [])

    def test_skips_service_roles(self):
        roles = [
            {"RoleName": "MyRole", "Arn": "arn:aws:iam::123456789012:role/MyRole"},
            {"RoleName": "ServiceRole", "Arn": "arn:aws:iam::123456789012:role/service-role/ServiceRole"},
        ]
        iam_client = self._setup_iam_client(roles=roles)
        reader = self._make_reader(iam_client)

        users, role_arns = reader.get_all_principal_arns()
        self.assertEqual(len(role_arns), 1)
        self.assertEqual(role_arns[0], "arn:aws:iam::123456789012:role/MyRole")

    def test_reads_role_attached_managed_policy(self):
        roles = [{"RoleName": "MyRole", "Arn": "arn:aws:iam::123456789012:role/MyRole"}]
        iam_client = self._setup_iam_client(roles=roles)

        # Override attached role policies paginator
        attached_pages = [{"AttachedPolicies": [{"PolicyArn": "arn:aws:iam::123456789012:policy/MyPolicy"}]}]
        def get_paginator(operation):
            if operation == "list_roles":
                return _mock_paginator([{"Roles": roles}])
            if operation == "list_attached_role_policies":
                return _mock_paginator(attached_pages)
            if operation == "list_role_policies":
                return _mock_paginator([{"PolicyNames": []}])
            if operation == "list_users":
                return _mock_paginator([{"Users": []}])
            return _mock_paginator([])

        iam_client.get_paginator.side_effect = get_paginator
        iam_client.get_policy.return_value = {"Policy": {"DefaultVersionId": "v1"}}
        iam_client.get_policy_version.return_value = {"PolicyVersion": {"Document": POLICY_DOC}}

        reader = self._make_reader(iam_client)
        policies = reader.get_iam_policies_for_prinicpal("arn:aws:iam::123456789012:role/MyRole")

        self.assertEqual(len(policies), 1)
        self.assertEqual(policies[0], POLICY_DOC)

    def test_reads_role_inline_policy(self):
        roles = [{"RoleName": "MyRole", "Arn": "arn:aws:iam::123456789012:role/MyRole"}]
        iam_client = self._setup_iam_client(roles=roles)

        def get_paginator(operation):
            if operation == "list_roles":
                return _mock_paginator([{"Roles": roles}])
            if operation == "list_attached_role_policies":
                return _mock_paginator([{"AttachedPolicies": []}])
            if operation == "list_role_policies":
                return _mock_paginator([{"PolicyNames": ["InlinePolicy1"]}])
            if operation == "list_users":
                return _mock_paginator([{"Users": []}])
            return _mock_paginator([])

        iam_client.get_paginator.side_effect = get_paginator
        iam_client.get_role_policy.return_value = {"PolicyDocument": POLICY_DOC}

        reader = self._make_reader(iam_client)
        policies = reader.get_iam_policies_for_prinicpal("arn:aws:iam::123456789012:role/MyRole")

        self.assertEqual(len(policies), 1)
        self.assertEqual(policies[0], POLICY_DOC)

    def test_reads_user_inline_and_attached_policies(self):
        users = [{"UserName": "alice", "Arn": "arn:aws:iam::123456789012:user/alice"}]
        iam_client = self._setup_iam_client(users=users)

        inline_doc = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "glue:*", "Resource": "*"}]}

        def get_paginator(operation):
            if operation == "list_roles":
                return _mock_paginator([{"Roles": []}])
            if operation == "list_users":
                return _mock_paginator([{"Users": users}])
            if operation == "list_user_policies":
                return _mock_paginator([{"PolicyNames": ["UserInline"]}])
            if operation == "list_attached_user_policies":
                return _mock_paginator([{"AttachedPolicies": [{"PolicyArn": "arn:aws:iam::123456789012:policy/UserManaged"}]}])
            if operation == "list_groups_for_user":
                return _mock_paginator([{"Groups": []}])
            return _mock_paginator([])

        iam_client.get_paginator.side_effect = get_paginator
        iam_client.get_user_policy.return_value = {"PolicyDocument": inline_doc}
        iam_client.get_policy.return_value = {"Policy": {"DefaultVersionId": "v1"}}
        iam_client.get_policy_version.return_value = {"PolicyVersion": {"Document": POLICY_DOC}}

        reader = self._make_reader(iam_client)
        policies = reader.get_iam_policies_for_prinicpal("arn:aws:iam::123456789012:user/alice")

        self.assertEqual(len(policies), 2)

    def test_reads_user_group_policies(self):
        users = [{"UserName": "bob", "Arn": "arn:aws:iam::123456789012:user/bob"}]
        iam_client = self._setup_iam_client(users=users)

        group_doc = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}

        def get_paginator(operation):
            if operation == "list_roles":
                return _mock_paginator([{"Roles": []}])
            if operation == "list_users":
                return _mock_paginator([{"Users": users}])
            if operation == "list_user_policies":
                return _mock_paginator([{"PolicyNames": []}])
            if operation == "list_attached_user_policies":
                return _mock_paginator([{"AttachedPolicies": []}])
            if operation == "list_groups_for_user":
                return _mock_paginator([{"Groups": [{"GroupName": "DataTeam", "Arn": "arn:aws:iam::123456789012:group/DataTeam"}]}])
            if operation == "list_group_policies":
                return _mock_paginator([{"PolicyNames": ["GroupPolicy1"]}])
            return _mock_paginator([])

        iam_client.get_paginator.side_effect = get_paginator
        iam_client.get_group_policy.return_value = {"PolicyDocument": group_doc}

        reader = self._make_reader(iam_client)
        policies = reader.get_iam_policies_for_prinicpal("arn:aws:iam::123456789012:user/bob")

        self.assertEqual(len(policies), 1)
        self.assertEqual(policies[0], group_doc)

    def test_caches_group_policies(self):
        """Two users in the same group should not re-read group policies."""
        users = [
            {"UserName": "alice", "Arn": "arn:aws:iam::123456789012:user/alice"},
            {"UserName": "bob", "Arn": "arn:aws:iam::123456789012:user/bob"},
        ]
        group = {"GroupName": "SharedGroup", "Arn": "arn:aws:iam::123456789012:group/SharedGroup"}
        group_doc = {"Version": "2012-10-17", "Statement": []}
        iam_client = self._setup_iam_client(users=users)

        def get_paginator(operation):
            if operation == "list_roles":
                return _mock_paginator([{"Roles": []}])
            if operation == "list_users":
                return _mock_paginator([{"Users": users}])
            if operation == "list_user_policies":
                return _mock_paginator([{"PolicyNames": []}])
            if operation == "list_attached_user_policies":
                return _mock_paginator([{"AttachedPolicies": []}])
            if operation == "list_groups_for_user":
                return _mock_paginator([{"Groups": [group]}])
            if operation == "list_group_policies":
                return _mock_paginator([{"PolicyNames": ["GP1"]}])
            return _mock_paginator([])

        iam_client.get_paginator.side_effect = get_paginator
        iam_client.get_group_policy.return_value = {"PolicyDocument": group_doc}

        reader = self._make_reader(iam_client)
        reader.get_all_principal_arns()

        # get_group_policy should only be called once (cached for second user)
        iam_client.get_group_policy.assert_called_once()

    def test_get_all_prinicial_policies_returns_iterator(self):
        roles = [{"RoleName": "R1", "Arn": "arn:aws:iam::123456789012:role/R1"}]
        iam_client = self._setup_iam_client(roles=roles)

        def get_paginator(operation):
            if operation == "list_roles":
                return _mock_paginator([{"Roles": roles}])
            if operation == "list_attached_role_policies":
                return _mock_paginator([{"AttachedPolicies": []}])
            if operation == "list_role_policies":
                return _mock_paginator([{"PolicyNames": ["IP1"]}])
            if operation == "list_users":
                return _mock_paginator([{"Users": []}])
            return _mock_paginator([])

        iam_client.get_paginator.side_effect = get_paginator
        iam_client.get_role_policy.return_value = {"PolicyDocument": POLICY_DOC}

        reader = self._make_reader(iam_client)
        all_policies = dict(reader.get_all_prinicial_policies())

        self.assertIn("arn:aws:iam::123456789012:role/R1", all_policies)
        self.assertEqual(len(all_policies["arn:aws:iam::123456789012:role/R1"]), 1)

    def test_only_reads_once(self):
        iam_client = self._setup_iam_client()
        reader = self._make_reader(iam_client)

        reader.get_all_principal_arns()
        reader.get_all_principal_arns()

        # list_roles paginator should only be created once
        calls = [c for c in iam_client.get_paginator.call_args_list if c[0][0] == "list_roles"]
        self.assertEqual(len(calls), 1)


if __name__ == '__main__':
    unittest.main()
