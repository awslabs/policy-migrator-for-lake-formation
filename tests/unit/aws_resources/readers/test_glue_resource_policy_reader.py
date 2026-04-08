import json
import unittest
from unittest.mock import Mock

from aws_resources.readers.glue_resource_policy_reader import GlueResourcePolicyReader


def _mock_paginator(pages):
    paginator = Mock()
    paginator.paginate.return_value = pages
    return paginator


class TestGlueResourcePolicyReader(unittest.TestCase):
    """Tests for GlueResourcePolicyReader."""

    def _make_reader(self, glue_client):
        session = Mock()
        session.client.return_value = glue_client
        return GlueResourcePolicyReader(session)

    def test_single_policy_single_statement(self):
        statement = {"Effect": "Allow", "Action": "glue:*", "Resource": "*"}
        policy_json = json.dumps({"Version": "2012-10-17", "Statement": [statement]})

        glue_client = Mock()
        glue_client.get_paginator.return_value = _mock_paginator([
            {"GetResourcePoliciesResponseList": [{"PolicyInJson": policy_json}]}
        ])

        reader = self._make_reader(glue_client)
        result = json.loads(reader.get_glue_resource_policy())

        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")

    def test_multiple_policies_merged(self):
        stmt1 = {"Effect": "Allow", "Action": "glue:GetTable", "Resource": "*"}
        stmt2 = {"Effect": "Deny", "Action": "glue:DeleteTable", "Resource": "*"}
        policy1 = json.dumps({"Version": "2012-10-17", "Statement": [stmt1]})
        policy2 = json.dumps({"Version": "2012-10-17", "Statement": [stmt2]})

        glue_client = Mock()
        glue_client.get_paginator.return_value = _mock_paginator([
            {"GetResourcePoliciesResponseList": [
                {"PolicyInJson": policy1},
                {"PolicyInJson": policy2},
            ]}
        ])

        reader = self._make_reader(glue_client)
        result = json.loads(reader.get_glue_resource_policy())

        self.assertEqual(len(result["Statement"]), 2)

    def test_empty_policies(self):
        glue_client = Mock()
        glue_client.get_paginator.return_value = _mock_paginator([
            {"GetResourcePoliciesResponseList": []}
        ])

        reader = self._make_reader(glue_client)
        result = json.loads(reader.get_glue_resource_policy())

        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 0)


if __name__ == '__main__':
    unittest.main()
