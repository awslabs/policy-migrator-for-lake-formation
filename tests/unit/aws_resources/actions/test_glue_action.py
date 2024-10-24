import unittest

from aws_resources.actions.glue_action import GlueAction

class TestGlueAction(unittest.TestCase):

    def test_glue_action(self):
        actions = GlueAction.get_glue_actions_with_wildcard("Get*")
        self.assertEqual(len(actions), 9)

        actions = GlueAction.get_glue_actions_with_wildcard("Create*")
        self.assertEqual(len(actions), 4)

        actions = GlueAction.get_glue_actions_with_wildcard("Delete*")
        self.assertEqual(len(actions), 5)

        actions = GlueAction.get_glue_actions_with_wildcard("UpdateTable")
        self.assertEqual(len(actions), 1)

    def test_glue_action_search_with_only_wildcard(self):
        actions = GlueAction.get_glue_actions_with_wildcard("*")
        self.assertListEqual(actions, ["glue:" + action for action in GlueAction])

if __name__ == '__main__':
    unittest.main()
