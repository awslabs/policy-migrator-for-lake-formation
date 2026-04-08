import unittest
import logging

from config.config_helper import ConfigHelper, ConfigException


class TestConfigHelperGetSection(unittest.TestCase):
    """Tests for ConfigHelper.get_section."""

    def test_returns_section_when_exists(self):
        args = {"mysection": {"key": "val"}}
        result = ConfigHelper.get_section(args, "mysection")
        self.assertEqual(result, {"key": "val"})

    def test_returns_default_when_missing(self):
        result = ConfigHelper.get_section({}, "missing")
        self.assertIsNone(result)

    def test_returns_custom_default(self):
        result = ConfigHelper.get_section({}, "missing", {"fallback": True})
        self.assertEqual(result, {"fallback": True})

    def test_raises_when_section_is_not_dict(self):
        with self.assertRaises(ConfigException):
            ConfigHelper.get_section({"bad": "string"}, "bad")

    def test_asserts_on_none_args(self):
        with self.assertRaises(AssertionError):
            ConfigHelper.get_section(None, "x")

    def test_asserts_on_none_sectionname(self):
        with self.assertRaises(AssertionError):
            ConfigHelper.get_section({}, None)


class TestConfigHelperGetConfigString(unittest.TestCase):
    """Tests for ConfigHelper.get_config_string."""

    def test_returns_string_value(self):
        self.assertEqual(ConfigHelper.get_config_string({"k": "v"}, "k"), "v")

    def test_returns_default_when_missing(self):
        self.assertIsNone(ConfigHelper.get_config_string({}, "k"))
        self.assertEqual(ConfigHelper.get_config_string({}, "k", "fallback"), "fallback")

    def test_raises_when_not_string(self):
        with self.assertRaises(ConfigException):
            ConfigHelper.get_config_string({"k": 123}, "k")


class TestConfigHelperGetConfigBoolean(unittest.TestCase):
    """Tests for ConfigHelper.get_config_boolean."""

    def test_true_values(self):
        self.assertTrue(ConfigHelper.get_config_boolean({"k": "true"}, "k"))
        self.assertTrue(ConfigHelper.get_config_boolean({"k": "True"}, "k"))
        self.assertTrue(ConfigHelper.get_config_boolean({"k": "TRUE"}, "k"))
        self.assertTrue(ConfigHelper.get_config_boolean({"k": "yes"}, "k"))
        self.assertTrue(ConfigHelper.get_config_boolean({"k": "Yes"}, "k"))

    def test_false_values(self):
        self.assertFalse(ConfigHelper.get_config_boolean({"k": "false"}, "k"))
        self.assertFalse(ConfigHelper.get_config_boolean({"k": "no"}, "k"))
        self.assertFalse(ConfigHelper.get_config_boolean({"k": "anything"}, "k"))

    def test_returns_default_when_missing(self):
        self.assertFalse(ConfigHelper.get_config_boolean({}, "k"))
        self.assertTrue(ConfigHelper.get_config_boolean({}, "k", True))

    def test_raises_when_not_string(self):
        with self.assertRaises(ConfigException):
            ConfigHelper.get_config_boolean({"k": True}, "k")


class TestConfigHelperConfigureLogger(unittest.TestCase):
    """Tests for ConfigHelper.configure_logger."""

    def test_sets_log_level_from_command_args(self):
        ConfigHelper.configure_logger({"log-level": "WARNING"}, {})
        self.assertEqual(logging.getLogger().level, logging.WARNING)

    def test_sets_log_level_from_config_file(self):
        ConfigHelper.configure_logger({}, {"logging": {"log_level": "ERROR"}})
        self.assertEqual(logging.getLogger().level, logging.ERROR)

    def test_config_file_overrides_command_args(self):
        ConfigHelper.configure_logger({"log-level": "WARNING"}, {"logging": {"log_level": "DEBUG"}})
        self.assertEqual(logging.getLogger().level, logging.DEBUG)

    def test_debug_level_sets_boto3_to_info(self):
        ConfigHelper.configure_logger({}, {"logging": {"log_level": "DEBUG"}})
        self.assertEqual(logging.getLogger('boto3').level, logging.INFO)
        self.assertEqual(logging.getLogger('botocore').level, logging.INFO)

    def test_no_logging_section_uses_default(self):
        ConfigHelper.configure_logger({}, {})
        self.assertEqual(logging.getLogger().level, logging.INFO)

    def test_raises_when_log_level_not_string(self):
        with self.assertRaises(ConfigException):
            ConfigHelper.configure_logger({}, {"logging": {"log_level": 123}})

    def test_raises_when_log_file_not_string(self):
        with self.assertRaises(ConfigException):
            ConfigHelper.configure_logger({}, {"logging": {"log_file": 123}})


if __name__ == '__main__':
    unittest.main()
