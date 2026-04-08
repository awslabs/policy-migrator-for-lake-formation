import os
import unittest

from config.config_reader import ConfigReader
from config.configuration_exceptions import ConfigurationInvalidException


class TestConfigReader(unittest.TestCase):
    """Tests for ConfigReader."""

    _TEST_FILE = "test_config_reader.ini"

    def tearDown(self):
        if os.path.exists(self._TEST_FILE):
            os.remove(self._TEST_FILE)

    def test_raises_on_none_path(self):
        with self.assertRaises(ConfigurationInvalidException):
            ConfigReader(None)

    def test_reads_ini_file(self):
        with open(self._TEST_FILE, "w") as f:
            f.write("[main]\n")
            f.write("dry_run = true\n")
            f.write("[logging]\n")
            f.write("log_level = DEBUG\n")

        reader = ConfigReader(self._TEST_FILE)
        config = reader.read_config()

        self.assertIn("main", config)
        self.assertEqual(config["main"]["dry_run"], "true")
        self.assertIn("logging", config)
        self.assertEqual(config["logging"]["log_level"], "DEBUG")

    def test_empty_file_returns_empty_dict(self):
        with open(self._TEST_FILE, "w") as f:
            f.write("")

        reader = ConfigReader(self._TEST_FILE)
        config = reader.read_config()
        self.assertEqual(config, {})

    def test_strips_whitespace_from_values(self):
        with open(self._TEST_FILE, "w") as f:
            f.write("[section]\n")
            f.write("key =   value_with_spaces   \n")

        reader = ConfigReader(self._TEST_FILE)
        config = reader.read_config()
        self.assertEqual(config["section"]["key"], "value_with_spaces")


if __name__ == '__main__':
    unittest.main()
