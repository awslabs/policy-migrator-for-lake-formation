import unittest

from config.argument_parser import ArgumentParser


class TestArgumentParser(unittest.TestCase):
    """Tests for ArgumentParser."""

    def test_default_args(self):
        result = ArgumentParser.parse_args(["prog"])
        self.assertIsNone(result[ArgumentParser.CONFIG_CONFIG_FILE_PATH])
        self.assertTrue(result[ArgumentParser.CONFIG_DRY_RUN])
        self.assertFalse(result[ArgumentParser.CONFIG_PRINT_MODULE_CONFIGURATIONS])
        self.assertEqual(result[ArgumentParser.CONFIG_LOG_LEVEL], "INFO")

    def test_config_file_path(self):
        result = ArgumentParser.parse_args(["prog", "-c", "myconfig.ini"])
        self.assertEqual(result[ArgumentParser.CONFIG_CONFIG_FILE_PATH], "myconfig.ini")

    def test_print_module_configurations(self):
        result = ArgumentParser.parse_args(["prog", "-p"])
        self.assertTrue(result[ArgumentParser.CONFIG_PRINT_MODULE_CONFIGURATIONS])

    def test_log_level(self):
        result = ArgumentParser.parse_args(["prog", "-l", "DEBUG"])
        self.assertEqual(result[ArgumentParser.CONFIG_LOG_LEVEL], "DEBUG")

    def test_all_args_combined(self):
        result = ArgumentParser.parse_args(["prog", "-c", "config.ini", "-d", "-p", "-l", "ERROR"])
        self.assertEqual(result[ArgumentParser.CONFIG_CONFIG_FILE_PATH], "config.ini")
        self.assertTrue(result[ArgumentParser.CONFIG_DRY_RUN])
        self.assertTrue(result[ArgumentParser.CONFIG_PRINT_MODULE_CONFIGURATIONS])
        self.assertEqual(result[ArgumentParser.CONFIG_LOG_LEVEL], "ERROR")


if __name__ == '__main__':
    unittest.main()
