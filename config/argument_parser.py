import argparse
from argparse import Namespace

class ArgumentParser:
    '''
    Parses the command line arguments.
    '''
    CONFIG_DRY_RUN = "dry_run"
    CONFIG_LOG_LEVEL = "log_level"
    CONFIG_PRINT_MODULE_CONFIGURATIONS = "print_module_configurations"
    CONFIG_CONFIG_FILE_PATH = "config_file_path"

    @staticmethod
    def parse_args(args) -> Namespace:
        #TODO: Fill out github link
        parser = argparse.ArgumentParser(
                    prog='Lake Formation Policy Migration Tool',
                    description='A tool that migrates permissions to Lake Formation.',
                    epilog='This code is being managed on Github at: <XYZ>')

        parser.add_argument('-c', f'--{ArgumentParser.CONFIG_CONFIG_FILE_PATH.replace("_", "-")}', default=None, help='The location of your config file, if different from default "config.ini"')
        parser.add_argument('-d', f'--{ArgumentParser.CONFIG_DRY_RUN.replace("_", "-")}', default=True, action='store_true', required=False, help='Flag that determines if this application should be a dry run or not. Default = true. Overrides whats in config.ini')
        parser.add_argument('-p', f'--{ArgumentParser.CONFIG_PRINT_MODULE_CONFIGURATIONS.replace("_", "-")}', action='store_true', required=False, help='If set, then will output the configuration of all available modules.')
        parser.add_argument("-l", f'--{ArgumentParser.CONFIG_LOG_LEVEL.replace("_", "-")}', default="INFO", required=False, help="log level as DEBUG, INFO, WARN, ERROR. Default = INFO. Overrides whats in config.ini' ")

        parsed_args = parser.parse_args(args[1:])
        return vars(parsed_args)
