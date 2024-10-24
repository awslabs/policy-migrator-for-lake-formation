import sys as _sys

from main_application import MainApplication
from config.config_reader import ConfigReader
from config.argument_parser import ArgumentParser

from config.config_helper import ConfigHelper

if __name__ == '__main__':
    command_args = ArgumentParser.parse_args(_sys.argv)

    print(command_args)

    if ArgumentParser.CONFIG_PRINT_MODULE_CONFIGURATIONS in command_args and \
            command_args[ArgumentParser.CONFIG_PRINT_MODULE_CONFIGURATIONS]:
        module_configurations = MainApplication.get_module_configurations()
        print("Module configurations: ")
        print("====================================================")
        for module_name, module_configuration in module_configurations.items():
            print(f"Module Name: {module_name}")
            print("------------------------------------------")
            if not module_configuration:
                print("No configuration required")
            else:
                for config_name, config_description in module_configuration.items():
                    print(f"    Config Name: {config_name}, Config Description: {config_description}")
            print("------------------------------------------")
        _sys.exit()

    if ArgumentParser.CONFIG_CONFIG_FILE_PATH not in command_args or \
        command_args[ArgumentParser.CONFIG_CONFIG_FILE_PATH] is None:
        print(f"ERROR: Configuration parameter {ArgumentParser.CONFIG_CONFIG_FILE_PATH} is required")
        _sys.exit(-1)

    config_reader = ConfigReader(command_args[ArgumentParser.CONFIG_CONFIG_FILE_PATH])
    configurations = config_reader.read_config()

    configLevel = ConfigHelper.configure_logger(command_args, configurations)

    mainApp = MainApplication(configurations)
    mainApp.run()
