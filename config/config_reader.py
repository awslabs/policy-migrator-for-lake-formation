import configparser

from .configuration_exceptions import ConfigurationInvalidException
class ConfigReader:
    '''
    Reads configuration file and returns a dictionary of the configuration
    '''

    def __init__(self, config_file_path):
        if config_file_path is None:
            raise ConfigurationInvalidException('config_file_path cannot be None')
        self._config_file_path = config_file_path

    def read_config(self) -> dict[str | dict[str]]:
        print(f"Reading from config file {self._config_file_path}")
        config = configparser.ConfigParser()
        config.read(self._config_file_path)

        return self.__convert_config_to_dict(config)

    def __convert_config_to_dict(self, config):
        config_dict = {}
        for section in config.sections():
            for key, value in config.items(section):
                config_dict.setdefault(section, {})[key] = value.strip() if value else None

        return config_dict
