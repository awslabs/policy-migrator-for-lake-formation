import logging

class ConfigException(Exception):
    '''
    Exception for configuration errors.
    '''

    def __init__(self, message):
        super().__init__(message)

class ConfigHelper:
    '''
    Helps when managing configuration.
    '''

    @staticmethod
    def get_section(args : dict[str | dict], sectionname : str, default = None) -> dict[str]:
        '''
        Get a section from the configuration. If the section doesn't exist, it will return
        an empty dictionary.
        '''
        assert args is not None
        assert sectionname is not None

        if sectionname in args:
            if not isinstance(args[sectionname], dict):
                raise ConfigException("Section " + sectionname + " is not a dictionary")
            return args[sectionname]
        return default

    @staticmethod
    def get_config_string(args : dict[str | dict], fieldname : str, default = None) -> str:
        if fieldname in args:
            if not isinstance(args[fieldname], str):
                raise ConfigException("Field " + fieldname + " is not a string")
            return args[fieldname]
        return default

    @staticmethod
    def get_config_boolean(args : dict[str | dict], fieldname : str, default = False) -> bool:
        if fieldname in args:
            val = args[fieldname]
            if not isinstance(val, str):
                raise ConfigException("Field " + fieldname + " is not a boolean")
            return val.lower() in ['true', 'yes']
        return default

    @staticmethod
    def configure_logger(command_args : dict[str | dict], config_file_args : dict[str | dict]):
        '''
        Configure the logger using arguments (as a dictionary). Logging configuration should be in the "logging" section.
        '''

        log_level = logging.INFO

        if "log-level" in command_args:
            log_level = command_args['log-level']

        if "logging" in config_file_args:
            logging_args = ConfigHelper.get_section(config_file_args, "logging")
            if "log_file" in logging_args:
                log_file = logging_args["log_file"]
                if not isinstance(log_file, str):
                    raise ConfigException("Field log_file is not a string")
                logging.basicConfig(filename=log_file)

            if "log_level" in logging_args:
                log_level = logging_args["log_level"]
                if not isinstance(log_level, str):
                    raise ConfigException("Field log_level is not a string")

        print(f"Logging level is being set to {log_level}")
        logging.getLogger().setLevel(level=log_level)

        # Always change boto3 to INFO because debug may be too verbose
        if log_level == "DEBUG":
            logging.getLogger('boto3').setLevel(logging.INFO)
            logging.getLogger('botocore').setLevel(logging.INFO)
            logging.getLogger('urllib3').setLevel(logging.INFO)
            logging.getLogger('awswrangler').setLevel(logging.INFO)
