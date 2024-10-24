# Import Policy Readers
from policy_readers.glue_cloudtrail_reader import GlueEventCloudTrailPolicyReader
from policy_readers.iam_policy_permissions_reader import IamPolicyPermissionsReader
from policy_readers.policy_reader_interface import PolicyReaderInterface
from policy_readers.s3_bucket_permissions_policy_reader import S3BucketPermissionsPolicyReader
from policy_readers.s3_cloudtrail_reader import S3CloudTrailDataEventsReader

# Import Policy Filters
from policy_filters.filter_invalid_actions_to_resources import FilterInvalidActionsToResources
from policy_filters.policy_filter_interface import PolicyFilterInterface
from policy_filters.iam_principal_validator import IAMPrincipalValidator
from policy_filters.glue_data_catalog_filter import FilterNotInGlueCatalog
from policy_filters.iam_policy_simulator_validator import IAMPolicySimulatorValidator

# Post Processing modules
from post_processing_plugins.post_processing_plugin_interface import PostProcessingPluginInterface
from post_processing_plugins.add_s3_permissions_from_glue_permissions import AddDataPermissionsFromGluePermissions

# Import Data Location processor
from lakeformation_utils.data_lake_location_generator import LakeFormationS3DataLakeLocationGenerator

# Lake Formation Committers
from lakeformation_committers.data_lake_location_committer import DataLakeLocationCommitter
from lakeformation_committers.commit_lake_formation_permissions import LakeFormationPermissionsCommitter

# Lake Formation Permissions Translator
from permissions.permissions_exporter import PermissionsImportExport
from permissions.translators.actions_to_lakeformation_permissions_translator import ActionsToLFPermissionsTranslator
from permissions.permissions_list import PermissionsList

from config.application_configuration import ApplicationConfiguration
from config.config_helper import ConfigHelper

import logging
logger = logging.getLogger(__name__)

class MainApplication:
    '''
    The main application class.
    '''
    _POLICY_READERS : list[PolicyReaderInterface] = [ GlueEventCloudTrailPolicyReader, S3CloudTrailDataEventsReader, S3BucketPermissionsPolicyReader, IamPolicyPermissionsReader]
    _POLICY_FILTERS : list[PolicyFilterInterface] = [ IAMPrincipalValidator, FilterNotInGlueCatalog, FilterInvalidActionsToResources, IAMPolicySimulatorValidator ]
    _POST_PROCESSING_PLUGINS : list[PostProcessingPluginInterface] = [ AddDataPermissionsFromGluePermissions ]

    def __init__(self, args):
        self._args = args
        self._main_args = ConfigHelper.get_section(args, "main")
        self._is_dry_run = ConfigHelper.get_config_boolean(self._main_args, "dry_run", True)
        self._app_conf = ApplicationConfiguration(args)
        self._import_export = PermissionsImportExport.createImportExport(args)

    def run(self):
        logger.info(f"=> Starting. Configuration: DryRun {self._is_dry_run}.")
        logger.debug(f"===> Configuration: {self._args}")

        permissionsList = PermissionsList()
        permissionsList = self._get_policies(permissionsList)
        permissionsList = self._filter_policies(permissionsList)
        lfpermissionsList = self._convert_permissions_to_lf_permissions(permissionsList)
        lfpermissionsList = self._run_post_processing_plugins(lfpermissionsList)

        self._process_data_locations()

        if not self._is_dry_run:
            logger.info("=> Committing Lake Formation permissions.")
            lfPermissionsCommitter = LakeFormationPermissionsCommitter(self._app_conf.get_boto3_session())
            lfPermissionsCommitter.commit_lakeformation_permissions(lfpermissionsList)
            logger.info("=> Completed Committing Lake Formation permissions.")

        logger.info("=> Completed.")

    def _get_policies(self, permissionsList : PermissionsList) -> PermissionsList:
        logger.info("=> Generating Permissions from source.")
        importedPermissionsList : PermissionsList | None = self._import_export.import_policy_readers_input()
        if importedPermissionsList is not None:
            logger.info("    Imported permissions from previous run.")
            return importedPermissionsList

        for module in MainApplication._POLICY_READERS:
            config_section = ConfigHelper.get_section(self._args, module.get_config_section(), {})
            if "enabled" in config_section and config_section["enabled"] == "true":
                logger.info(f"=> Starting to read policies using {module.get_name()}")
                reader : PolicyReaderInterface = module(self._app_conf, config_section)
                newPermissionsList = reader.read_policies()
                permissionsList.add_permissions_from_list(newPermissionsList)
                logger.info(f"=> Finished reading policies from {module.get_name()}. Found {newPermissionsList.get_permissions_count()} permissions.")

        self._import_export.export_policy_readers_output(permissionsList)

        logger.info(f"=> Finished generating permission policies from sources. Generated {permissionsList.get_permissions_count()} permissions after merging all sources.")

        return permissionsList

    def _filter_policies(self, permissionsList : PermissionsList) -> PermissionsList:
        logger.info("=> Starting to filtering permissions.")
        imported_permissions : PermissionsList | None = self._import_export.import_filtered_permissions_input()
        if imported_permissions is not None:
            return imported_permissions

        for module in MainApplication._POLICY_FILTERS:
            config_section = ConfigHelper.get_section(self._args, module.get_config_section(), {})
            if "enabled" in config_section and config_section["enabled"] == "true":
                logger.info(f"=> Starting to filter policies using {module.get_name()}. Current Policy Count: {permissionsList.get_permissions_count()}")
                policyFilter : PolicyFilterInterface = module(self._app_conf, config_section)
                filteredPermissions = policyFilter.filter_policies(permissionsList)
                permissionsList.remove_permissions(filteredPermissions)
                logger.info(f"=> Finished filtering policies using {module.get_name()}. Found {policyFilter.get_number_filtered()} permissions to filter. Current Policy Count: {permissionsList.get_permissions_count()}")
                self._output_current_permissions_list(permissionsList)

        self._import_export.export_filtered_permissions_output(permissionsList)

        logger.info("=> Finished filtering permissions.")
        return permissionsList

    def _run_post_processing_plugins(self, permissionsList) -> PermissionsList:
        logger.info("=> Starting to run post processing plugins.")
        imported_permissions : PermissionsList | None = self._import_export.import_post_processed_permissions_input()
        if imported_permissions is not None:
            return self._import_export.import_post_processed_permissions_input()

        for module in MainApplication._POST_PROCESSING_PLUGINS:
            config_section = ConfigHelper.get_section(self._args, module.get_config_section(), {})
            if "enabled" in config_section and config_section["enabled"] == "true":
                logger.info(f"=> Starting to filter policies using {module.get_name()}")
                post_processing_plugin : PostProcessingPluginInterface = module(self._app_conf, config_section)
                permissionsList = post_processing_plugin.process(permissionsList)
                logger.info(f"=> Finished reading policies from {module.get_name()}. ")

        self._import_export.export_post_processed_permissions_output(permissionsList)

        return permissionsList

    def _process_data_locations(self):
        do_process_data_locations = ConfigHelper.get_section(self._args, "lakeformation_data_location_registration")
        if not do_process_data_locations:
            return

        section = ConfigHelper.get_section(self._args, "lakeformation_data_location_registration")
        registration_role = ConfigHelper.get_config_string(section, "iam_role_arn")
        use_service_linked_role = ConfigHelper.get_config_boolean(section, "use_service_linked_role")
        enabled = ConfigHelper.get_config_boolean(section, "enabled", False)

        if not enabled:
            return

        logger.info("=> Generating Lake Formation data locations.")

        dataLocationGenerator = LakeFormationS3DataLakeLocationGenerator()
        locations = dataLocationGenerator.generate_data_lake_locations(self._app_conf.get_glue_data_catalog())

        if registration_role is not None and use_service_linked_role:
            logger.error("Cannot use service linked role and a specific role at the same time. Not registering data locations")
            return

        dataLocationCommitter = DataLakeLocationCommitter(self._app_conf.get_boto3_session(), locations, registration_role)

        logger.info("=> Finished generating data locations")

        if not self._is_dry_run:
            logger.info("=> Committing Data Lake locations.")
            dataLocationCommitter.register_locations()
            logger.info("=> Finished committing data locations.")

    def _convert_permissions_to_lf_permissions(self, permissionsList : PermissionsList):
        translator = ActionsToLFPermissionsTranslator(self._app_conf.get_s3_to_table_translator())
        lfpermissions = translator.translate_iam_permissions_to_lf_permissions(permissionsList)

        self._import_export.export_lf_permissions_output(lfpermissions)
        return lfpermissions

    def _output_current_permissions_list(self, permissionsList : PermissionsList):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("=> Current permission list:")
            for permission in permissionsList:
                logger.debug(f"     {permission}")

    @staticmethod
    def get_module_configurations() -> dict[str | dict]:
        module_configurations = {}

        for module in MainApplication._POLICY_READERS:
            module_configurations[module.get_name()] = module.get_required_configuration()

        for module in MainApplication._POLICY_FILTERS:
            module_configurations[module.get_name()] = module.get_required_configuration()

        for module in MainApplication._POST_PROCESSING_PLUGINS:
            module_configurations[module.get_name()] = module.get_required_configuration()

        return module_configurations
