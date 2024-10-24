from permissions.permission_record import PermissionRecord
from .permissions_list import PermissionsList
from config.config_helper import ConfigHelper

import logging
import csv
import os

logger = logging.getLogger(__name__)

class PermissionsImportExport:
    '''
    Exports and imports permissions to a file so it can be reused in subsequent runs.
    '''

    _PERMISSIONS_FIELD_NAMES = ['principal_arn', 'resource_arn', 'permission_actions']

    def __init__(self, args = None, enabled = False):
        self._enabled = enabled
        logger.debug(f"PermissionsImportExport: Enabled: {enabled}")
        _args = ConfigHelper.get_section(args, 'import_export')
        if enabled:
            logger.debug(f"PermissionsImportExport: Args: {_args}")
            self._export_policy_readers_filename = ConfigHelper.get_config_string(_args, "export_policy_readers_filename")
            self._import_policy_readers = ConfigHelper.get_config_boolean(_args, "import_policy_readers", False)
            self._export_filtered_permissions_filename = ConfigHelper.get_config_string(_args, "export_filtered_permissions_filename")
            self._import_filtered_permissions = ConfigHelper.get_config_boolean(_args, "import_filtered_permissions", False)
            self._export_post_processed_permissions_filename = ConfigHelper.get_config_string(_args, "export_post_processed_permissions_filename")
            self._import_post_processed_permissions = ConfigHelper.get_config_boolean(_args, "import_post_processed_permissions", False)
            self._export_lf_permissions_filename = ConfigHelper.get_config_string(_args, "export_lf_permissions_filename")
            self._import_lf_permissions = ConfigHelper.get_config_boolean(_args, "import_lf_permissions", False)

    @staticmethod
    def createImportExport(args):
        if "import_export" in args:
            return PermissionsImportExport(args, True)
        return PermissionsImportExport(args, False)

    def export_policy_readers_output(self, permissionsList : PermissionsList):
        if self._enabled and self._export_policy_readers_filename is not None:
            self._export_permissions(permissionsList, self._export_policy_readers_filename)

    def import_policy_readers_input(self):
        if self._enabled and self._import_policy_readers:
            return self._import_permissions(self._export_policy_readers_filename)
        return None

    def export_filtered_permissions_output(self, permissionsList : PermissionsList):
        if self._enabled and self._export_filtered_permissions_filename is not None:
            self._export_permissions(permissionsList, self._export_filtered_permissions_filename)

    def import_filtered_permissions_input(self):
        if self._enabled and self._import_filtered_permissions:
            return self._import_permissions(self._export_filtered_permissions_filename)
        return None

    def export_post_processed_permissions_output(self, permissionsList : PermissionsList):
        if self._enabled and self._export_post_processed_permissions_filename is not None:
            self._export_permissions(permissionsList, self._export_post_processed_permissions_filename)

    def import_post_processed_permissions_input(self):
        if self._enabled and self._import_post_processed_permissions:
            return self._import_permissions(self._export_post_processed_permissions_filename)
        return None

    def export_lf_permissions_output(self, permissionsList : PermissionsList):
        if self._enabled and self._export_lf_permissions_filename is not None:
            self._export_permissions(permissionsList, self._export_lf_permissions_filename)

    def import_lf_permissions_input(self):
        if self._enabled and self._import_lf_permissions:
            return self._import_permissions(self._export_lf_permissions_filename)
        return None

    def _export_permissions(self, permissionsList : PermissionsList, output_filename : str):
        logger.info(f"PermissionsImportExport: Exporting permissions to {output_filename}")
        with open(output_filename, mode='w', encoding="utf-8") as exportFile:
            csvWriter = csv.DictWriter(exportFile, delimiter=',', quotechar='\"', escapechar="\\", quoting=csv.QUOTE_ALL, fieldnames=PermissionsImportExport._PERMISSIONS_FIELD_NAMES)
            for permission in permissionsList:
                csvWriter.writerow( {  PermissionsImportExport._PERMISSIONS_FIELD_NAMES[0] : permission.principal_arn(),
                                       PermissionsImportExport._PERMISSIONS_FIELD_NAMES[1] : permission.resource_arn(),
                                       PermissionsImportExport._PERMISSIONS_FIELD_NAMES[2] : "[" + ",".join([action for action in permission.permission_actions()]) + "]"
                                    }
                                  )


    def _import_permissions(self, input_filename : str):
        if not os.path.exists(input_filename):
            logger.debug(f"PermissionsImportExport: File {input_filename} does not exist")
            return None

        logger.info(f"importing permissions from {input_filename}")
        permissionsList = PermissionsList()
        with open(input_filename, mode='r', encoding="utf-8") as importFile:
            reader = csv.DictReader(importFile, delimiter=',', quotechar='\"', escapechar="\\", quoting=csv.QUOTE_ALL, fieldnames=PermissionsImportExport._PERMISSIONS_FIELD_NAMES)
            for row in reader:
                permissionsList.add_permission_record(
                        PermissionRecord(row['principal_arn'],
                                               row['resource_arn'],
                                               set(row['permission_actions'][1:-1].split(","))
                        ))

        return permissionsList
