from aws_resources.actions.glue_action import GlueAction
from aws_resources.actions.s3_action import S3Action
from aws_resources.aws_arn_utils import AwsArnUtils
from aws_resources.aws_resource import AwsObject
from aws_resources.glue_data_catalog import GlueDataCatalog
from aws_resources.glue_database import GlueDatabase
from aws_resources.glue_table import GlueTable
from aws_resources.readers.iam_policy_reader import IamPolicyReader
from config.application_configuration import ApplicationConfiguration
from lakeformation_utils.s3_to_table_mapper import S3ToTableMapper
from permissions.permissions_list import PermissionsList

import logging
logger = logging.getLogger(__name__)

class IamPolicyParser:
    """
    Parses an IAM policy for Glue and S3 permissions.

    Limitation: We do not support NotResource. Only Resource in Policies.
    """
    def __init__(self, appConfig : ApplicationConfiguration):
        self._glue_data_catalog : GlueDataCatalog = appConfig.get_glue_data_catalog()
        self._s3_to_table_mapper : S3ToTableMapper = appConfig.get_s3_to_table_translator()
        self._iam_policy_reader : IamPolicyReader = appConfig.get_iam_policy_reader()

    def read_iam_principal_allow_policies(self, permissionsList : PermissionsList, principal : str, policy : dict[str | dict]) -> PermissionsList:
        for statement in policy["Statement"]:
            if statement["Effect"] == "Allow" and self._is_valid_statement(statement):
                logger.info(f"Processing For allow Statements for {principal}, policy: {policy}")
                self._read_allow_statements(principal, statement, permissionsList)

        return permissionsList

    def read_iam_deny_policies(self, permissionsList : PermissionsList, principal : str, policy : dict[str | dict]) -> PermissionsList:
        for statement in policy["Statement"]:
            if statement["Effect"] == "Deny" and self._is_valid_statement(statement):
                logger.info(f"Processing For deny Statements for {principal}, policy: {policy}")
                self._read_deny_statements(principal, statement, permissionsList)

        return permissionsList

    def read_resource_policy_allow_policies(self, permissionsList : PermissionsList, resource_arn : str, policy : dict[str, str | dict]) -> PermissionsList:
        for statement in policy["Statement"]:
            if statement["Effect"] == "Allow" and self._is_valid_statement(statement):
                logger.info(f"Processing For allow Statements for {resource_arn}, policy: {policy}")
                self._fix_action_and_resource(resource_arn, statement)
                principal_arns : list[str] = self._get_principals(statement["Principal"])
                for principal_arn in principal_arns:
                    self._read_allow_statements(principal_arn, statement, permissionsList)
        return permissionsList

    def read_resource_policy_deny_policies(self, permissionsList : PermissionsList, resource_arn : str, policy : dict[str, str | dict]) -> PermissionsList:
        for statement in policy["Statement"]:
            if statement["Effect"] == "Deny" and self._is_valid_statement(statement):
                logger.info(f"Processing For deny Statements for {resource_arn}, policy: {policy}")
                self._fix_action_and_resource(resource_arn, statement)
                principal_arns = self._get_principals(statement["Principal"])
                for principal_arn in principal_arns:
                    self._read_deny_statements(principal_arn, statement, permissionsList)
        return permissionsList

    def _get_principals(self, principal_statement: str | list[str]) -> list[str]:
        principal_arns : list[str] = []

        if "AWS" not in principal_statement:
            logger.debug("AWS section in Principal is not found in Resource Policy. Ignoring this policy.")
            return principal_arns

        principal_statement = principal_statement["AWS"]

        logger.debug(f"Principal is: {principal_statement}")
        if isinstance(principal_statement, list):
            for principal_arn in principal_statement:
                if principal_arn == "*":
                    iam_users, iam_roles = self._iam_policy_reader.get_all_principal_arns()
                    principal_arns.extend(iam_users)
                    principal_arns.extend(iam_roles)
                    return principal_arns
                principal_arns.append(principal_arn)
        else:
            if principal_statement == "*":
                iam_users, iam_roles = self._iam_policy_reader.get_all_principal_arns()
                principal_arns.extend(iam_users)
                principal_arns.extend(iam_roles)
                return principal_arns
            principal_arns.append(principal_statement)
        return principal_arns

    def _fix_action_and_resource(self, resource_arn, statement):
        try:
            actions = statement["Action"]
            resources = statement["Resource"]
            if actions == "*" or (isinstance(actions, list) and actions.count("*") > 0):
                statement["Action"] = AwsArnUtils.get_service_from_arn(resource_arn) + ":*"
            if resources == "*" or (isinstance(resources, list) and resources.count("*") > 0):
                statement["Resource"] = [resource_arn]
        except Exception as e:
            logger.error(f"Error in fixing Action {actions} and Resource {resources} Resource Arn: {resource_arn}: {e}")
            raise e

    def _read_allow_statements(self, principal : str, statement : dict[str | dict], permissions_list : PermissionsList):
        glue_resources, s3_resources = self._filter_resources(statement["Resource"])
        glue_actions, s3_actions = self._filter_actions(statement["Action"])

        logger.debug(f"Adding permissions for {principal} on Glue: {glue_resources} : {glue_actions} and S3: {s3_resources} : {s3_actions}")

        # TODO: Split glue resources and filter acounts to Catalog, Database, Table
        for resource in glue_resources:
            for action in glue_actions:
                permissions_list.add_permission(principal, resource, action)

        for resource in s3_resources:
            for action in s3_actions:
                permissions_list.add_permission(principal, resource, action)

    def _read_deny_statements(self, principal : str, statement : dict[str | dict], permissions_list : PermissionsList):
        glue_resources, s3_resources = self._filter_resources(statement["Resource"])
        glue_actions, s3_actions = self._filter_actions(statement["Action"])

        logger.debug(f"Removing permissions (if exists) for {principal} on {glue_resources} : {glue_actions} and {s3_resources} : {s3_actions}")

        for resource in glue_resources:
            permissions_list.remove_permission(principal, resource, set(glue_actions))

        for resource in s3_resources:
            permissions_list.remove_permission(principal, resource, set(s3_actions))

    def _filter_resources(self, resource_arns : list[str] | str) -> tuple[list[str], list[str]]:
        glue_resources : list[str] = []
        s3_resources : list[str] = []
        if isinstance(resource_arns, list):
            for resource in resource_arns:
                self._filter_resource(glue_resources, s3_resources, resource)
        else:
            self._filter_resource(glue_resources, s3_resources, resource_arns)
        return glue_resources, s3_resources

    def _filter_resource(self, glue_resources : list[str], s3_resources : list[str], resource : str):
        if AwsArnUtils.isS3Arn(resource):
            if resource.endswith("*"):
                tables = self._s3_to_table_mapper.get_all_tables_from_s3_arn_prefix(resource[:-1])
                logger.debug(f"Resource is {resource} : Tables: {[table.get_database() + ":" + table.get_name() + ":" + table.get_location() for table in tables]}")
                s3_resources.extend([AwsArnUtils.get_s3_arn_from_s3_path(table.get_location()) + "*" for table in tables if table.get_location()])
        if AwsArnUtils.isGlueArn(resource):
            logger.debug(f"Is Glue Resource: {resource}")
            awsObject : AwsObject = AwsArnUtils.getAwsObjectFromArn(resource)
            if not awsObject:
                return
            catalog_id = awsObject.get_catalog_id()
            if isinstance(awsObject, GlueTable):
                logger.debug(f"Is Glue Table: {resource}")
                glueTable : GlueTable = awsObject
                # pylint: disable=no-value-for-parameter
                database = glueTable.get_database()
                table = glueTable.get_name()
                logger.debug(f"Catalog_id: {catalog_id} Database: {database} Table: {table}")
                tables = self._glue_data_catalog.get_resources_by_wildcard(catalog_id, database, table)
                glue_resources.extend([table.get_arn() for table in tables])
            elif isinstance(awsObject, GlueDatabase):
                logger.debug(f"Is Glue Database: {resource}")
                database = awsObject.get_name()
                databases = self._glue_data_catalog.get_resources_by_wildcard(catalog_id, database)
                glue_resources.extend([database.get_arn() for database in databases])

    def _filter_actions(self, actions) -> tuple[list[str], list[str]]:
        glue_actions : list[str] = []
        s3_actions : list[str] = []

        if isinstance(actions, list):
            for action in actions:
                self._filter_action(glue_actions, s3_actions, action)
        else:
            self._filter_action(glue_actions, s3_actions, actions)

        return glue_actions, s3_actions

    def _filter_action(self, glue_actions : list[GlueAction], s3_actions : list[S3Action], action : str):
        logger.debug(f"Filtering action: {action}")
        if action == "*":
            glue_actions.extend(GlueAction.get_glue_actions_with_wildcard(action))
            s3_actions.extend(S3Action.get_s3_actions_with_wildcard(action))
            return
        if action.startswith("s3:"):
            s3_actions.extend(S3Action.get_s3_actions_with_wildcard(action[3:]))
        if action.startswith("glue:"):
            glue_actions.extend(GlueAction.get_glue_actions_with_wildcard(action[5:]))

    def _is_valid_statement(self, statement : dict[str | dict]) -> bool:
        if "Action" not in statement or "Effect" not in statement or "Resource" not in statement:
            logger.debug(f"Invalid statement: {statement}")
            return False
        return True
