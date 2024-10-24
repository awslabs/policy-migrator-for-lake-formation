from aws_resources.aws_arn_utils import AwsArnUtils
from permissions.permissions_list import PermissionsList

from .policy_filter_interface import PolicyFilterInterface

from config.application_configuration import ApplicationConfiguration

import boto3
import logging
import math

logger = logging.getLogger(__name__)
class IAMPolicySimulatorValidator(PolicyFilterInterface):
    '''
    Limitations: 
     - Does not take into account of cross account permissions
     - Does not use Glue Resource Policies or S3 Bucket policies to validate permissions.
       This is going to be done via a different filter.

    '''

    _REQUIRED_CONFIGURATION = {}
    _CONFIGURATION_SECTION = "policy_filter_iam_policy_validator"

    _iam_client : boto3.client = None
    _glue_client : boto3.client = None
    _s3_client : boto3.client = None
    _permissions_list : PermissionsList

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        super().__init__(appConfig, conf)
        boto3Session = appConfig.get_boto3_session()
        self._iam_client = boto3Session.client('iam')
        self._glue_client = boto3Session.client('glue')
        self._s3_client = boto3Session.client('s3')

    def filter_policies(self, permissionsList : PermissionsList):
        self._validate_glue_policies(permissionsList)
        return self.get_filtered_permissions()
        #validate_s3_policies()

    def _validate_glue_policies(self, permissionsList : PermissionsList) -> PermissionsList:
        logger.debug("Validating Glue policies.")

        #glue_resource_policy = self._getGlueResourcePolicy()
        glue_resource_policy = None

        #Need to split the catalog level, database level and table level permissions
        catalog_permissions, database_permissions, table_permissions = self._split_glue_permissions(permissionsList)

        self._validate_permissions(catalog_permissions, glue_resource_policy)
        self._validate_permissions(database_permissions, glue_resource_policy)
        self._validate_permissions(table_permissions, glue_resource_policy)

        return self.get_filtered_permissions()

    def _validate_s3_policies(self, permissionsList : PermissionsList) -> PermissionsList:
        logger.debug("Validating S3 policies.")

        #glue_resource_policy = self._getGlueResourcePolicy()
        s3_resource_policy = None

        self._validate_permissions(permissionsList, s3_resource_policy)

    def _validate_permissions(self, permissionsList : PermissionsList, resource_policy: str) -> int:
        ''' This will call three times (Permission Resource Type) per principal with all resources and actions found. We will remove anything from
            our permissions list if its denied. 

            - returns number of permissions that didn't meet validation.
            '''

        for principal_arn in permissionsList.get_principal_arns():
            logger.debug(f"Validating permissions for principal: {principal_arn}")
            permsForPrincipal = list(permissionsList.get_permissions_for_principal(principal_arn))

            resource_arns, action_names = self._get_resources_actions(permsForPrincipal)
            if not resource_arns or not action_names:
                continue

            resource_arns_pages = [resource_arns]
            # May have to split up the requests into multiple requests becasue validator only accepts up to 1000 combined actions and resources
            if len(resource_arns) * len(action_names) > 1000:
                num_pages = math.ceil(1000 / len(action_names))
                page_size = math.floor(len(resource_arns) / num_pages)
                resource_arns_pages = [resource_arns[i * page_size: min((i + 1) * page_size, len(resource_arns))] for i in range(0, num_pages + 1)]
                logger.debug(f"Splitting resource arns into {num_pages} pages with {page_size} number of resources per page because there are {len(action_names)} actions. ")

            for resource_arns_page in resource_arns_pages:
                paginator = self._iam_client.get_paginator('simulate_principal_policy')
                logger.debug(f"Submitting {len(resource_arns_page)} resource_arns out of {len(resource_arns)}. Resources: {resource_arns_page}")
                #TODO: Pass in the resource_policy into paginator
                try:
                    for page in paginator.paginate(PolicySourceArn=principal_arn, ActionNames=action_names, ResourceArns=resource_arns_page):
                        for result in page["EvaluationResults"]:
                            self._inspect_simulation_result(result, principal_arn, permsForPrincipal)
                    logger.debug("Completed iterating through all resource_arns")
                except Exception as e:
                    logger.error(f"Error validating permissions for principal: {principal_arn}. Error: {e}")
                    logger.error(f"{permissionsList}")

    def _inspect_simulation_result(self, result, principal_arn, permsForPrincipal) -> None:
        if 'EvalDecision' not in result:
            logger.info(f"EvalDecision is Null! {result}.")
        if result['EvalDecision'] != 'allowed':
                            #if there isn't any ResourceSpecificResults, that means the IAM principal has NO permissions on any resources.
            if 'ResourceSpecificResults' in result:
                for resource_decision in result['ResourceSpecificResults']:
                    if resource_decision['EvalResourceDecision'] != 'allowed':
                        self._add_filtered_permission(principal_arn, resource_decision["EvalResourceName"], result['EvalActionName'])
            else:
                self._remove_action_from_principal(permsForPrincipal, result['EvalActionName'])

    def _get_resources_actions(self, permissions : PermissionsList) -> tuple[list, list]:
        action_names = set()
        resource_arns = []
        for permission in permissions:
            action_names.update(permission.permission_actions())
            resource_arns.append(permission.resource_arn())
        return (resource_arns, list(action_names))

    def _remove_action_from_principal(self, permsForPrincipal, action_to_filter) -> None:
        for perm in permsForPrincipal:
            if action_to_filter in perm.permission_actions():
                self._add_filtered_permission(perm.principal_arn(), perm.resource_arn(), action_to_filter)

    def _getS3ResourcePolicy(self, s3_bucket_arn) -> str:
        '''
            _getS3ResourcePolicy Gets all of the the S3 resource policies as a single policy to be able to
            pass to IAM Policy Simulator.
        '''
        try:
            response = self._s3_client.get_bucket_policies(s3_bucket_arn)
            return response['Policy']
        except:
            logger.debug(f"Could not get S3 bucket policy for Bucket: {s3_bucket_arn}")
            return None

    def _split_glue_permissions(self, permissionsList : PermissionsList) -> tuple[PermissionsList, PermissionsList, PermissionsList]:
        catalog_permissions = PermissionsList()
        database_permissions = PermissionsList()
        table_permissions = PermissionsList()
        for permission in permissionsList:
            if AwsArnUtils.isGlueCatalogArn(permission.resource_arn()):
                catalog_permissions.add_permission_record(permission)
            elif AwsArnUtils.isGlueDatabaseArn(permission.resource_arn()):
                database_permissions.add_permission_record(permission)
            elif AwsArnUtils.isGlueTableArn(permission.resource_arn()):
                table_permissions.add_permission_record(permission)
        return (catalog_permissions, database_permissions, table_permissions)

    @classmethod
    def get_name(cls) -> str:
        return IAMPolicySimulatorValidator.__name__

    @classmethod
    def get_required_configuration(cls) -> dict:
        return IAMPolicySimulatorValidator._REQUIRED_CONFIGURATION

    @classmethod
    def get_config_section(cls) -> str:
        return IAMPolicySimulatorValidator._CONFIGURATION_SECTION
