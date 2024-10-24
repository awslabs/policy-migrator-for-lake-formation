
import boto3
import logging

logger = logging.getLogger(__name__)

class IamPolicyReader():
    '''
        Limitations: 
        - IAM Users will not have group policies.
    '''

    def __init__(self, boto3Session : boto3.Session):
        self._iam_client = boto3Session.client('iam')
        self._iam_policies : dict[str, dict] = {}
        self._iam_principal_policies : dict [str, list[dict]] = {}
        self._iam_role_arns : list[str] = []
        self._iam_user_arns : list[str] = []
        self._iam_group_policies : dict[str, dict] = {}

        self._initialized : bool = False

    def get_iam_policies_for_prinicpal(self, iam_principal_arn : str) -> list[str]:
        self._read_policies()
        return self._iam_principal_policies.get(iam_principal_arn, [])

    def get_all_prinicial_policies(self) -> iter:
        self._read_policies()
        return iter(self._iam_principal_policies.items())

    def get_all_principal_arns(self) -> tuple[list[str], list[str]]:
        self._read_policies()
        return self._iam_user_arns, self._iam_role_arns

    def _read_policies(self):
        if self._initialized:
            return
        self._read_iam_role_policies()
        self._read_iam_user_policies()
        self._initialized = True

    def _read_iam_role_policies(self):
        logger.debug("Reading IAM role policies.")
        role_paginator = self._iam_client.get_paginator('list_roles')
        for page in role_paginator.paginate():
            for role in page['Roles']:
                # Skip service roles
                if "/service-role/" in role['Arn']:
                    continue
                logger.debug(f"Reading for Role: {role['Arn']}")
                self._iam_role_arns.append(role['Arn'])
                self._read_role_policies_for_role(role)

    def _read_role_policies_for_role(self, role):
        role_name = role['RoleName']
        role_arn = role['Arn']

        #Get attached managed policies
        attached_role_policy_paginator = self._iam_client.get_paginator('list_attached_role_policies')
        for attached_role_policy in attached_role_policy_paginator.paginate(RoleName=role_name):
            for policy in attached_role_policy['AttachedPolicies']:
                policy_arn = policy['PolicyArn']
                if policy_arn not in self._iam_policies:
                    self._read_policy(policy_arn)
                self._iam_principal_policies.setdefault(role_arn, []).append(self._iam_policies[policy_arn])

        #Get inline policies for role
        inline_role_policy_paginator = self._iam_client.get_paginator('list_role_policies')
        for inline_role_policy_page in inline_role_policy_paginator.paginate(RoleName=role_name):
            for policy_name in inline_role_policy_page['PolicyNames']:
                policy_response = self._iam_client.get_role_policy(RoleName = role_name, PolicyName = policy_name)
                self._iam_principal_policies.setdefault(role['Arn'], []).append(policy_response['PolicyDocument'])

    def _read_policy(self, policy_arn):
        policy = self._iam_client.get_policy(PolicyArn = policy_arn)
        policy_document = self._iam_client.get_policy_version(PolicyArn = policy_arn, VersionId = policy['Policy']['DefaultVersionId'])
        self._iam_policies[policy_arn] = policy_document['PolicyVersion']['Document']

    def _read_iam_user_policies(self):
        logger.debug("Reading IAM user policies.")
        user_paginator = self._iam_client.get_paginator('list_users')
        for page in user_paginator.paginate():
            logger.debug(f"Reading for User: {page['Users'][0]['UserName']}")
            for user in page['Users']:
                self._iam_user_arns.append(user['Arn'])
                self._read_iam_user_policies_for_user(user['UserName'], user['Arn'])

    def _read_iam_user_policies_for_user(self, username, user_arn):
        #Get inline policies for user
        user_policy_paginator = self._iam_client.get_paginator('list_user_policies')
        for user_policy_page in user_policy_paginator.paginate(UserName = username):
            for policy_name in user_policy_page['PolicyNames']:
                policy_response = self._iam_client.get_user_policy(UserName = username, PolicyName = policy_name)
                self._iam_principal_policies.setdefault(user_arn, []).append(policy_response['PolicyDocument'])

        #Get attached managed policies
        attached_iam_policy_paginator = self._iam_client.get_paginator('list_attached_user_policies')
        for attached_role_policy in attached_iam_policy_paginator.paginate(UserName=username):
            for policy in attached_role_policy['AttachedPolicies']:
                policy_arn = policy['PolicyArn']
                if policy_arn not in self._iam_policies:
                    self._read_policy(policy_arn)
                self._iam_principal_policies.setdefault(user_arn, []).append(self._iam_policies[policy_arn])

        #Get group policies for user
        user_group_paginator = self._iam_client.get_paginator('list_groups_for_user')
        for user_group_page in user_group_paginator.paginate(UserName = username):
            for group in user_group_page['Groups']:
                group_arn = group['Arn']
                group_name = group['GroupName']
                self._read_iam_user_policies_for_group(group_name, group_arn)
                self._iam_principal_policies.setdefault(user_arn, []).extend(self._iam_group_policies.get(group_arn, []))

    def _read_iam_user_policies_for_group(self, group_name, group_arn) -> list:
        if group_arn in self._iam_group_policies:
            return

        group_policy_paginator = self._iam_client.get_paginator('list_group_policies')
        for group_policy_page in group_policy_paginator.paginate(GroupName = group_name):
            for policy_name in group_policy_page['PolicyNames']:
                policy_response = self._iam_client.get_group_policy(GroupName = group_name, PolicyName = policy_name)
                self._iam_group_policies.setdefault(group_arn, []).append(policy_response['PolicyDocument'])
