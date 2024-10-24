
import boto3
import json

class GlueResourcePolicyReader:
    '''
        GlueResourcePolicyReader Gets all of the the catalogs resource policy as a single policy to be able to
        pass to IAM Policy Simulator.
    '''

    def __init__(self, boto3Session : boto3.Session):
        self._glue_client = boto3Session.client('glue')

    def get_glue_resource_policy(self) -> str:
        '''
            Returns the glue resource policy as a single policy to be able to pass to IAM Policy Simulator.
        '''
        totalPolicy = {}
        totalPolicy['Version'] = '2012-10-17'
        totalPolicy['Statement'] = []
        iterator = self._glue_client.get_paginator('get_resource_policies')
        for page in iterator.paginate():
            for policy in page["GetResourcePoliciesResponseList"]:
                subPolicies = json.loads(policy["PolicyInJson"])["Statement"]
                for subPolicy in subPolicies:
                    totalPolicy["Statement"].append(subPolicy)

        return json.dumps(totalPolicy, indent=0, separators=(',', ':'))