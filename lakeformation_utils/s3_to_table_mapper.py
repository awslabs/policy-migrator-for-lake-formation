from aws_resources.glue_data_catalog import GlueDataCatalog
from aws_resources.glue_table import GlueTable
from aws_resources.aws_arn_utils import AwsArnUtils
from .s3_tree import S3Tree
from .tree_node import TreeNode
import logging

logger = logging.getLogger(__name__)

class S3ToTableMapper:
    '''
        Converts S3 path to GlueTables.
    '''

    def __init__(self, glueDataCatalog : GlueDataCatalog):
        self._s3_tree = S3Tree()

        #Loop through glueDataCatalog and filter by tables. Take the tables location and 
        #put it in our _s3_to_table_map
        for table in glueDataCatalog.get_tables():
            if table.get_location() is not None and table.get_location().startswith("s3://"):
                self._s3_tree.add_path(table.get_location(), table)

    def get_tables_from_s3_arn_postfix(self, s3_arn : str) -> list[GlueTable]:
        '''
        Does a backwards look up for tables in which an S3 Path exists in.
        eg. s3://bucket/path1/path2/path3/path4/ will find tables with locations to the first level:
        Table1: s3://bucket/path1/path2/path3/
        '''
        s3_path = AwsArnUtils.get_s3_path_from_arn(s3_arn)
        if not s3_path.endswith("/"):
            s3_path += "/"
        return self.get_tables_from_s3_location_postfix(s3_path)

    def get_tables_from_s3_location_postfix(self, s3_path : str) -> list[GlueTable]:
        '''
        Does a backwards look up for tables in which an S3 Path exists in.
        eg. s3://bucket/path1/path2/path3/path4/ will find tables with locations to the first level:
        Table1: s3://bucket/path1/path2/path3/
        '''
        node : TreeNode = self._s3_tree.get_last_node_from_path(s3_path)
        if node is not None:
            return list(node.get_values())
        return []

    def get_all_tables_from_s3_path_prefix(self, s3_path : str) -> list[GlueTable]:
        '''
        Gets all the tables that have the table locations that are prefixed the S3 path,
        ie it does a recursive look up for all tables.
        eg. s3://bucket/path1/path2/path3/ will find tables all tables under this path:
        Table1: s3://bucket/path1/path2/path3/path4/
        Table2: s3://bucket/path1/path2/path3/path5/
        '''
        if s3_path.startswith("arn:"):
            raise ValueError("This function does not take in ARNs. Call get_tables_from_s3_arn instead.")
        return self._s3_tree.get_all_subtree_values_from_path(s3_path)

    def get_all_tables_from_s3_arn_prefix(self, s3_arn : str) -> list[GlueTable]:
        '''
        Gets all the tables that have the table locations that are prefixed the S3 path,
        ie it does a recursive look up for all tables.
        eg. s3://bucket/path1/path2/path3/ will find tables all tables under this path:
        Table1: s3://bucket/path1/path2/path3/path4/
        Table2: s3://bucket/path1/path2/path3/path5/
        '''
        s3_path = AwsArnUtils.get_s3_path_from_arn(s3_arn)
        if not s3_path.endswith("/"):
            s3_path += "/"
        logger.debug(f"get_all_tables_from_s3_arn_prefix: S3 Path: {s3_path}")
        return self.get_all_tables_from_s3_path_prefix(s3_path)

    def get_all_tables(self) -> list[GlueTable]:
        return self._s3_tree.get_all_subtree_values_from_path("s3://")
