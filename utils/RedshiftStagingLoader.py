import boto3
import psycopg2
from attrdict import AttrDict
from utils.RedshiftDbQueryExecutor import RedshiftDbQueryExecutor

from infrastructure.InfrastructureSettings import InfrastructureSettings


class RedshiftStagingLoader:
    """ Abstract class to load into Redshift table
    
    """

    def loadIntoRedshift(self, staging_table_name :str, aws_iam_role :str, region :str, source: dict):
        pass
        



class RedshiftJsonS3StagingLoader(RedshiftStagingLoader):
    """ Implemented class to load redshift table from data files from S3

    Parameters:
    settings : infratructure settings 

    """

    def __init__(self, settings : InfrastructureSettings):
        self.redshiftDbQueryExecutor = RedshiftDbQueryExecutor(settings)

    def loadIntoRedshift(self, staging_table_name :str, aws_iam_role :str, region :str, source: AttrDict):
        """ Load the source passed into redshift table 

        Parameters:
        staging_table_name : target table 
        aws_iam_role : IAM role to be used for load
        region : AWS Region
        source : dictionary for source paramters
        """

        try:
            SQL_COPY = """            
            truncate table {staging_table_name};
            copy {staging_table_name} from '{s3_path}' 
            iam_role '{aws_iam_role}'            
             region '{region}'
             json '{jsonpath}'
             ACCEPTINVCHARS
             COMPUPDATE OFF
             
                    """.format(**{"staging_table_name": staging_table_name, "aws_iam_role" :aws_iam_role, "s3_path" : source.s3_path, "region": region
                    , "jsonpath" : source.jsonpath, "manifest" : ("MANIFEST"  if source.is_manifest == True else "")})

            
            self.redshiftDbQueryExecutor.executeQuery(SQL_COPY)            
            print('Uploaded table: {0} from S3 Path: {1}'.format(staging_table_name, source.s3_path))
        except Exception as ex:
            print(ex)
            raise
        

