import boto3
from botocore import errorfactory
import configparser
from botocore.exceptions import ClientError
import json


class InfraBootstrapper:
    """ Bootstrapper responsible for creating infrastructure

    Parameters:
    services_config : config settings 

    """

    def __init__(self, services_config : dict):
        self.services_config = services_config
        self.aws_key = self.services_config.get('AWS','KEY')
        self.aws_secret = self.services_config.get('AWS','SECRET')
        self.aws_region  = self.services_config.get('AWS','REGION')
    
    def init(self ):
        """ Create the infrastructure
        
        """

        etl_role_name = self.services_config.get('IAM_ROLE','ROLE_NAME')
        etl_role_arn = self.get_or_create_aws_role(etl_role_name)        

        cluster_type= self.services_config.get('CLUSTER','DWH_CLUSTER_TYPE')
        node_type = self.services_config.get('CLUSTER','DWH_NODE_TYPE')
        num_of_nodes = int(self.services_config.get('CLUSTER','DWH_NUM_NODES'))
        database_name = self.services_config.get('CLUSTER','DB_NAME') 
        cluster_identifier = self.services_config.get('CLUSTER','CLUSTER_IDENTIFIER') 
        master_username = self.services_config.get('CLUSTER','DB_USER') 
        master_userpassword = self.services_config.get('CLUSTER','DB_PASSWORD') 
        
        self.create_redshift_cluster(etl_role_arn, cluster_type, node_type, num_of_nodes, database_name, cluster_identifier,
            master_username ,master_userpassword )

        print( 'Infrastructure Created. Please wait till the cluster is ready')


    def get_or_create_aws_role(self, etl_role_name: str):
        """ Create the aws IAM Role

        Parameters:
        etl_role_name : Role name to create the new rolw
        
        """
        iam = boto3.client('iam', aws_access_key_id=self.aws_key,
                       aws_secret_access_key=self.aws_secret, region_name=self.aws_region )

        self.create_aws_role(iam, etl_role_name)
        
        roleArn = iam.get_role(RoleName=etl_role_name)['Role']['Arn']
        return roleArn

    def create_aws_role(self, iam, etl_role_name: str):
        """ Create the aws IAM Role

        Parameters:
        iam : aws IAM boto3 client
        etl_role_name : Role name to create the new rolw
        
        """
        try:
            iam.create_role(
                Path='/',
                RoleName=etl_role_name,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'}))

            iam.attach_role_policy(RoleName=etl_role_name,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']
            print('Role {0} created'.format(etl_role_name))
        except iam.exceptions.EntityAlreadyExistsException as ex: 
            print('AWS Role already exists')
        except Exception as ex:
            print(ex)
            raise
            

    def create_redshift_cluster(self, etl_role_arn : str, cluster_type: str, node_type : str, num_of_nodes :int , database_name: str, cluster_identifier: str
                , master_username: str, master_userpassword: str ):
        """ Create the redshift cluster

        Parameters:
        etl_role_arn : Role arn to use
        cluster_type : cluster_type to create the redshift cluster.
        node_type : The instance to use while creating cluster
        num_of_nodes : Number of nodes in the cluster
        database_name: Redshift database name
        cluster_identifier : Cluster identifier in the account
        master_username: username to connect to the database
        master_userpassword: password to connect to the database
        
        """
        try:            
            redshift = boto3.client('redshift', aws_access_key_id=self.aws_key,
                       aws_secret_access_key=self.aws_secret, region_name=self.aws_region ) 
            redshift.create_cluster(     
                ClusterType=cluster_type,
                NodeType=node_type,
                NumberOfNodes=num_of_nodes,
                DBName=database_name,
                ClusterIdentifier=cluster_identifier,
                MasterUsername=master_username,
                MasterUserPassword=master_userpassword,        
                IamRoles=[etl_role_arn]  )
            print('Redshift Cluster {0} created'.format(cluster_identifier))

        except Exception as ex:
            print('Cluster already created')

    def drop_infrastucture(self):
        """ Tear down the infrastructure
        1. tears down redshift cluster.

        """
        try:
            redshift = boto3.client('redshift', aws_access_key_id=self.aws_key,
                        aws_secret_access_key=self.aws_secret, region_name=self.aws_region ) 
            response = redshift.delete_cluster(
                    ClusterIdentifier= self.services_config.get('CLUSTER','CLUSTER_IDENTIFIER') ,
                    SkipFinalClusterSnapshot=True)
            print('Cluster deleted')
        except Exception as ex:
            print('Cluster already deleted')
        
    
    
