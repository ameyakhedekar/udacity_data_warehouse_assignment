import boto3
import configparser
from botocore.exceptions import ClientError
import json
import time

class InfrastructureSettings:   
    """ Responsible to store the settings of the cluster

    Parameters:
    services_config : config settings 

    """

    def __init__(self, services_config):
        self.services_config = services_config
        self.aws_key = self.services_config.get('AWS','KEY')
        self.aws_secret = self.services_config.get('AWS','SECRET')
        self.aws_region  = self.services_config.get('AWS','REGION')
        self.initialize_settings()

    
    @property
    def RoleArn(self):
        return self.role_arn

    @property
    def S3DataRegion(self):
        return self.s3DataRegion

    @property
    def redshift_connection_string(self):
        return self.rs_connection_string

    def initialize_settings(self):
        """ Initialize the properties of the settings object       

        """

        try:            

            cluster_identifier = self.services_config.get('CLUSTER','CLUSTER_IDENTIFIER') 
            cluster_props = self.get_cluster_properties(cluster_identifier) 
            
            database_name = self.services_config.get('CLUSTER','DB_NAME')             
            master_username = self.services_config.get('CLUSTER','DB_USER') 
            rs_port = self.services_config.get('CLUSTER','DB_PORT') 
            master_userpassword = self.services_config.get('CLUSTER','DB_PASSWORD') 
            endpoint = cluster_props['Endpoint']['Address']
            role_arn = cluster_props['IamRoles'][0]['IamRoleArn']
            
            self.rs_connection_string = "postgresql://{}:{}@{}:{}/{}".format(master_username, master_userpassword, endpoint, rs_port,database_name)
            self.role_arn = role_arn

            self.s3DataRegion = self.services_config.get('S3','DATA_REGION') 
            
        except Exception as ex:
            print("Cluster not present: " + str(ex))
            raise
    
    def get_cluster_properties(self, cluster_identifier):
        """ Get Cluster properties of the Redshift cluster

        """

        try:
            redshift = boto3.client('redshift', aws_access_key_id=self.aws_key, aws_secret_access_key=self.aws_secret, region_name=self.aws_region )

            cluster_props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]            
            status = cluster_props['ClusterStatus']
            while (status != 'available'):
                if (status == 'deleting'):
                    raise Exception('Cluster is being deleted. Retry with a new cluster name, or return in some time')
                    

                print ('Waiting for cluster to be ready...(Checking in 20 secs)')
                time.sleep(20)
                cluster_props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
                status = cluster_props['ClusterStatus']
            return cluster_props
        except Exception as ex:
            print("Cluster not present: " + str(ex))
            raise


    



  

