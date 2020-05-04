import boto3
import psycopg2
from attrdict import AttrDict
from infrastructure.InfrastructureSettings import InfrastructureSettings
from utils.RedshiftStagingLoader import RedshiftStagingLoader
from steps.EtlStepBase import EtlStepBase

class LogDataStagingLoadEtlStep(EtlStepBase):
    """ Load the event log staging  table

    Parameters:
    services_config : config settings 
    infra_settings : infratructure settings 

    """

    def __init__(self, services_config, infra_settings : InfrastructureSettings ,redshiftStagingLoader: RedshiftStagingLoader):
        super().__init__(services_config, infra_settings)        
        self.redshiftStagingLoader = redshiftStagingLoader

    def run(self):
        """ Run the step- execute query 
    
        """
        print("Start LogDataStagingLoadEtlStep")
        loader_attr = AttrDict({"s3_path": self.services_config.get('S3','LOG_DATA'),"jsonpath" : self.services_config.get('S3','LOG_JSONPATH') , "is_manifest" : False})
        self.redshiftStagingLoader.loadIntoRedshift("staging.stg_events", self.infra_settings.RoleArn, self.infra_settings.S3DataRegion, loader_attr)
        print(" End LogDataStagingLoadEtlStep")

        
