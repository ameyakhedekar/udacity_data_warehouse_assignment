import boto3
import psycopg2
from attrdict import AttrDict
from infrastructure.InfrastructureSettings import InfrastructureSettings
from utils.RedshiftStagingLoader import RedshiftStagingLoader

from steps.EtlStepBase import EtlStepBase
from urllib.parse import urlparse
import boto3
import json

class SongStagingLoadEtlStep(EtlStepBase):
    """ Load the song dimension table

    Parameters:
    services_config : config settings 
    infra_settings : infratructure settings 

    """

    def __init__(self, services_config, infra_settings : InfrastructureSettings ,redshiftStagingLoader: RedshiftStagingLoader):
        super().__init__(services_config, infra_settings)        
        self.redshiftStagingLoader = redshiftStagingLoader
        self.aws_key = self.services_config.get('AWS','KEY')
        self.aws_secret = self.services_config.get('AWS','SECRET')
        self.aws_region  = self.services_config.get('AWS','REGION')        

    def run(self):
        """ Run the step- execute query 
    
        """
        print("Start SongStagingLoadEtlStep")
        loader_attr = AttrDict({"s3_path": self.services_config.get('S3','SONG_DATA'),"jsonpath" : self.services_config.get('S3','SONG_JSONPATH'), "is_manifest": False})
        self.redshiftStagingLoader.loadIntoRedshift("staging.stg_songs_data", self.infra_settings.RoleArn, self.infra_settings.S3DataRegion, loader_attr)
        print("End SongStagingLoadEtlStep")

        




