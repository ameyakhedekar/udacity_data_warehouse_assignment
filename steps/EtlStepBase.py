import boto3
import psycopg2
from attrdict import AttrDict
from infrastructure.InfrastructureSettings import InfrastructureSettings


class EtlStepBase:
    """ Base class to implement steps in the ETL process
    
    Parameters:
    services_config : config settings 
    infra_settings : infratructure settings 

    """
    def __init__(self, services_config, infra_settings : InfrastructureSettings ):
        self.services_config = services_config
        self.infra_settings = infra_settings

    def run(self):
        pass



        

