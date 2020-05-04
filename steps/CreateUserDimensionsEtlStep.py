from steps.EtlStepBase import EtlStepBase
from utils.RedshiftDbQueryExecutor import RedshiftDbQueryExecutor
from infrastructure.InfrastructureSettings import InfrastructureSettings

class CreateUserDimensionsEtlStep(EtlStepBase):
    """ Load the user dimension table

    Parameters:
    services_config : config settings 
    infra_settings : infratructure settings 

    """

    def __init__(self, services_config, infra_settings : InfrastructureSettings ):
        super().__init__(services_config, infra_settings)        
        self.redshiftDbQueryExecutor = RedshiftDbQueryExecutor(infra_settings)
        
    def run(self):
        """ Run the step- execute query 
    
        """

        print('start CreateUserDimensionsEtlStep')

        dim_song_query = """insert into dwh.dim_users(user_id,first_name, last_name, gender, "level" )
                select user_id,first_name, last_name, gender, "level" from (
                select userid as user_id, firstname as first_name, lastname as last_name, gender, level , 
                    ROW_NUMBER() over (partition by userid order by ts desc) as rownumber
                from staging.stg_events
                where user_id is not null and upper(page)= 'NEXTSONG')
                where rownumber = 1;"""

        self.redshiftDbQueryExecutor.executeQuery(dim_song_query)
        print('end CreateUserDimensionsEtlStep')





