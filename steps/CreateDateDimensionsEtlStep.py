from steps.EtlStepBase import EtlStepBase
from utils.RedshiftDbQueryExecutor import RedshiftDbQueryExecutor
from infrastructure.InfrastructureSettings import InfrastructureSettings


class CreateDateDimensionsEtlStep(EtlStepBase):
    """ Load the date dimension table

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
        print('start CreateDateDimensionsEtlStep')
        dim_time_query = """
        insert into dwh.dim_time (start_time, weekday, year, month, week, day, hour)
            select distinct start_time,
            extract (weekday from start_time) as weekday, extract (year from start_time) as year, 
            extract (month from start_time) as month , extract (week from start_time) as week, extract (day from start_time) as day,
            extract (hour from start_time) as hour from
            dwh.fact_songplays

        
        """

        self.redshiftDbQueryExecutor.executeQuery(dim_time_query)
        print('end CreateDateDimensionsEtlStep')
        





