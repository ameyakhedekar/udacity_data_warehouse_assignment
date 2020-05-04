from steps.EtlStepBase import EtlStepBase
from utils.RedshiftDbQueryExecutor import RedshiftDbQueryExecutor
from infrastructure.InfrastructureSettings import InfrastructureSettings

class CreateSongDimensionsEtlStep(EtlStepBase):
    """ Load the song dimension table

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
        print('start CreateSongDimensionsEtlStep')

        dim_song_query = """insert into  dwh.dim_songs(song_id ,title ,artist_id ,year ,duration)
                            select distinct song_id ,title ,artist_id ,year ,duration 
                            from staging.stg_songs_data"""

        self.redshiftDbQueryExecutor.executeQuery(dim_song_query)
        print('start CreateSongDimensionsEtlStep')
        





