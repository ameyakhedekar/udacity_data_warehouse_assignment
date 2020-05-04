from steps.EtlStepBase import EtlStepBase
from utils.RedshiftDbQueryExecutor import RedshiftDbQueryExecutor
from infrastructure.InfrastructureSettings import InfrastructureSettings

class CreateArtistDimensionsEtlStep(EtlStepBase):
    """ Load the artist dimension table

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
        print('start CreateArtistDimensionsEtlStep')
        dim_artist_query = """ 
                    insert into dwh.dim_artists(artist_id ,name ,location ,latitude  ,longitude  )
                    select artist_id ,name ,location ,latitude  ,longitude  from 
                    (
                    select artist_id ,artist_name as name , artist_location as location, artist_latitude as  latitude  , artist_longitude  as longitude,
                    ROW_NUMBER() over (partition by artist_id order by year desc) as rownumber
                    from staging.stg_songs_data
                    )
                    where rownumber = 1;  """

        self.redshiftDbQueryExecutor.executeQuery(dim_artist_query)

        print('end CreateArtistDimensionsEtlStep')
        





