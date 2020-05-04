from steps.EtlStepBase import EtlStepBase
from utils.RedshiftDbQueryExecutor import RedshiftDbQueryExecutor
from infrastructure.InfrastructureSettings import InfrastructureSettings

class CreateSongPlayFactsEtlStep(EtlStepBase):
    """ Load the SongPlay Facts table

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
        print('start CreateSongPlayFactsEtlStep')

        dim_song_query = """ insert into dwh.fact_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, length, item_in_session, registration, status, auth)
            select distinct (TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' ) as start_time ,
            e.userid as user_id , e."level", sa.song_id, sa.artist_id, e.sessionid as session_id, e.location, e.useragent as user_agent,
            e.length, e.iteminsession, e.registration ,e.status, e.auth
            from staging.stg_events e left join 
                    ( select ds.song_id, da.artist_id, ds.title , da.name		 
                    from dwh.dim_songs ds full outer join dwh.dim_artists da
                        on ds.artist_id = da.artist_id)  as sa
                                    on upper(e.artist) = upper(sa.name) and (upper(e.song) = upper(sa.title) )
            where upper(e.page)= 'NEXTSONG'
                ; """

        self.redshiftDbQueryExecutor.executeQuery(dim_song_query)
        
        print('End CreateSongPlayFactsEtlStep')





