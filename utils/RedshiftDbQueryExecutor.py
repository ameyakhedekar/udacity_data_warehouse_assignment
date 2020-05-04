import boto3
import psycopg2
from attrdict import AttrDict

from infrastructure.InfrastructureSettings import InfrastructureSettings
        

class RedshiftDbQueryExecutor():
    """ Helper class to execute sql query on the redshift cluster

    """

    def __init__(self, settings : InfrastructureSettings):
        self.settings = settings
        
        
    def executeQuery(self, query: str):
        """ Execute the given query 

        Parameters:
        query : Query to execute
        """

        with  psycopg2.connect(self.settings.redshift_connection_string) as conn:
            cur = conn.cursor()

            try:                
                cur.execute(query)
                cur.close()
            except Exception as ex:
                print(ex)                
                raise
            

