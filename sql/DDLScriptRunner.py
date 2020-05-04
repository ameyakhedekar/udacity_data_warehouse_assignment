import configparser
import psycopg2
from sql.ddl_queries import ddl_schema_queries, drop_tables, ddl_table_queries
from infrastructure.InfrastructureSettings import InfrastructureSettings


class DDLScriptRunner():
    """ Responsible to prepare schema, tables on the Redshift cluster

    Parameters:
    services_config : config settings 

    """

    def init(self, config):
        """ prepare the Redshift cluster 

        """
        infra_settings = InfrastructureSettings(config)
        conn = psycopg2.connect(infra_settings.redshift_connection_string)
        cur = conn.cursor()

        self.run_ddl_schema(cur, conn)
        self.run_drop_tables(cur, conn)
        self.run_ddl_tables(cur, conn)    

        conn.close()

    def run_ddl_schema(self,cur, conn):
        """ Create Schema on the cluster
        """
        for query in ddl_schema_queries:
            cur.execute(query)
            conn.commit()

    def run_drop_tables(self,cur, conn):
        """ Clean up tables
        """
        for query in drop_tables:
            cur.execute(query)
            conn.commit()


    def run_ddl_tables(self, cur, conn):
        """ Create tables on the cluster
        """
        for query in ddl_table_queries:
            cur.execute(query)
            conn.commit()

