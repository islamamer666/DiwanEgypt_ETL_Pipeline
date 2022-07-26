import configparser
import logging
import psycopg2
from pathlib import Path

logger = logging.getLogger(__name__)
config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))


class DiwanEgyptDataWarehouse:

    def __init__(self):
        self._conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        self._cur = self._conn.cursor()

    def setup_staging_tables(self, create_staging_schema=None, create_staging_tables=None, drop_staging_tables=None):
        logging.debug("Creating schema for staging.")
        self.execute_query([create_staging_schema])

        logging.debug("Dropping Staging tables.")
        self.execute_query(drop_staging_tables)

        logging.debug("Creating Staging tables.")
        self.execute_query(create_staging_tables)

    def load_staging_tables(self, copy_staging_tables=None):
        logging.debug("Populating staging tables")
        self.execute_query(copy_staging_tables)

    def setup_warehouse_tables(self, create_warehouse_tables=None, create_warehouse_schema=None):
        logging.debug("Creating scheam for warehouse.")
        self.execute_query([create_warehouse_schema])

        logging.debug("Creating Warehouse tables.")
        self.execute_query(create_warehouse_tables)

    def perform_upsert(self, upsert_queries=None):
        logging.debug("Performing Upsert.")
        self.execute_query(upsert_queries)

    def execute_query(self, query_list):
        for query in query_list:
            print(query)
            logging.debug(f"Executing Query : {query}")
            self._cur.execute(query)
            self._conn.commit()
