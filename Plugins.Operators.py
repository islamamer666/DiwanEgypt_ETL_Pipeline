from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        if tables is None:
            tables = []
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for table in self.tables:

            self.log.info("Starting data quality validation on table : {}".format(table))
            records = redshift_hook.get_records("select count(*) from {};".format(table))

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error("Data Quality validation failed for table : {}.".format(table))
                raise ValueError("Data Quality validation failed for table : {}".format(table))
            self.log.info("Data Quality Validation Passed on table : {}!!!".format(table))


class LoadAnalyticsOperator(BaseOperator):
        ui_color = '#80BD9E'

     @apply_defaults
    def __init__(self,
                     redshift_conn_id="",
                 sql_query=None,
                     *args, **kwargs):
            super(LoadAnalyticsOperator, self).__init__(*args, **kwargs)
            if sql_query is None:
                sql_query = []
            self.redshift_conn_id = redshift_conn_id
            self.sql_query = sql_query

        def execute(self):
            redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            for query in self.sql_query:
                self.log.info("Running Analytics query :  {}".format(query))
                redshift_hook.run(self.sql_query)
                self.log.info("Query ran successfully!!")

