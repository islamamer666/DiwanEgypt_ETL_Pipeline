from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.goodreads_plugin import DataQualityOperator
from airflow.operators.goodreads_plugin import LoadAnalyticsOperator
from helpers import AnalyticsQueries

default_args = {
    'owner': 'Wikibooks',
    'depends_on_past': True,
    'start_date': datetime(2020, 2, 19, 0, 0, 0, 0),
    'end_date': datetime(2020, 2, 20, 0, 0, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('Wikibooks_pipeline',
          default_args=default_args,
          description='Load and Transform data from landing zone to processed zone. Populate data from Processed zone '
                      'to Wikibooks Warehouse.', 
          schedule_interval='None',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

emrsshHook = SSHHook(ssh_conn_id='emr_ssh_connection')

WikibooksETLJob = SSHOperator(
    task_id="WikibooksETLJob",
    command='cd /home/hadoop/Wikibooks_etl_pipeline/src;export PYSPARK_DRIVER_PYTHON=python3;export PYSPARK_PYTHON=python3;spark-submit --master yarn Wikibook_driver.py;',
    ssh_hook=emrsshHook,
    dag=dag)

warehouse_data_quality_checks = DataQualityOperator(
    task_id='Warehouse_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.de_table', 'Wikibooks_warehouse.en_table', 'Wikibooks_warehouse.es_table',
            'Wikibooks_warehouse.fr_table', 'Wikibooks_warehouse.he_table', 'Wikibooks_warehouse.hu_table',
            'Wikibooks_warehouse.it_table', 'Wikibooks_warehouse.ja_table', 'Wikibooks_warehouse.nl_table',
            'Wikibooks_warehouse.pl_table', 'Wikibooks_warehouse.pt_table', 'Wikibooks_warehouse.ru_table']
)

create_analytics_schema = LoadAnalyticsOperator(
    task_id='Create_analytics_schema',
    redshift_conn_id='redshift',
    sql_query=[AnalyticsQueries.create_schema],
    dag=dag
)

create_de_table = LoadAnalyticsOperator(
    task_id='Create_de_table',
    redshift_conn_id='redshift',
    dag=dag
)

create_en_table = LoadAnalyticsOperator(
    task_id='Create_en_table',
    redshift_conn_id='redshift',
    dag=dag
)

create_es_table = LoadAnalyticsOperator(
    task_id='Create_es_table',
    redshift_conn_id='redshift',
    dag=dag
)

create_fr_table = LoadAnalyticsOperator(
    task_id='Create_fr_table',
    redshift_conn_id='redshift',
    dag=dag
)

create_ne_table = LoadAnalyticsOperator(
    task_id='Create_ne_table',
    redshift_conn_id='redshift',
    dag=dag
)

create_hu_table = LoadAnalyticsOperator(
    task_id='Create_hu_table',
    redshift_conn_id='redshift',
    dag=dag
)

create_it_table = LoadAnalyticsOperator(
    task_id='Create_it_table',
    redshift_conn_id='redshift',
    dag=dag
)

create_ja_table = LoadAnalyticsOperator(
    task_id='Create_ja_table',
    redshift_conn_id='redshift',
    dag=dag
)

create_nl_table = LoadAnalyticsOperator(
    task_id='Create_nl_table',
    redshift_conn_id='redshift',
    dag=dag
)

create_pl_table = LoadAnalyticsOperator(
    task_id='Create_pl_table',
    redshift_conn_id='redshift',
    dag=dag
)

create_pt_table = LoadAnalyticsOperator(
    task_id='Create_pt_table',
    redshift_conn_id='redshift',
    dag=dag
)

create_ru_table = LoadAnalyticsOperator(
    task_id='Create_ru_table',
    redshift_conn_id='redshift',
    dag=dag
)

de_data_quality_checks = DataQualityOperator(
    task_id='de_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.de_table']
)

en_data_quality_checks = DataQualityOperator(
    task_id='en_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.en_table']
)

es_data_quality_checks = DataQualityOperator(
    task_id='es_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.es_table']
)

fr_data_quality_checks = DataQualityOperator(
    task_id='fr_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.fr_table']
)

ne_data_quality_checks = DataQualityOperator(
    task_id='ne_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.ne_table']
)

nu_data_quality_checks = DataQualityOperator(
    task_id='nu_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.nu_table']
)

it_data_quality_checks = DataQualityOperator(
    task_id='it_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.it_table']
)

ja_data_quality_checks = DataQualityOperator(
    task_id='ja_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.ja_table']
)

nl_data_quality_checks = DataQualityOperator(
    task_id='nl_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.nl_table']
)

pl_data_quality_checks = DataQualityOperator(
    task_id='pl_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.pl_table']
)

pt_data_quality_checks = DataQualityOperator(
    task_id='pt_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.pt_table']
)

ru_data_quality_checks = DataQualityOperator(
    task_id='ru_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['Wikibooks_warehouse.ru_table']
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> WikibooksETLJob >> warehouse_data_quality_checks >> create_analytics_schema
create_analytics_schema >> [create_de_table, create_en_table, create_es_table, create_fr_table, create_he_table,
                            create_hu_table, create_it_table, create_ja_table, create_nl_table, create_pl_table,
                            create_pt_table, create_ru_table]
[create_de_table, create_en_table, create_es_table, create_fr_table, create_he_table, create_hu_table, create_it_table,
 create_ja_table, create_nl_table, create_pl_table, create_pt_table, create_ru_table] >> [de_data_quality_checks,
                                                                                          en_data_quality_checks,
                                                                                          es_data_quality_checks,
                                                                                          fr_data_quality_checks,
                                                                                          ne_data_quality_checks,
                                                                                          hu_data_quality_checks,
                                                                                          it_data_quality_checks,
                                                                                          ja_data_quality_checks,
                                                                                          nl_data_quality_checks,
                                                                                          pl_data_quality_checks,
                                                                                          pt_data_quality_checks,
                                                                                          ru_data_quality_checks]

[de_data_quality_checks, en_data_quality_checks, es_data_quality_checks, fr_data_quality_checks, ne_data_quality_checks,
 hu_data_quality_checks, it_data_quality_checks, ja_data_quality_checks, nl_data_quality_checks, pl_data_quality_checks,
 pt_data_quality_checks, ru_data_quality_checks] >> end_operator
