

# Steps carried out in this code:-
# 1. Getting input from GCS
# 2. Processing the data to the requirements using airflow environment in GCP cloud composer
# 3. Saving the processed data into BigQuery partitioned table



from datetime import datetime, date, timedelta

from airflow.providers.google.cloud.operators.dataproc import \
    DataprocCreateClusterOperator, \
    DataprocDeleteClusterOperator, \
    DataprocSubmitPySparkJobOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow import models, DAG

# from airflow.operators import bash_operator

from airflow.models import variable

from airflow.utils.trigger_rule import TriggerRule

the_bucket= "gs://tamilzh-etl"

the_pyspark_job= the_bucket + "/gcs_proc_gcs.py"

current_date= str(date.today())

default_arguments={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 'gettingstarted-356807',
    'scheduled_interval': "30 2 * * *"
}

with DAG(
    'flights-etl',
    default_args= default_arguments
)as dag:

    to_create_cluster= DataprocCreateClusterOperator(
        task_id='to_create_cluster',
        cluster_name='cluster-for-flights-etl',
        master_machine_type='n1-standard-2',
        worker_machine_type='n1-standard-2',
        num_workers=2,
        region='us-central1',
        zone='us-central1-a',
        master_disk_type='pd-standard',
        master_disk_size= 500,
        worker_disk_type='pd-standard',
        worker_disk_size=500
    )

    to_submit_pyspark_job= DataprocSubmitPySparkJobOperator(
        task_id= 'to_submit_pyspark_job',
        cluster_name= 'cluster-for-flights-etl',
        main= the_pyspark_job,
        region= 'us-central1'
    )

    gcs_to_bq_delays_by_distance_category= GCSToBigQueryOperator(
        task_id= 'to_load_output_by_distance_category',
        bucket= "tamilzh-etl",
        source_objects=["de-course-project-outputs/"+current_date+"/"+current_date+"_output_of_distance_category/part-*"],
        destination_project_dataset_table= "gettingstarted-356807.data_analysis.output_by_distance_category",
        autodetect= True,
        source_format= "NEWLINE_DELIMITED_JSON",
        create_disposition= "CREATE_IF_NEEDED",
        skip_leading_rows= 0,
        write_disposition= "WRITE_APPEND",
        max_bad_records= 0
    )

    gcs_to_bq_delays_by_flight_nums = GCSToBigQueryOperator(
        task_id='to_load_output_by_flight_nums',
        bucket="tamilzh-etl",
        source_objects=["de-course-project-outputs/" + current_date + "/" + current_date + "_output_of_flight_nums/part-*"],
        destination_project_dataset_table="gettingstarted-356807.data_analysis.output_by_flight_nums",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )

    to_delete_cluster= DataprocDeleteClusterOperator(
        task_id= 'to_delete_cluster',
        cluster_name= 'cluster-for-flights-etl',
        region= 'us-central1',
        trigger_rule= TriggerRule.ALL_DONE
    )

to_create_cluster >> to_submit_pyspark_job >> \
gcs_to_bq_delays_by_distance_category >> gcs_to_bq_delays_by_flight_nums >> \
to_delete_cluster
