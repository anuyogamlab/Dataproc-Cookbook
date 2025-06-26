from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
from pendulum import timezone

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Set timezone to Central Time (CT)
ct_timezone = timezone('America/Chicago')

# Common Dataproc job configuration details
project_id = 'nsadineni'
region = 'us-central1'  # e.g., 'us-central1'
cluster_name = 'composer-examples'

# Define the Dataproc jobs
teragen_job = {
    "reference": {"project_id": project_id},
    "placement": {"cluster_name": cluster_name},
    "hadoop_job": {
        "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
        "args": [
            "teragen",
            "1000000",  # Number of rows to generate
            "/user/hadoop/teragen-output"
        ],
    },
}

terasort_job = {
    "reference": {"project_id": project_id},
    "placement": {"cluster_name": cluster_name},
    "hadoop_job": {
        "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
        "args": [
            "terasort",
            "/user/hadoop/teragen-output",
            "/user/hadoop/terasort-output"
        ],
    },
}

wordcount_job = {
    "reference": {"project_id": project_id},
    "placement": {"cluster_name": cluster_name},
    "hadoop_job": {
        "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
        "args": [
            "wordcount",
            "/user/hadoop/teragen-output",
            "/user/hadoop/wordcount-output"
        ],
    },
}

teravalidate_job = {
    "reference": {"project_id": project_id},
    "placement": {"cluster_name": cluster_name},
    "hadoop_job": {
        "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
        "args": [
            "teravalidate",
            "/user/hadoop/terasort-output",
            "/user/hadoop/teravalidate-output"
        ],
    },
}

# Define the DAG
with DAG(
        'hadoop_terasuite_pipeline_dataproc',
        default_args=default_args,
        description='A DAG to demonstrate a composed Hadoop job sequence using Dataproc',
        schedule_interval='0 8 * * *',  # Run every day at 8 AM CT
        start_date=days_ago(1),
        catchup=False,
        tags=['example'],
        #timezone=ct_timezone,  # Set the DAG's timezone to Central Time (CT)
) as dag:

    # Job A: TeraGen
    teragen = DataprocSubmitJobOperator(
        task_id='teragen',
        job=teragen_job,
        region=region,
        project_id=project_id,
    )

    # Job B: TeraSort
    terasort = DataprocSubmitJobOperator(
        task_id='terasort',
        job=terasort_job,
        region=region,
        project_id=project_id,
    )

    # Job C: WordCount
    wordcount = DataprocSubmitJobOperator(
        task_id='wordcount',
        job=wordcount_job,
        region=region,
        project_id=project_id,
    )

    # Job D: TeraValidate
    teravalidate = DataprocSubmitJobOperator(
        task_id='teravalidate',
        job=teravalidate_job,
        region=region,
        project_id=project_id,
    )

    # Define the task dependencies
    teragen >> [terasort, wordcount] >> teravalidate