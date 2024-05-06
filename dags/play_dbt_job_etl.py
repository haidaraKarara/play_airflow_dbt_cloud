"""
# The dbt cloud elt dag
The dag will call a dbt cloud job that is supposed 
to do some transformation (from raw layer to the silver layer) in snowflake.

"""

from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

# Pendulum is a Python package to ease datetimes manipulation: https://pendulum.eustace.io/docs/
from pendulum import datetime, duration

# get the airflow.task logger

DBT_CLOUD_CONN_ID = Variable.get("DBT_CLOUD_CONN_ID")
DBT_JOB_ID = Variable.get("SECRET_DBT_JOB_ID")
LOOK_EXTERNAL_FILE = Variable.get("LOOK_EXTERNAL_FILE")
TASK_ID = "trigger_dbt_cloud_job_runDaily"  # You define you own name
# Default settings applied to all tasks
DEFAULT_ARGS = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": duration(minutes=1),
}


@dag(
    dag_id="dbt_cloud_elt_in_snowflake",  # You define you own name
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    tags=["dbt_cloud_job"],
    doc_md=__doc__,
    description="Run a DBT cloud Job",
    schedule_interval=None,
    catchup=False,
    # include path to look for external files
    template_searchpath=LOOK_EXTERNAL_FILE,
)
def dbt_cloud_elt():
    # Task 1
    call_dbt_job = DbtCloudRunJobOperator(
        task_id=TASK_ID,
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=DBT_JOB_ID,
        check_interval=25,
        wait_for_termination="True",
        timeout=300,
    )

    call_dbt_job


dbt_cloud_elt()
