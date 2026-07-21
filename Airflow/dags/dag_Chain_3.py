from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dbt_Chain_3",
    start_date=datetime(2026, 7, 16),
    schedule=None,
    catchup=False,
    tags=["dbt", "databricks"],
) as dag:

    deploy = BashOperator(
        task_id="deploy",
        bash_command="""
        MODEL="{{ dag_run.conf.get('model_name') }}"

        echo "Deploying model: $MODEL"

        # deployment commands go here
        """
    )