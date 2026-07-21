from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dbt_test_pipeline",
    start_date=datetime(2026, 7, 16),
    schedule=None,
    catchup=False,
    tags=["dbt", "databricks"],
) as dag:

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /opt/airflow/dbt

        MODEL="{{ dag_run.conf.get('model_name') }}"

        echo "=================================="
        echo "Testing model: $MODEL"
        echo "=================================="

        dbt test -s $MODEL --profiles-dir .
        """
    )