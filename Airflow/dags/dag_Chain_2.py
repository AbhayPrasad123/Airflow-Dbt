from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="dbt_Chain_2",
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

        echo "Testing $MODEL"

        dbt test -s $MODEL --profiles-dir .
        """
    )

    trigger_dag3 = TriggerDagRunOperator(
        task_id="trigger_dag3",
        trigger_dag_id="{{ dag_run.conf.get('dag3') }}",
        conf={
            "model_name": "{{ dag_run.conf.get('model_name') }}"
        }
    )

    dbt_test >> trigger_dag3