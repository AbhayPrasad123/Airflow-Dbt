from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="dbt_Chain_1",
    start_date=datetime(2026, 7, 16),
    schedule=None,
    catchup=False,
    tags=["dbt", "databricks"],
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command="""
        cd /opt/airflow/dbt
        dbt debug --profiles-dir .
        """
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="""
        cd /opt/airflow/dbt

        MODEL="{{ dag_run.conf.get('model_name') }}"

        echo "Building $MODEL"

        dbt build -s $MODEL --profiles-dir .

        echo "===== COMPILED SQL ====="

        find target/compiled -name "$MODEL.sql" -exec cat {} \\;
        """
    )

    trigger_dag2 = TriggerDagRunOperator(
        task_id="trigger_dag2",
        trigger_dag_id="{{ dag_run.conf.get('dag2') }}",
        conf={
            "model_name": "{{ dag_run.conf.get('model_name') }}",
            "dag3": "{{ dag_run.conf.get('dag3') }}"
        }
    )

    dbt_debug >> dbt_build >> trigger_dag2