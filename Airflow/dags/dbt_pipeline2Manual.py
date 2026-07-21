from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="dbt_build_pipeline",
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

        echo "=================================="
        echo "Building model: $MODEL"
        echo "=================================="

        dbt build -s $MODEL --profiles-dir .

        echo ""
        echo "========== COMPILED SQL =========="

        find target/compiled -name "$MODEL.sql" -exec cat {} \\;

        echo ""
        echo "=================================="
        """
    )

    trigger_test_dag = TriggerDagRunOperator(
        task_id="trigger_test_dag",
        trigger_dag_id="dbt_test_pipeline",
        conf={
            "model_name": "{{ dag_run.conf.get('model_name') }}"
        }
    )

    dbt_debug >> dbt_build >> trigger_test_dag