from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dbt_databricks_pipeline",
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
        dbt build -s dim_employee_scd2 --profiles-dir .
        echo "===== COMPILED SQL ====="
        cat target/compiled/dbtPrac1/models/marts/dim_employee_scd2.sql
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /opt/airflow/dbt
        dbt test -s dim_employee_scd2 --profiles-dir .
        """
    )

    dbt_debug >> dbt_build >> dbt_test