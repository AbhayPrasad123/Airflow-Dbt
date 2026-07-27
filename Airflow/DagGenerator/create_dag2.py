import os
import sys
import time
import subprocess

from jinja2 import Environment, FileSystemLoader


# Check command line argument
if len(sys.argv) != 2:
    print("Usage: python create_dag.py <dag_name>")
    sys.exit(1)


dag_name = sys.argv[1]



env = Environment(
    loader=FileSystemLoader("templates")
)

template = env.get_template("dag_template.j2")


output = template.render(
    dag_name=dag_name
)



output_dir = "../dags/generated"

os.makedirs(output_dir, exist_ok=True)

output_file = os.path.join(
    output_dir,
    f"{dag_name}.py"
)



with open(output_file, "w") as f:
    f.write(output)


print("=" * 50)
print(f"DAG Created Successfully : {dag_name}")
print(f"Location : {output_file}")
print("=" * 50)


# Wait for Airflow Scheduler
print("\nWaiting for Airflow Scheduler to detect the DAG...")
time.sleep(180)


# Trigger DAG
print(f"\nTriggering DAG : {dag_name}\n")

cmd = [
    "docker",
    "exec",
    "-i",
    "airflow-airflow-scheduler-1",
    "airflow",
    "dags",
    "trigger",
    dag_name
]

result = subprocess.run(
    cmd,
    capture_output=True,
    text=True
)


# Output
if result.returncode == 0:
    print(result.stdout)
    print("=" * 50)
    print("DAG Triggered Successfully")
    print("=" * 50)
else:
    print(result.stderr)
    print("=" * 50)
    print("Failed to Trigger DAG")
    print("=" * 50)


    import json

# conf = {
#     "model_name": "employee_scd2",
#     "dag2": "dbt_Chain_2",
#     "dag3": "dbt_Chain_3"
# }

# cmd = [
#     "docker",
#     "exec",
#     "-i",
#     "airflow-airflow-scheduler-1",
#     "airflow",
#     "dags",
#     "trigger",
#     dag_name,
#     "--conf",
#     json.dumps(conf)
# ]