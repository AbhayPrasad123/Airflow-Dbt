import os
import sys

from jinja2 import Environment, FileSystemLoader


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

print("=" * 40)
print(f"DAG Created : {dag_name}")
print(f"Saved at : {output_file}")
print("=" * 40)


# python create_dag.py employee_pipeline