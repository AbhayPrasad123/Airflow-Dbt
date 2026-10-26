from pyspark.sql import functions as F
from delta.tables import DeltaTable

source_table = "workspace.bronze.employee_source"
target_table = "workspace.default_gold.employee_scd2"

src = spark.table(source_table).alias("src")

tgt = (
    spark.table(target_table)
    .filter(F.col("is_current") == True)
    .alias("tgt")
)

# Compare source and current target attributes
same_values = (
    F.col("src.employee_name").eqNullSafe(F.col("tgt.employee_name"))
    & F.col("src.department").eqNullSafe(F.col("tgt.department"))
    & F.col("src.salary").eqNullSafe(F.col("tgt.salary"))
    & F.col("src.city").eqNullSafe(F.col("tgt.city"))
)

changed = ~same_values

# 1. New employees and new versions of changed employees
new_versions = (
    src.join(
        tgt,
        F.col("src.employee_id") == F.col("tgt.employee_id"),
        "left"
    )
    .filter(
        F.col("tgt.employee_id").isNull() | changed
    )
    .select(
        F.lit(None).cast("int").alias("merge_key"),
        F.col("src.employee_id").alias("employee_id"),
        F.col("src.employee_name").alias("employee_name"),
        F.col("src.department").alias("department"),
        F.col("src.salary").alias("salary"),
        F.col("src.city").alias("city"),
        F.to_date("src.updated_at").alias("start_date"),
        F.to_date(F.lit("9999-12-31")).alias("end_date"),
        F.lit(True).alias("is_current")
    )
)

# 2. Old current versions that need to expire
old_versions = (
    src.join(
        tgt,
        F.col("src.employee_id") == F.col("tgt.employee_id"),
        "inner"
    )
    .filter(changed)
    .select(
        F.col("src.employee_id").alias("merge_key"),
        F.col("tgt.employee_id").alias("employee_id"),
        F.col("tgt.employee_name").alias("employee_name"),
        F.col("tgt.department").alias("department"),
        F.col("tgt.salary").alias("salary"),
        F.col("tgt.city").alias("city"),
        F.col("tgt.start_date").alias("start_date"),
        F.date_sub(
            F.to_date("src.updated_at"), 1
        ).alias("end_date"),
        F.lit(False).alias("is_current")
    )
)

# Combine insert and expire actions
staged = new_versions.unionByName(old_versions)

# Apply changes to Delta target
target = DeltaTable.forName(spark, target_table)

(
    target.alias("target")
    .merge(
        staged.alias("staged"),
        """
        target.employee_id = staged.merge_key
        AND target.is_current = true
        """
    )
    .whenMatchedUpdate(
        set={
            "end_date": "staged.end_date",
            "is_current": "staged.is_current"
        }
    )
    .whenNotMatchedInsert(
        values={
            "employee_id": "staged.employee_id",
            "employee_name": "staged.employee_name",
            "department": "staged.department",
            "salary": "staged.salary",
            "city": "staged.city",
            "start_date": "staged.start_date",
            "end_date": "staged.end_date",
            "is_current": "staged.is_current"
        }
    )
    .execute()
)

display(
    spark.table(target_table)
    .orderBy("employee_id", "start_date")
)