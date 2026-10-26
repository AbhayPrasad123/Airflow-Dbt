from pyspark.sql import functions as F
from pyspark.sql.window import Window

source_table = "workspace.bronze.employee_source"
target_table = "workspace.default_gold.employee_scd2"

# 1. Read existing history
history = (
    spark.table(target_table)
    .select(
        "employee_id",
        "employee_name",
        "department",
        "salary",
        "city",
        F.col("start_date").alias("effective_date")
    )
    .withColumn("source_priority", F.lit(0))
)

# 2. Read latest source records
source = (
    spark.table(source_table)
    .select(
        "employee_id",
        "employee_name",
        "department",
        "salary",
        "city",
        F.to_date("updated_at").alias("effective_date")
    )
    .withColumn("source_priority", F.lit(1))
)

# 3. Combine history and new source data
combined = history.unionByName(source)

# Same employee/date par source ko priority do.
# Assumption: ek employee ke liye ek effective change per day.
dedup_window = (
    Window.partitionBy("employee_id", "effective_date")
    .orderBy(F.col("source_priority").desc())
)

combined = (
    combined
    .withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn", "source_priority")
)

# 4. Generate hash to compare employee attributes
hashed = combined.withColumn(
    "row_hash",
    F.sha2(
        F.concat_ws(
            "|",
            F.coalesce(F.col("employee_name"), F.lit("")),
            F.coalesce(F.col("department"), F.lit("")),
            F.coalesce(F.col("salary").cast("string"), F.lit("")),
            F.coalesce(F.col("city"), F.lit(""))
        ),
        256
    )
)

# 5. LAG: get previous version's hash
history_window = (
    Window.partitionBy("employee_id")
    .orderBy("effective_date")
)

lagged = hashed.withColumn(
    "prev_hash",
    F.lag("row_hash").over(history_window)
)

# 6. Keep first record and records where values changed
filtered = lagged.filter(
    F.col("prev_hash").isNull()
    | (F.col("row_hash") != F.col("prev_hash"))
)

# 7. LEAD: get the next version's effective date
version_window = (
    Window.partitionBy("employee_id")
    .orderBy("effective_date")
)

versioned = (
    filtered
    .withColumn(
        "start_date",
        F.col("effective_date")
    )
    .withColumn(
        "next_start_date",
        F.lead("effective_date").over(version_window)
    )
    .withColumn(
        "end_date",
        F.date_sub(F.col("next_start_date"), 1)
    )
)

# 8. Final SCD2 output
rebuilt_df = versioned.select(
    "employee_id",
    "employee_name",
    "department",
    "salary",
    "city",
    "start_date",
    F.coalesce(
        F.col("end_date"),
        F.to_date(F.lit("9999-12-31"))
    ).alias("end_date"),
    F.col("next_start_date").isNull().alias("is_current")
)

display(rebuilt_df.orderBy("employee_id", "start_date"))