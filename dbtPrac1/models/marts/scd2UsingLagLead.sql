from pyspark.sql import functions as F
from pyspark.sql.window import Window

source_table = "workspace.bronze.employee_source"
target_table = "workspace.default_gold.employee_scd2"

# 1. combined
history = spark.table(target_table).select(
    "employee_id",
    "employee_name",
    "department",
    "salary",
    "city",
    F.col("start_date").alias("effective_date")
)

source = spark.table(source_table).select(
    "employee_id",
    "employee_name",
    "department",
    "salary",
    "city",
    F.to_date("updated_at").alias("effective_date")
)

combined = history.unionByName(source)

# 2. hashed
hashed = combined.withColumn(
    "row_hash",
    F.md5(
        F.concat_ws(
            "|",
            F.coalesce(F.col("employee_name"), F.lit("")),
            F.coalesce(F.col("department"), F.lit("")),
            F.coalesce(F.col("salary").cast("string"), F.lit("")),
            F.coalesce(F.col("city"), F.lit(""))
        )
    )
)

# 3. lagged
w = Window.partitionBy("employee_id").orderBy("effective_date")

lagged = hashed.withColumn(
    "prev_hash",
    F.lag("row_hash").over(w)
)

# 4. filtered
filtered = lagged.filter(
    F.col("prev_hash").isNull()
    | (F.col("row_hash") != F.col("prev_hash"))
)

# 5. versioned
versioned = (
    filtered
    .withColumn(
        "start_date",
        F.col("effective_date")
    )
    .withColumn(
        "next_date",
        F.lead("effective_date").over(w)
    )
    .withColumn(
        "end_date",
        F.date_sub(F.col("next_date"), 1)
    )
)

# 6. final SELECT — same output columns as SQL
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
    F.col("next_date").isNull().alias("is_current")
)

display(rebuilt_df.orderBy("employee_id", "start_date"))