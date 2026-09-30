from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =========================================================
# 1. Read source
# =========================================================

new_data = spark.table("stg_employee")


# =========================================================
# 2. Incremental logic
# =========================================================

target_table = "dim_employee_scd2"

if spark.catalog.tableExists(target_table):

    max_valid_from = (
        spark.table(target_table)
        .agg(
            F.coalesce(
                F.max("valid_from"),
                F.lit("1900-01-01")
            ).alias("max_valid_from")
        )
        .collect()[0]["max_valid_from"]
    )

    new_data = new_data.filter(
        F.col("effective_date") > F.lit(max_valid_from)
    )

else:

    # Empty target for first run
    pass


# =========================================================
# 3. Get current records from target
# =========================================================

if spark.catalog.tableExists(target_table):

    current_data = (
        spark.table(target_table)
        .filter(F.col("is_current") == True)
        .select(
            "id",
            "name",
            "org",
            "age",
            F.col("valid_from").alias("effective_date")
        )
    )

else:

    current_data = spark.createDataFrame(
        [],
        """
        id long,
        name string,
        org string,
        age long,
        effective_date date
        """
    )


# =========================================================
# 4. Combine new + current
# =========================================================

combined = new_data.select(
    "id",
    "name",
    "org",
    "age",
    "effective_date"
).unionByName(
    current_data.select(
        "id",
        "name",
        "org",
        "age",
        "effective_date"
    )
)


# =========================================================
# 5. Create hash
# =========================================================

hashed = combined.withColumn(
    "row_hash",
    F.md5(
        F.concat_ws(
            "|",
            F.coalesce(F.col("name"), F.lit("")),
            F.coalesce(F.col("org"), F.lit("")),
            F.coalesce(F.col("age").cast("string"), F.lit("0"))
        )
    )
)


# =========================================================
# 6. LAG(previous hash)
# =========================================================

window_spec = (
    Window
    .partitionBy("id")
    .orderBy("effective_date")
)

lagged = hashed.withColumn(
    "prev_hash",
    F.lag("row_hash").over(window_spec)
)


# =========================================================
# 7. Keep only changed records
# =========================================================

filtered = lagged.filter(
    F.col("prev_hash").isNull()
    |
    (F.col("row_hash") != F.col("prev_hash"))
)


# =========================================================
# 8. LEAD() -> valid_to
# =========================================================

final = filtered.withColumn(
    "valid_to",
    F.lead("effective_date").over(window_spec)
)


# =========================================================
# 9. is_current
# =========================================================

final = final.withColumn(
    "is_current",
    F.when(
        F.col("valid_to").isNull(),
        True
    ).otherwise(False)
)


# =========================================================
# 10. Final columns
# =========================================================

result = final.select(
    "id",
    "name",
    "org",
    "age",
    F.col("effective_date").alias("valid_from"),
    "valid_to",
    "is_current"
)


result.show()