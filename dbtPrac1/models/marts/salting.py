from pyspark.sql import functions as F

num_salts = 5

# -----------------------------
# TABLE 1 - Large / Skewed
# -----------------------------

table1_salted = table1.withColumn(
    "salt",
    F.floor(F.rand() * num_salts)
)

table1_salted = table1_salted.withColumn(
    "saltkey",
    F.concat(
        F.col("customer_id").cast("string"),
        F.lit("_"),
        F.col("salt").cast("string")
    )
)


# -----------------------------
# TABLE 2 - Small
# -----------------------------

table2_salted = table2.withColumn(
    "salt",
    F.sequence(
        F.lit(0),
        F.lit(num_salts - 1)
    )
)

table2_salted = table2_salted.withColumn(
    "salt",
    F.explode("salt")
)

table2_salted = table2_salted.withColumn(
    "saltkey",
    F.concat(
        F.col("customer_id").cast("string"),
        F.lit("_"),
        F.col("salt").cast("string")
    )
)


# -----------------------------
# JOIN
# -----------------------------

result = table1_salted.join(
    table2_salted,
    on="saltkey",
    how="inner"
)

result = result.drop(
    table1_salted.salt,
    table2_salted.salt,
    "saltkey"
)