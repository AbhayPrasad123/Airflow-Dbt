from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# =====================================================
# 1. Read CDC table
# =====================================================

cdc = spark.table("employee_cdc")


# =====================================================
# 2. ROW_NUMBER() OVER (
#       PARTITION BY id
#       ORDER BY change_time DESC
#    )
# =====================================================

window_spec = (
    Window
    .partitionBy("id")
    .orderBy(F.col("change_time").desc())
)

latest_cdc = (
    cdc
    .withColumn(
        "rn",
        F.row_number().over(window_spec)
    )
    .filter(F.col("rn") == 1)
    .drop("rn")
)


# =====================================================
# 3. Load target Delta table
# =====================================================

target = DeltaTable.forName(
    spark,
    "target_employee"
)


# =====================================================
# 4. MERGE
# =====================================================

(
    target.alias("t")

    .merge(
        latest_cdc.alias("s"),

        # SQL:
        # ON t.id = s.id

        "t.id = s.id"
    )

    # =================================================
    # UPDATE
    # =================================================

    .whenMatchedUpdate(
        condition="s.operation = 'U'",

        set={
            "name": "s.name",
            "org": "s.org",
            "age": "s.age"
        }
    )

    # =================================================
    # DELETE
    # =================================================

    .whenMatchedDelete(
        condition="s.operation = 'D'"
    )

    # =================================================
    # INSERT
    # =================================================

    .whenNotMatchedInsert(
        condition="s.operation = 'I'",

        values={
            "id": "s.id",
            "name": "s.name",
            "org": "s.org",
            "age": "s.age"
        }
    )

    .execute()
)