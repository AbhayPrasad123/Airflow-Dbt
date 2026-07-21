def model(dbt, session):

    dbt.config(materialized="table")

    from pyspark.sql.functions import col
   
    def add_tax_py(column_name, rate=0.1):
        return col(column_name) * rate

    df = dbt.ref("stg_orders")

    df = df.withColumn("tax", add_tax_py("amount"))

    return df