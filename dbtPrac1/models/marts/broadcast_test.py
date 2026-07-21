# def model(dbt, session):

#     dbt.config(
#         materialized="table",
#         submission_method="job_cluster",

#         job_cluster_config={
#             "spark_version": "13.3.x-scala2.12",
#             "node_type_id": "Standard_D3_v2",
#             "num_workers": 1
#         }
#     )

#     from pyspark.sql.functions import broadcast

#     df_large = dbt.ref("large_table")
#     df_small = dbt.ref("small_table")

#     joined_df = df_large.join(broadcast(df_small), "id")

#     return joined_df


def model(dbt, session):
    # materialization (table banani hai)
    dbt.config(
        materialized="table"
    )

    # upstream tables (dbt refs)
    large_df = dbt.ref("large_table")
    small_df = dbt.ref("small_table")

    # import broadcast
    from pyspark.sql.functions import broadcast

    # broadcast join
    result = large_df.join(
        broadcast(small_df),
        large_df["id"] == small_df["id"],
        "inner"
    )

    return result

