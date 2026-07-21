def model(dbt, session):

    dbt.config(materialized="table")

    df = session.range(1000)

    df2 = df.repartition(10)

    print("Partitions:", df2.rdd.getNumPartitions())

    return df2