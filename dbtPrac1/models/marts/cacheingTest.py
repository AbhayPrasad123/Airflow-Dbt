def model(dbt, session):

    dbt.config(materialized="table")

    import time

    df = session.range(0, 10_000_000)

    df_cached = df.cache()

    start = time.time()
    df_cached.count()
    end = time.time()
    print("First run:", end - start)

    start = time.time()
    df_cached.count()
    end = time.time()
    print("Second run:", end - start)

    return df_cached