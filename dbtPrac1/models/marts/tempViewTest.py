def model(dbt, session):

    dbt.config(materialized="table")

    df = session.range(10)

   
    df.createOrReplaceTempView("temp_view")

    
    result = session.sql("select * from temp_view")

    return result