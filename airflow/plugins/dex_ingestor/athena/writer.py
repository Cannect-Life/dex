import awswrangler as wr

def write_pandas_to_table(
    df,
    s3_path,
    database,
    table,
    df_athena_schema,
    aws_session,
    mode="overwrite_partitions",
    partition_cols=["partition_date", "account"],
    **kwargs
):
    
    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        dataset=True,
        mode=mode,
        partition_cols=partition_cols,
        boto3_session=aws_session,
        database=database,
        table=table,
        dtype=df_athena_schema,
        **kwargs
    )