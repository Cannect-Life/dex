import awswrangler as wr

def read_data_from_athena(query, aws_session, database="default", s3_output=None, return_dict=True):

    df = wr.athena.read_sql_query(
        sql=query,
        database=database,
        s3_output=s3_output,
        boto3_session=aws_session,
    )
    
    if return_dict:
        return df.to_dict(orient="records")
    return df