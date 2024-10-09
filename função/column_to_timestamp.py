from pyspark.sql.functions import to_timestamp

def column_to_date(df,column_name,format):

    df = df.withColumn(column_name, to_timestamp(column_name,format))

    return df