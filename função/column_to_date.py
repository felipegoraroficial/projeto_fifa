from pyspark.sql.functions import to_date

def column_to_date(df,column_name,format):

    df = df.withColumn(column_name, to_date(column_name,format))

    return df