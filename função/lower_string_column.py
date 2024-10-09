from pyspark.sql.functions import col,lower

def lower_string_column(df,column_name):

    df = df.withColumn(column_name, lower(col(column_name)))

    return df