from pyspark.sql.functions import col,upper

def upper_string_column(df,column_name):

    df = df.withColumn(column_name, upper(col(column_name)))

    return df