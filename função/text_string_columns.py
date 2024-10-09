from pyspark.sql.functions import initcap,col

def text_string_columns(df):

    string_column = [col_name for col_name, data_type in df.dtypes if data_type == 'string']

    for column in string_column:
        df = df.withColumn(column, initcap(col(column)))

    return df