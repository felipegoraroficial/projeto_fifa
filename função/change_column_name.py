def change_column_name(df,col_original,col_change):

    df = df.withColumnRenamed(col_original,col_change)

    return df