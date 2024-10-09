def change_null_numeric(df,type):

    numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type == type]
    df = df.na.fill(0, subset = numeric_columns)

    return df