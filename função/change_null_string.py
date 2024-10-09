def change_null_string(df):

    string_columns = [col_name for col_name, data_type in df.dtypes if data_type == 'string']
    df = df.na.fill('-', subset = string_columns)

    return df