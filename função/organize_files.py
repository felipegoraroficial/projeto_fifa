
from pyspark.sql.functions import explode, col, regexp_extract, to_date, row_number
from pyspark.sql.window import Window


def process_data_to_bronze(df):

    df = df.withColumn("item", explode(col("items")))
    
    item_columns = [f"item.{field.name} as {field.name}" for field in df.select("item.*").schema.fields]
    df = df.selectExpr("file_name", *item_columns)
    
    df = df.withColumn("file_date", regexp_extract(col("file_name"), r'\d{4}-\d{2}', 0))
    df = df.withColumn("file_date", to_date(col("file_date"), "yyyy-MM"))

    window_spec = Window.partitionBy("id").orderBy(col("file_date").desc())

    df = df.withColumn("row_number", row_number().over(window_spec))

    df = df.filter(col("row_number") == 1).drop("row_number")

    return df