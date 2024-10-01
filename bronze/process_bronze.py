
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, explode, col, regexp_extract, to_date, row_number
from pyspark.sql.window import Window

def bronze_step():

    spark = SparkSession.builder.appName("BronzeStep").getOrCreate()

    def process_data_to_bronze(path):

        df = spark.read.option("multiline", "true").json(path)

        df = df.withColumn("file_name",input_file_name())

        df = df.withColumn("item", explode(col("items")))

        item_columns = [f"item.{field.name} as {field.name}" for field in df.select("item.*").schema.fields]
        df = df.selectExpr("file_name", *item_columns)

        df = df.withColumn("file_date", regexp_extract(col("file_name"), r'\d{4}-\d{2}-\d{2}', 0))
        df = df.withColumn("file_date", to_date(col("file_date"), "yyyy-MM-dd"))

        window_spec = Window.partitionBy("id").orderBy(col("file_date").desc())

        df = df.withColumn("row_number", row_number().over(window_spec))

        df = df.filter(col("row_number") == 1).drop("row_number")

        return df
    directory_nations = "/home/fececa/airflow/dags/fifa/data/extract/país/raw/"
    nation = process_data_to_bronze(directory_nations)
    directory_league = "/home/fececa/airflow/dags/fifa/data/extract/liga/raw/"
    league = process_data_to_bronze(directory_league)
    directory_clubs = "/home/fececa/airflow/dags/fifa/data/extract/clube/raw"
    clubs = process_data_to_bronze(directory_clubs)
    directory_players = "/home/fececa/airflow/dags/fifa/data/extract/jogadores/raw/"
    players = process_data_to_bronze(directory_players)


    output_path = "/home/fececa/airflow/dags/fifa/data/bronze/nation"
    os.makedirs(output_path, exist_ok=True)
    nation.write.format('csv').mode('overwrite').partitionBy('file_date').option('overwriteSchema','true').option("header", "true").option("sep", ",").save(output_path)

    output_path = "/home/fececa/airflow/dags/fifa/data/bronze/league/"
    os.makedirs(output_path, exist_ok=True)
    league.write.format('csv').mode('overwrite').partitionBy('file_date').option('overwriteSchema','true').option("header", "true").option("sep", ",").save(output_path)

    output_path = "/home/fececa/airflow/dags/fifa/data/bronze/clubs/"
    os.makedirs(output_path, exist_ok=True)
    clubs.write.format('csv').mode('overwrite').partitionBy('file_date').option('overwriteSchema','true').option("header", "true").option("sep", ",").save(output_path)

    output_path = "/home/fececa/airflow/dags/fifa/data/bronze/players/"
    os.makedirs(output_path, exist_ok=True)
    players.write.format('csv').mode('overwrite').partitionBy('file_date').option('overwriteSchema','true').option("header", "true").option("sep", ",").save(output_path)



