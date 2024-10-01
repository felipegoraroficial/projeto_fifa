from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, LongType
import os

def silver_step_palyers():

    spark = SparkSession.builder \
        .appName("SilverStep") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
        .getOrCreate()

    path = '/home/fececa/airflow/dags/fifa/data/bronze/players/'

    df = spark.read.format('csv').option('sep',',').option('header','true').load(path)

    df = df.select('id','club','league','nation','age','birthDate','color','name',
                'firstName','lastName','foot','gender','height','weight','position',
                'rating','shooting','physicality','passing','pace','dribbling',
                'defending','attackWorkRate','defenseWorkRate','file_name','file_date')

    df = df.withColumn("file_date", col("file_date").cast(DateType())) \
                    .withColumn("birthDate", col("birthDate").cast(DateType())) \
                    .withColumn("age", col("age").cast(LongType())) \
                    .withColumn("height", col("height").cast(LongType())) \
                    .withColumn("weight", col("weight").cast(LongType())) \
                    .withColumn("rating", col("rating").cast(LongType())) \
                    .withColumn("shooting", col("shooting").cast(LongType())) \
                    .withColumn("physicality", col("physicality").cast(LongType())) \
                    .withColumn("passing", col("passing").cast(LongType())) \
                    .withColumn("pace", col("pace").cast(LongType())) \
                    .withColumn("dribbling", col("dribbling").cast(LongType())) \
                    .withColumn("defending", col("defending").cast(LongType())) \
                    .withColumn("id", col("id").cast(LongType())) \
                    .withColumn("club", col("club").cast(LongType())) \
                    .withColumn("league", col("league").cast(LongType())) \
                    .withColumn("nation", col("nation").cast(LongType()))

    numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type == 'long']
    df = df.na.fill(0, subset = numeric_columns)

    string_columns = [col_name for col_name, data_type in df.dtypes if data_type == 'string']
    df = df.na.fill('-', subset=string_columns)

    df.printSchema()
    df.show(1)

    output_path = '/home/fececa/airflow/dags/fifa/data/silver/players/'
    os.makedirs(output_path, exist_ok=True)

    df.write.format('avro') \
        .mode('overwrite') \
        .partitionBy('file_date') \
        .option('overwriteSchema', 'true') \
        .save(output_path)

    print(f"DataFrame salvo no formato Avro em: {output_path}")
