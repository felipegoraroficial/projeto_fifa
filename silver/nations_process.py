from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, LongType
import os

def silver_step_nation():

    spark = SparkSession.builder \
        .appName("SilverStep") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
        .getOrCreate()

    path = '/home/fececa/airflow/dags/fifa/data/bronze/nation/'

    df = spark.read.format('csv').option('sep',',').option('header','true').load(path)

    df = df.withColumn("file_date", col("file_date").cast(DateType())) \
                    .withColumn("id", col("id").cast(LongType()))

    df = df.fillna({'id': 0})

    string_columns = [col_name for col_name, data_type in df.dtypes if data_type == 'string']
    df = df.na.fill('-', subset=string_columns)

    df.printSchema()
    df.show()

    output_path = '/home/fececa/airflow/dags/fifa/data/silver/nations/'
    os.makedirs(output_path, exist_ok=True)

    df.write.format('avro') \
        .mode('overwrite') \
        .partitionBy('file_date') \
        .option('overwriteSchema', 'true') \
        .save(output_path)

    print(f"DataFrame salvo no formato Avro em: {output_path}")
