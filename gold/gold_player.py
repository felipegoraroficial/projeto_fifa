from pyspark.sql import SparkSession

def gold_players():

    spark = SparkSession.builder \
        .appName("GoldStep") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
        .config("spark.jars", "/usr/share/java/mysql-connector-java-9.0.0.jar") \
        .getOrCreate()


    caminho_pasta = "/home/fececa/airflow/dags/fifa/data/silver/players"

    df = spark.read.format("avro").load(caminho_pasta)

    df = df.filter((df['id'] != 0) & (df['club'] != 0) & (df['league'] != 0) & (df['nation'] != 0))

    df.show()

    url = "jdbc:mysql://localhost:3306/silver"
    properties = {
        "user": "root",
        "password": "Fececa13",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    df.write \
    .jdbc(url=url, table="players_fifa", mode="overwrite", properties=properties)

