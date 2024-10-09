from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import lit
import os

spark = SparkSession.builder \
    .appName("Read Json Files") \
    .getOrCreate()

def read_and_unify_local_files(directory_path):

    all_dfs = []

    for file_name in os.listdir(directory_path):
        if file_name.endswith(".json"):
            file_path = os.path.join(directory_path, file_name)
            
            with open(file_path, 'r') as file:
                content = file.read()
            
            json_content = json.loads(content)
            json_str = json.dumps(json_content)
            
            df = spark.read.option("multiline", "true").json(spark.sparkContext.parallelize([json_str]))

            df = df.withColumn("file_name", lit(file_name))

            all_dfs.append(df)

    if all_dfs:
        final_df = all_dfs[0]
        for df in all_dfs[1:]:
            final_df = final_df.union(df)
    else:
        final_df = spark.createDataFrame([], schema="file_name STRING")

    return final_df
