from pyspark.sql import SparkSession

def union_gold():

    spark = SparkSession.builder \
        .appName("MySQLReader") \
        .config("spark.jars", "/usr/share/java/mysql-connector-java-9.0.0.jar") \
        .getOrCreate()

    jdbcHostname = "localhost"
    jdbcPort = 3306
    jdbcDatabase = "silver"
    jdbcUsername = "root"
    jdbcPassword = "Fececa13"

    jdbcUrl = f"jdbc:mysql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}"

    connectionProperties = {
        "user": jdbcUsername,
        "password": jdbcPassword,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    nation = spark.read.jdbc(url=jdbcUrl, table="nations_fifa", properties=connectionProperties)
    league = spark.read.jdbc(url=jdbcUrl, table="league_fifa", properties=connectionProperties)
    club = spark.read.jdbc(url=jdbcUrl, table="clubs_fifa", properties=connectionProperties)
    players = spark.read.jdbc(url=jdbcUrl, table="players_fifa", properties=connectionProperties)

    nation = nation.withColumnRenamed('name', 'coutry')
    union = players.join(nation, players['nation'] == nation['id'], 'left').select(players['*'], nation['coutry'])
    union = union.drop('nation')

    league = league.withColumnRenamed('name','footballLeague')
    union = union.join(league,union['league'] == league['id'], 'left').select(union['*'], league['footballLeague'])
    union = union.drop('league')

    club = club.withColumnRenamed('name','footballClub')
    union = union.join(club,union['club'] == club['id'], 'left').select(union['*'], club['footballClub'])
    union = union.drop('club')

    jdbcDatabaseGold = "gold"
    jdbcUrlGold = f"jdbc:mysql://{jdbcHostname}:{jdbcPort}/{jdbcDatabaseGold}"

    union.write \
    .jdbc(url=jdbcUrlGold, table="fifa", mode="overwrite", properties=connectionProperties)