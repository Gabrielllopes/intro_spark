# importa bibliotecas e inicia sessão spark
# Referência: https://spark.apache.org/docs/latest/sql-getting-started.html
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, ByteType
from pyspark.sql.functions import from_unixtime

# inicia sessão
spark = SparkSession \
    .builder \
    .appName("create_parquet") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# criando esquema de u_data
# note que o formato do data esta em string
# isso pq este arquivo usa o timestamp unix secondis since 1/1/1970 UTC
# e sera convertido depois
data_schema = StructType() \
      .add("user_id",IntegerType(),True) \
      .add("movie_id",IntegerType(),True) \
      .add("nota",IntegerType(),True) \
      .add("data",StringType(),True) 

df_data = spark.read.csv("u_data.csv", header=False, sep="\t", schema=data_schema)
df_data = df_data.withColumn("date", from_unixtime(df_data["data"]))
# removendo coluna em formato de data unix
df_data = df_data.drop('data')

# cria esquema u_item
item_schema = StructType() \
    .add("movie_id",IntegerType(),True) \
    .add("movie_title",StringType(),True) \
    .add("release_data",StringType(),True) \
    .add("video_release_date",StringType(),True) \
    .add("imdb_url",StringType(),True) \
    .add("unknow",StringType(),True) \
    .add("action",ByteType(),True) \
    .add("adventure",ByteType(),True) \
    .add("animation",ByteType(),True) \
    .add("children",ByteType(),True) \
    .add("comedy",ByteType(),True) \
    .add("crime",ByteType(),True) \
    .add("documentary",ByteType(),True) \
    .add("drama",ByteType(),True) \
    .add("fantasy",ByteType(),True) \
    .add("film-noir",ByteType(),True) \
    .add("horror",ByteType(),True) \
    .add("musical",ByteType(),True) \
    .add("mystery",ByteType(),True) \
    .add("romance",ByteType(),True) \
    .add("sci-fi",ByteType(),True) \
    .add("thriller",ByteType(),True) \
    .add("war",ByteType(),True) \
    .add("western",ByteType(),True)

df_item = spark.read.csv("u_item.csv", header=False, sep="|", schema=item_schema)

# removendo coluna vazia/desconhecida
df_item = df_item.drop('unknow')

# cria esquema u_user
user_schema = StructType() \
    .add("user_id",IntegerType(),True) \
    .add("age",IntegerType(),True) \
    .add("gender",StringType(),True) \
    .add("occupation",StringType(),True) \
    .add("zip_code",StringType(),True) 

df_user = spark.read.csv("u_user.csv", header=False, sep="|", schema=user_schema)

# juntando u_data e u_user por id
new_data = df_data.join(df_user, on=['user_id'], how='left')
# juntando por id do filme
new_data = new_data.join(df_item, on=["movie_id"], how='left')

# salvando os dados em parquet
new_data.write.parquet("movies_parquet")