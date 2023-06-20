from pyspark.sql import SparkSession
import sys
import psycopg2
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import initcap
from pyspark.sql.functions import when, col, lit
from pyspark.sql.types import IntegerType


# table_for_read=sys.argv[7]
# table_name=sys.argv[8]

spark: SparkSession = SparkSession.builder\
        .appName('Transform')\
        .getOrCreate()


#set parameters 
postgres_user = sys.argv[1]
postgres_password = sys.argv[2]
postgres_host = sys.argv[3]
postgres_port = sys.argv[4]
postgres_database = sys.argv[5]
# Connection database
conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password
)

#Create database if it doesn't exist
cursor = conn.cursor()
cursor.execute(f'CREATE TABLE IF NOT EXISTS stating(first_name VARCHAR(50), last_name VARCHAR(50), age VARCHAR(50), sex VARCHAR(50));')
conn.commit()
conn.close()


df_readsource = spark.read.jdbc(url=f'jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}', table="source", properties={
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver"
        })
        
# remove special characters
df_readsource = df_readsource.withColumn('first_name', regexp_replace('first_name', r'[^a-zA-Z0-9 ]', ''))
df_readsource = df_readsource.withColumn('last_name', regexp_replace('last_name', r'[^a-zA-Z0-9 ]', ''))

#remove whitespace
df_readsource = df_readsource.withColumn('first_name', regexp_replace('first_name', ' ', ''))
df_readsource = df_readsource.withColumn('last_name', regexp_replace('last_name', ' ', ''))
df_readsource = df_readsource.withColumn('sex', regexp_replace('sex', ' ', ''))

#The first character is uppercase
df_readsource = df_readsource.withColumn('first_name', initcap('first_name'))
df_readsource = df_readsource.withColumn('last_name', initcap('last_name'))
        # df_readsource.show() --> take(n(row)) 

try:
        df_readsource = df_readsource.withColumn("age", col("age").cast(IntegerType()))
except:
        df_readsource = df_readsource.withColumn("age", lit(None).cast(IntegerType()))  # Set value to null if casting fails
        
df_readsource2 = df_readsource.na.drop()
# df_readsource2.show() --> take(n(row)) 
df_readsource2 = df_readsource2.withColumn('sex', 
        when(col('sex').isin(['Female', 'Girl', 'f']), 'F')
        .when(col('sex').isin(['Man', 'Male', 'FM', 'MF']), 'M')
        .when(col('sex').isin(['both']), 'LGBT')
        .when(col('sex').isin(['m']), 'M')
        .when(col('sex') == '-', 'Not Defined')
        .otherwise(col('sex'))
 )

# df_readsource2.show() --> take(n(row)) 
df_readsource2.write.format("jdbc") \
        .option("url", f'jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}') \
        .option("dbtable", "stating") \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .mode("overwrite") \
        .save()