from pyspark.sql import SparkSession
import psycopg2
import sys

try:
        spark: SparkSession = SparkSession.builder\
                .appName('Load')\
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

        cursor = conn.cursor()
        cursor.execute(f'CREATE TABLE IF NOT EXISTS final(first_name VARCHAR(50), last_name VARCHAR(50), age VARCHAR(50), sex VARCHAR(50));')
        conn.commit()
        conn.close()

        df_readstaing = spark.read.jdbc(url=f'jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}', table="stating", properties={
        "user": "local",
        "password": "password",
        "driver": "org.postgresql.Driver"
        })
        df_readstaing.show()

        df_readstaing.write.format("jdbc") \
        .option("url", f'jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}') \
        .option("dbtable", "final") \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .mode("overwrite") \
        .save()
except Exception as err:
    print(err) 