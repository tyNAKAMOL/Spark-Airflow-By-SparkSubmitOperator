from pyspark.sql import SparkSession
import sys
import psycopg2

try:
    spark: SparkSession = SparkSession.builder\
        .appName('Etract')\
        .getOrCreate()


    df = spark.read.csv(sys.argv[6], sep=',', header=True, inferSchema=True)
    # df = spark.read.csv('/opt/data/raw.csv', sep=',', header=True, inferSchema=True)
    
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
    cursor.execute(f'CREATE TABLE IF NOT EXISTS source(first_name VARCHAR(50), last_name VARCHAR(50), age VARCHAR(50), sex VARCHAR(50));')
    conn.commit()
    conn.close()
    
    #insert df into Database
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres-database:5432/data_people") \
        .option("dbtable", "source") \
        .option("user", "local") \
        .option("password", "password") \
        .mode("overwrite") \
        .save()

except Exception as err:
    print(err)  