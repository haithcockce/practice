#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.sql.functions import monotonically_increasing_id as mon_inc_id
import ipdb  #for debugging! :D

db_props = {
        "url": "jdbc:postgresql://localhost/iris",
        "dbtable": "iris_table",
        "user": "postgres", 
        "password": "notasecurepassword",
        "driver": "org.postgresql.Driver",
        'mode': 'overwrite'
        }

def create_app():
    return SparkSession.builder \
            .config("spark.jars", "./postgresql-42.2.10.jar") \
            .appName("iris") \
            .getOrCreate()


def etl_extract(spark):
    """
    """
    # Constructing custom schema. Unecessary, however, predefined custom
    # schemas reduce processing time by removing need to determine
    # schema via interpreting and autodetecting it
    schema = StructType([
        StructField("sepal_length", DoubleType()), 
        StructField("sepal_width", DoubleType()),
        StructField("pedal_length", DoubleType()),
        StructField("pedal_width", DoubleType()), 
        StructField("class", StringType())])
    return spark.read.csv("iris.data", schema=schema)


def etl_transform(spark, df):
    """Adding a column for primary keys for database use.
    
    With larger data sets with NaNs/Nulls, more goes here, 
    but not with the iris data set.
    """
    return df.withColumn("id", mon_inc_id())

def etl_load(spark, df):
    """Write this new data into a database
    """
    # Create the table first. Not sure if it's more "correct" to do this
    # outside of pyspark via a script or if doing it here is ok. 
    table_col_types = "sepal_length FLOAT, " + \
            "sepal_width FLOAT, " + \
            "pedal_length FLOAT, " + \
            "pedal_width FLOAT, " + \
            "class VARCHAR(1000), " + \
            "id INT"
    df.write.option('createTableColumnTypes', table_col_types) \
            .jdbc(url=db_props['url'],
                    mode=db_props['mode'],
                    table=db_props['dbtable'],
                    properties={'driver': db_props['driver'],
                        'user': db_props['user'],
                        'password': db_props['password']})

def check_insertion(spark, amount):
    row_count = spark.read.jdbc(url=db_props['url'],
            table=db_props['dbtable'],
            properties={'driver': db_props['driver'],
                'user': db_props['user'],
                'password': db_props['password']}) \
            .count()

    return row_count == amount

if __name__ == '__main__':
    spark = create_app()
    iris_data = etl_extract(spark)
    iris_data = etl_transform(spark, iris_data)
    etl_load(spark, iris_data)
    print('Successfully inserted 150 records') if check_insertion(spark, 150) else print("UH OH Try again")
    db_props['mode'] = 'append'
    etl_load(spark, iris_data)
    print('Successfully inserted another 150 records') if check_insertion(spark, 300) else print("UH OH Try again")

    spark.stop()
