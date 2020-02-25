#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
import ipdb  #for debugging! :D

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

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Iris").getOrCreate()
    extracted = etl_extract(spark)
