#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

def initialize_spark():
    return SparkSession.builder \
            .appname("irises") \
            .getOrCreate()

def initialize_data(sc):
    df_no_header =  sc.read.csv("iris.data")
    # To be clear, the easier method would be to read it in
    # as csv and then df = df_no_header.toDF(["my", "cool", "headers"])
    # however it is more expensive to have any inference to type
    # so manually designating the types and headers (even when headers
    # are provided) is good practice in the field.
    schema = StructType([
        StructField("sepal_length", DoubleType()),
        StructField("sepal_width", DoubleType()), 
        StructField("pedal_length", DoubleType()), 
        StructField("pedal_width", DoubleType()), 
        StructField("class", StringType())])

if __name__ == '__main__':
    sc = initialize_spark()
    iris_data = initialize_data(sc)
