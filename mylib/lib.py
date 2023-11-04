
import os
import requests
from pyspark.sql import SparkSession

from pyspark.sql.types import (
     StructType, 
     StructField, 
     FloatType, 
     StringType
)


def extract(
    url="""
   https://github.com/fivethirtyeight/data/blob/15f210532b2a642e85738ddefa7a2945d47e2585/world-cup-predictions/wc-20140609-140000.csv?raw=True 
    """,
    file_path="dataset/wc-20140609-140000.csv",
    directory="dataset",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path

def load(spark, data="dataset/wc-20140609-140000.csv", name="WorldCupPredictions"):
    """load data"""
    schema = StructType([
        StructField("country", StringType(), True),
        StructField("country_id", StringType(), True),
        StructField("group", StringType(), True),
        StructField("spi", FloatType(), True),
        StructField("spi_offense", FloatType(), True),
        StructField("spi_defense", FloatType(), True),
        StructField("win_group", FloatType(), True),
        StructField("sixteen", FloatType(), True),
        StructField("quarter", FloatType(), True),
        StructField("semi", FloatType(), True),
        StructField("cup", FloatType(), True),
        StructField("win", FloatType(), True)
    ])

    df = spark.read.option("header", "true").schema(schema).csv(data)

    return df

def describe(df):
    return df.describe().show()

def group_win_prob_query(df, name): 
    """queries using spark sql"""
    spark = SparkSession.builder.appName("WorldCupPredictions").getOrCreate()
    df = df.createOrReplaceTempView(name)
    group_win_prob = spark.sql("SELECT group, AVG(spi) AS avg_soccer_power_in_group, COUNT(win) AS win_possibility FROM world_cup_data GROUP BY group")
    return group_win_prob.show()

def transform(df):
    USA = df.filter(df["country"] == "USA")
    return USA.show()
