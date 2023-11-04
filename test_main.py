"""
Test goes here

"""
import os
from mylib.lib import (
    extract,
    load,
    describe,
    group_win_prob_query,
    transform,
)
from pyspark.sql import SparkSession


def test_extract():
    file_path = extract()
    assert os.path.exists(file_path) is True


def test_load(spark):
    df = load(spark)
    assert df is not None


def test_describe(spark):
    df = load(spark)
    result = describe(df)
    assert result is None


def test_query(spark):
    df = load(spark)
    name = "world_cup_data"
    result = group_win_prob_query(df, name)
    assert result is None


def test_transform(spark):
    df = load(spark)
    result = transform(df)
    assert result is None


if __name__ == "__main__":
    spark = SparkSession.builder.appName("WorldCupPredictions").getOrCreate()
    test_extract()
    test_load(spark)
    test_describe(spark)
    test_query(spark)
    test_transform(spark)
    spark.stop()
