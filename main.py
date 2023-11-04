from mylib.lib import (
    extract,
    load,
    describe,
    group_win_prob_query,
    transform,
)
from pyspark.sql import SparkSession


def main():
    # extract data
    extract()
    # start spark session
    spark = SparkSession.builder.appName("WorldCupPredictions").getOrCreate()
    # load data into dataframe
    df = load(spark)
    # example metrics
    describe(df)
    # query
    group_win_prob_query(df,"world_cup_data")
    # example transform
    transform(df)
    # end spark session
    spark.stop()


if __name__ == "__main__":
    main()
