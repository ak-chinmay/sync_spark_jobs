import os

from src.io.spark_connection import SparkConnection
from pyspark.sql import functions as f


def get_spark_session(app_name):
    """
    This method returns the Spark Session
    :param app_name: Name of the application context
    :return: SparkSession
    """
    spark_url = "local[*]"  # Default URL for local test
    if "SPARK_JOB_MASTER" in os.environ and os.environ['SPARK_JOB_MASTER'] is not None:
        spark_url = os.environ['SPARK_JOB_MASTER']
    return SparkConnection().connect_spark(app_name, spark_url)


def get_pivoted_stats(df, group_by_column, datetime_column, mode='MONTH'):
    """
    This generic method performs the pivot transformation returns the pivoted dataframe.
    :param df: PySpark dataframe
    :param group_by_column: Column to be grouped by on
    :param datetime_column: Date time column
    :param mode: 'MONTH'/'YEAR'
    :return: PySpark Dataframe
    """
    modified_df = df
    if mode.upper() == "MONTH":
        modified_df = df.withColumn("time_column",
                                    f.date_format(f.to_date(f.col(datetime_column), "MM"), "MMMM"))
    elif mode.upper() == "YEAR":
        modified_df = df.withColumn("time_column",
                                    f.year(datetime_column))
    else:
        return None

    return modified_df.groupBy(group_by_column).pivot("time_column").count()
