from pyspark.sql.types import *

from src import utils


class Player:
    """ This class represents the Player entity """

    PLAYER_SCHEMA = StructType([StructField("id", LongType(), True),
                                StructField("last_name", StringType(), True),
                                StructField("first_name", StringType(), True),
                                StructField("address", StringType(), True),
                                StructField("email", StringType(), True),
                                StructField("phone", StringType(), True),
                                StructField("description", StringType(), True)
                                ])