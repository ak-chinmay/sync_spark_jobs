from pyspark.sql.types import *

from src import utils
from pyspark.sql import functions as f

from src.io.export import Export
from src.jobs.player import Player


class Event:
    """ This class represents Entity class """
    EVENT_SCHEMA = StructType([StructField("event_id", LongType(), True),
                               StructField("game_id", LongType(), True),
                               StructField("player_id", LongType(), True),
                               StructField("event_type", StringType(), True),
                               StructField("event_time", TimestampType(), True)])

    def __init__(self, input_file_names):
        self.spark = utils.get_spark_session("event_job")
        self.event_df = self.spark.read.schema(self.EVENT_SCHEMA).json(input_file_names[0])
        self.input_files = input_file_names


    def get_players_with_max_events(self, event_df, event_type):
        """
        This method returns players with maximum events from the data.
        :param event_df: input EventsDF
        :param event_type: 'CALL', 'RAISE' or 'FOLD'
        :return: PySpark Dataframe with events, event_type filtered dataframe, Maximum number of events, Sum of events
        """
        events_calls_df = event_df \
            .filter(event_df.event_type == event_type)  # 1. Operation to get 'CALL'/event_type events
        events_agg_df = events_calls_df \
            .groupby('player_id') \
            .agg(f.expr('count(distinct event_id)').alias("num_events"))  # 2. Events per Player
        # 3. Maximum event occurrence
        max_events = events_agg_df.agg(f.expr("max(num_events)").alias("max_events")).take(1)[0]['max_events']
        sum_events = events_agg_df.agg(f.expr("sum(num_events)").alias("sum_events")).take(1)[0]['sum_events']
        print(f"Max events: {max_events}")
        events_with_maximum_occurrence = events_agg_df.filter(events_agg_df.num_events == max_events)
        return events_with_maximum_occurrence, events_calls_df, max_events, sum_events

    def get_players_with_most_events(self, arg_dict: dict):
        """
        This wrapper method exports the players with most events
        :param arg_dict: contains event_type: 'CALL', 'RAISE' or 'FOLD' and export_path
        """
        export_path = arg_dict['export_path']
        if 'arguments' not in arg_dict:
            raise ValueError("No arguments, please provide")
        print(arg_dict)
        event_type = arg_dict['arguments'][0]

        if len(self.input_files) <= 1:
            raise ValueError("No input path provided for Player Source")
        player_df = self.spark.read.schema(Player.PLAYER_SCHEMA).json(self.input_files[1])

        events_with_maximum_occurrence, events_calls_df, max_events, sum_events = \
            self.get_players_with_max_events(self.event_df, event_type)

        player_filtered_df = player_df.join(f.broadcast(events_with_maximum_occurrence),
                                            player_df.id == events_with_maximum_occurrence.player_id,
                                            "inner")

        event_player_df = events_calls_df.join(f.broadcast(player_filtered_df),
                                               player_filtered_df.id == events_calls_df.player_id,
                                               "inner")
        result_df = event_player_df.select(
            f.concat(f.col('first_name'), f.lit(" "), f.col('last_name')).alias("Player"))
        result_df = result_df.withColumn(f"{event_type}_events", f.lit(max_events))
        Export.export_to_local(result_df, export_path, file_name_prefix="players_with_most_events")

    def get_pivoted_events_type_stats_by_month(self, arg_dict: dict):
        """
        This wrapper method exports the events data pivoted by month in a year.
        :param arg_dict: Contains export_path
        """
        export_path = arg_dict['export_path']
        processed_df = utils.get_pivoted_stats(self.event_df,
                                               group_by_column="event_type",
                                               datetime_column="event_time",
                                               mode="MONTH")
        if processed_df is None:
            raise ValueError("Invalid Pivot mode, please enter MONTH, YEAR")
        Export.export_to_local(processed_df, export_path, file_name_prefix="pivoted_month")

    def get_pivoted_events_type_stats_by_year(self, arg_dict: dict):
        """
        This method exports the pivoted event type stats by year
        :param arg_dict: Consists arg_dict
        """
        export_path = arg_dict['export_path']
        processed_df = utils.get_pivoted_stats(self.event_df,
                                               group_by_column="event_type",
                                               datetime_column="event_time",
                                               mode="YEAR")
        if processed_df is None:
            raise ValueError("Invalid Pivot mode, please enter MONTH, YEAR")
        Export.export_to_local(processed_df, export_path,  file_name_prefix="pivoted_year")
