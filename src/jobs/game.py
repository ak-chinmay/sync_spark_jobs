from pyspark.sql.types import *

from src import utils
from src.io.export import Export
from pyspark.sql import functions as f, DataFrame


class Game:
    """ This class handles the Game entity jobs """
    GAME_SCHEMA = StructType([StructField("game_id", LongType(), True),
                              StructField("location", StringType(), True),
                              StructField("type", StringType(), True),
                              StructField("started_at", TimestampType(), True),
                              StructField("tournament_id", LongType(), True)])

    def __init__(self, input_file_names):
        self.spark = utils.get_spark_session("region_job")
        self.game_df = self.spark.read.schema(self.GAME_SCHEMA).json(input_file_names[0])
        self.input_file_names = input_file_names

    def get_most_games_by_region(self, df) -> DataFrame:
        """
        This local method returns the most games by region
        :param df: PySpark dataframe
        :return: PySpark dataframe
        """
        most_games_df = df.groupby('location').agg(f.expr('count(distinct game_id)').alias("games"))
        max_games = most_games_df.agg(f.expr("max(games)").alias("max_games")).take(1)[0]['max_games']
        filtered_most_games_df = most_games_df.filter(most_games_df.games == max_games)
        return filtered_most_games_df

    def get_region_with_most_games(self, arg_dict: dict):
        """
        This method exports the region by most games
        :param arg_dict: Contains export_path
        """
        export_path = arg_dict['export_path']
        processed_df = self.get_most_games_by_region(self.game_df)
        Export.export_to_local(processed_df, export_path, file_name_prefix="region_with_most_games")

    def get_region_with_most_games_in_dates(self, arg_dict: dict):
        """
        This wrapper method exports the region by most games between certain dates
        :param arg_dict: Contains export_path, arguments containing start_date and end_date
        """
        export_path = arg_dict['export_path']
        if 'arguments' not in arg_dict:
            raise ValueError("No arguments, please provide")

        start_date = arg_dict['arguments'][0]
        end_date = arg_dict['arguments'][1]

        filtered_game_df = self.game_df.filter(
            (self.game_df.started_at >= start_date) & (self.game_df.started_at < end_date))
        processed_df = self.get_most_games_by_region(filtered_game_df)
        Export.export_to_local(processed_df, export_path, file_name_prefix="region_with_most_games_dates")

    def get_stats_for_each_region(self, arg_dict: dict):
        """
        This wrapper method exports the stats for each region
        :param arg_dict: Containing the export_path
        """
        export_path = arg_dict['export_path']
        stats_for_each_region = self.game_df.groupby('location', 'type').agg(
            f.expr('count(distinct game_id)').alias("games"))
        Export.export_to_local(stats_for_each_region, export_path, file_name_prefix="stats_for_region")


if __name__ == '__main__':
    Game().get_region_with_most_games("../../export/game_sept_2021.json", "../../export/most_games_by_region.csv")
    # Game().get_region_with_most_games_in_dates("../../export/game_sept_2021.json", "1999-06-20", "2005-06-20",
    #                                            "../../export/most_games_by_region_within_dates.csv")
    # Game().get_stats_for_each_region("../../export/game_sept_2021.json", "../../export/stats_by_region.csv")
