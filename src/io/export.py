import os
import pathlib
from datetime import datetime

from pyspark.sql import DataFrame


class Export:
    @staticmethod
    def process_file_name(path, placeholder_prefix):
        dir_path = pathlib.Path(path).parent
        file_name = os.path.basename(path)
        file_name_prefix = datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
        if file_name == "":
            file_name = placeholder_prefix
        file_name = f"{file_name_prefix}_{file_name}"
        file_path = os.path.join(dir_path, file_name)
        file_path = os.path.realpath(file_path)
        return file_path

    @staticmethod
    def export_to_local(df: DataFrame, path, is_single_file=True, file_name_prefix="export"):
        path = Export.process_file_name(path, file_name_prefix)
        if is_single_file:
            df = df.coalesce(1)
        df.write.option("header", True).csv(path)
