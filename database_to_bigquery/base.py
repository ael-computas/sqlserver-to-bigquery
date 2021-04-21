# -*- coding: utf-8 -*-
from time import strftime, localtime
from datetime import timedelta
from typing import List


def elapsed_string(elapsed=None):
    if elapsed is None:
        return strftime("%Y-%m-%d %H:%M:%S", localtime())
    else:
        return str(timedelta(seconds=elapsed))


class DatabaseToCsv:
    """
    TODO: Make into a more generic base class to support more databases.
    """
    pass


class Column:
    def __init__(self, name, data_type):
        self.name = name
        self.data_type = data_type
        self.pk = False

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.name == other.name
        elif isinstance(other, str):
            return self.name == other
        else:
            return False

    def __str__(self):
        return f"{self.name} {self.data_type} [{self.pk}]"


class SplitResult:
    def __init__(self, content_file: str, crc_file: str, elapsed: float, cache_hit: bool, row_count: int):
        self.content_file: str = content_file
        self.crc_file: str = crc_file
        self.elapsed: float = elapsed
        self.cache_hit: bool = cache_hit
        self.row_count: int = row_count

    def __str__(self):
        return f"[{'CACHE' if self.cache_hit else 'RELOAD'}] {self.content_file} {elapsed_string(self.elapsed)}" \
               f" ({self.row_count if self.row_count > 0 else 'unknown'} rows)"


class CopyResult:
    """
    This class contains details about the result of a copy operation in the form of aggregates.
    The split_results contains the results for the individual splits/chunks.
    """
    def __init__(self, table_name: str, table_rows: int, schema_name: str, base_path: str, elapsed_time: float,
                 split_results: List[SplitResult], column_type: List[Column]):
        self.base_path: str = base_path
        self.elapsed_time: float = elapsed_time
        self.split_results: list = split_results
        self.column_type: List[Column] = column_type
        self.table_name: str = table_name
        self.schema_name: str = schema_name
        self.table_rows: int = table_rows

    def is_fully_cached(self) -> bool:
        for split_res in self.split_results:
            if not split_res.cache_hit:
                return False
        return True

    def __str__(self):
        return f"{self.schema_name}.{self.table_name} ({self.table_rows}) -> {self.base_path} - " \
               f"{elapsed_string(self.elapsed_time)}, {len(self.split_results)} splits"


class IngestResult:
    def __init__(self, copy_result: CopyResult,
                 rows_in_table: int,
                 table_id: str,
                 timing_all: float,
                 timing_bigquery: float,
                 bigquery_schema_location: str):
        self.csv_copy_result: CopyResult = copy_result
        self.rows_in_table: int = rows_in_table
        self.table_id: str = table_id
        self.timing_all: float = timing_all
        self.timing_bigquery: float = timing_bigquery
        self.bigquery_schema_location: str = bigquery_schema_location

    def __str__(self) -> str:
        return f"{self.table_id} ({self.rows_in_table}) - {elapsed_string(self.timing_all)}"

    def full_str(self) -> str:
        full_str = [f"{self.table_id} - {elapsed_string(self.timing_all)}",
                    f"\tSQl Server -> CSV",
                    f"\t\tBigQuery Schema: {self.bigquery_schema_location}",
                    f"\t\t{self.csv_copy_result}",
                    f"\t\tSplits:"
                    ]
        for split_result in self.csv_copy_result.split_results:
            full_str.append(f"\t\t\t{split_result}")
        full_str.append(f"\t\tElapsed: {elapsed_string(self.csv_copy_result.elapsed_time)}")
        full_str.append("\tBigQuery:")
        full_str.append(f"\t\t{self.table_id} ({self.rows_in_table})")
        full_str.append(f"\t\tElapsed: {elapsed_string(self.timing_bigquery)}")
        full_str.append("\tTotal:")
        full_str.append(f"\t\tElapsed: {elapsed_string(self.timing_all)}")
        return "\n".join(full_str)


class DatabaseToBigquery:
    """
    TODO: Make into a more generic base class to support more databases.
    """
    pass
