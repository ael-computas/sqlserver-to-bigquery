# -*- coding: utf-8 -*-
#import pymssql
import json
from typing import Tuple, List, Optional
import logging
import smart_open
from smart_open.gcs import UploadFailedError
from sqlalchemy.exc import OperationalError
import csv
import decimal
import platform
import os
import pyodbc
import backoff
from time import time
from google.cloud import bigquery
from sqlalchemy import create_engine
from sqlalchemy import engine
from database_to_bigquery.base import DatabaseToCsv, elapsed_string, Column, CopyResult, SplitResult, IngestResult, \
    DatabaseToBigquery

logger = logging.getLogger("DatabaseToBigquery")
logger.setLevel(logging.INFO)
logging.getLogger('backoff').addHandler(logging.StreamHandler())

driver = '{ODBC Driver 17 for SQL Server}'
if platform.system() == 'Windows':
    driver = '{SQL Server}'


def retry_https_status_codes():
    return [503]


class SqlServerToCsv(DatabaseToCsv):
    """
    TODO: a lot of the concepts in this class can/should be moved to base class.

    This class handles reading from a SQL Server and exporting that to CSV files, together with a schema and crc.

    The program tries to have a small memory footprint, and streams the data to GCS using the library "smart open" to
    handle this.
    """
    SPLIT_MIN_SIZE = 1000000
    CSV_CONTENT_POSTFIX = "content"

    SPLIT_NO_SPLIT = -1
    SPLIT_DYNAMIC = 0

    def __init__(self, username: str, password: str, host: str, database: str, destination: str,
                 extra_crc_fields: Optional[List[str]] = None):
        """

        :param username:
        :param password:
        :param host:
        :param database:
        :param destination:
        :param extra_crc_fields: Optional list of extra fields that will be used for crc in splits.
                                 will do min/max on the field in the group by. Will be gracefully ignored if field doesnt exist.
        """
        self.username: str = username
        self.password: str = password
        self.host: str = host
        self.database: str = database
        self.destination: str = destination
        self.port = os.getenv("DB_PORT", "1433")
        self.connection_driver = os.getenv("DB_DRIVER", driver)

        # This method seems to handle cases where instance name is supplied better than sqlalchemy create engine with
        # an url string.
        self.sql_engine = create_engine('mssql://', creator=lambda x: pyodbc.connect(driver=self.connection_driver,
                                                                                     server=self.host,
                                                                                     database=self.database,
                                                                                     uid=self.username,
                                                                                     pwd=self.password,
                                                                                     port=self.port)
                                        )
        self.strip_char_type = True
        # bigquery doesnt really have blobs/lobs/..
        self.ignore_mssql_types = ["VARBINARY"]
        if extra_crc_fields:
            self.extra_crc_fields = [f for f in extra_crc_fields]
        else:
            self.extra_crc_fields = None

    @backoff.on_exception(backoff.expo,
                          OperationalError,
                          max_tries=8,
                          jitter=None,
                          max_time=300,
                          giveup=lambda e: "timeout" not in f"{e}")
    def connect(self) -> engine.Connection:
        logger.info(f"Connecting to {self.database} on {self.host} as user {self.username}")
        return self.sql_engine.connect()

    def safe_cast(self, the_value):
        """
        Cast the value to something bigquery can handle
        """
        if isinstance(the_value, decimal.Decimal):
            # Cast to string so we dont loose precision by casting to float.
            return str(the_value)
        else:
            if isinstance(the_value, str):
                # bigquery fails if loading null terminations.
                return the_value.replace(b"\x00".decode(), "")
            return the_value

    def row_to_bq(self, mssql_row: dict, mssql_datatypes: dict) -> dict:
        """
        Convert a row of data to something that is compatible with bigquery
        :param mssql_row:
        :param mssql_datatypes:
        :return: a row with datatyoes that are compatible with BigQuery
        """
        bq_row = {}
        for k, v in mssql_row.items():
            if mssql_datatypes[k].upper() == 'CHAR' and self.strip_char_type:
                if v:
                    bq_row[k] = v.strip()
                else:
                    bq_row[k] = None
            else:
                bq_row[k] = self.safe_cast(v)

        return bq_row

    def _get_columns_with_access(self, connection: engine.Connection, tbl_schema: str, tbl_name: str) -> Optional[List[str]]:
        try:
            res = next(connection.execute(f"SELECT TOP 1 * FROM {tbl_schema}.{tbl_name}"))

            columns = [i for i in res.keys()]
            return columns
        except StopIteration as empty_table:
            return None

    def get_columns(self, tbl_schema: str, tbl_name: str) -> Tuple[List[Column], List[str]]:
        """
        Gets a list of columns belonging to the table tbl_name
        :param tbl_schema: schema where the table resides.
        :param tbl_name: name of the table
        :return: A tuple containing a list of columns + list of primary keys
        """
        with self.connect() as connection:
            accessable_columns = self._get_columns_with_access(connection, tbl_schema, tbl_name)
            schema_res = connection.execute("SELECT C.column_name, C.data_type from INFORMATION_SCHEMA.COLUMNS as C WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", (tbl_schema, tbl_name))
            columns = []
            debug_data = []
            for schema_row in schema_res:
                column = Column(name=schema_row['column_name'],
                                data_type=schema_row['data_type'].upper())
                accessable = accessable_columns is None or column.name in accessable_columns
                if schema_row['data_type'].upper() not in self.ignore_mssql_types:
                    if accessable:
                        columns.append(column)
                if not accessable:
                    logger.warning(f"{column} is not accessbile when doing select top 1 * from {tbl_schema}.{tbl_name},"
                                   f" removing from column list.")
                debug_data.append(column)
            if len(columns) == 0:
                print(f"Oh no. No columns on table {tbl_name} after filtering.  Dumping debug stack!")
                print(
                    f"SELECT C.column_name, C.data_type from INFORMATION_SCHEMA.COLUMNS as C WHERE TABLE_SCHEMA='{tbl_schema}' AND TABLE_NAME='{tbl_name}'")
                for d in debug_data:
                    print(d)
                raise RuntimeError("im broken.")

            sql = "SELECT  K.TABLE_NAME , K.COLUMN_NAME , K.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS K ON C.TABLE_NAME = K.TABLE_NAME AND C.CONSTRAINT_CATALOG = K.CONSTRAINT_CATALOG AND C.CONSTRAINT_SCHEMA = K.CONSTRAINT_SCHEMA AND C.CONSTRAINT_NAME = K.CONSTRAINT_NAME WHERE C.CONSTRAINT_TYPE = 'PRIMARY KEY' AND K.TABLE_NAME=?"
            pk_res = connection.execute(sql, tbl_name)
            pk_list = []
            for pk_row in pk_res:
                pk_column = pk_row['COLUMN_NAME']
                pk_list.append(pk_column)
                columns[columns.index(pk_column)].pk = True

            return columns, pk_list

    def _generate_view_sql(self, table: str, schema: str, columns: list, split_keys: list, split_size: int):
        if split_size > 0:
            sql_view = f"WITH splits AS (SELECT (ROW_NUMBER() OVER(ORDER BY {','.join(split_keys)})) / {split_size} + 1 as internal_split,{','.join(columns)} from {schema}.{table})"
        else:
            sql_view = f"WITH splits AS (SELECT 1 AS internal_split,{','.join(columns)} from {schema}.{table})"
        return sql_view

    # TODO: maybe tmp table to lock records.
    def generate_splits(self, table: str, schema: str, columns: list, split_keys: list, split_size: int) -> dict:
        """
        Split table into chunks.
        Generate a split for empty sources as well.
        """
        splits = {}
        sql_view = self._generate_view_sql(table=table, schema=schema, columns=columns, split_keys=split_keys,
                                           split_size=split_size)
        minmax_keys = [c for c in split_keys]
        if self.extra_crc_fields:
            for c in self.extra_crc_fields:
                if c in columns:
                    minmax_keys.append(c)
        sql_minmax = ",".join([f"MIN({c}) as {c}_min, MAX({c}) as {c}_max" for c in minmax_keys])
        if len(sql_minmax) > 0:
            sql_minmax += ","
        sql = f"select {split_size} AS split_size," \
              f"internal_split, " \
              f"count(*) as cnt," \
              f"{sql_minmax}" \
              f"CHECKSUM_AGG(CHECKSUM(*)) as crc " \
              f"from splits group by internal_split"

        sql_from_view = f"{sql_view} {sql}"
        logger.info(f"GENERATED SPLIT SQL: {sql_from_view}")
        with self.connect() as connection:
            split_res = connection.execute(sql_from_view)
            cnt = 0
            for split in split_res:
                cnt += 1
                splits[split['internal_split']] = dict(split)
                logger.info(f"Split with id {split['internal_split']} has crc = {split}")
            if cnt == 0:
                logger.warning(f"Source table {schema}.{table} is EMPTY. generating an empty split")
                splits[1] = {
                    'split_size': split_size,
                    'internal_split': 1,
                    'cnt': 0
                }
        return splits

    @backoff.on_exception(backoff.expo,
                          UploadFailedError,
                          max_tries=8,
                          jitter=None,
                          max_time=300,
                          giveup=lambda e: e.status_code not in retry_https_status_codes())
    def destination_result_exists(self, split: dict, destination_file: str) -> bool:
        split_id = split['internal_split']
        split_size = split['split_size']
        split_data = json.dumps(split, default=str).encode()
        crc_location = self.crc_location(self.base_destination(destination_file, split_size),split_id)
        content_location = self.content_location(self.base_destination(destination_file, split_size),split_id)
        try:
            with smart_open.open(f"{crc_location}") as crc:
                existing_content = crc.read()
                if existing_content == split_data.decode():
                    logger.info(
                        f"{destination_file}: A resultset exists at destination, and CRC is matching, verifying csv.")
                    # This will throw if file not exists.
                    with smart_open.open(content_location) as tmp:
                        logger.info(f"{destination_file}: Content file is present.")
                    return True
                else:
                    logger.info(
                        f"{destination_file}: A resultset exists at destination ({content_location}), but it has changed.")
                    logger.info(f"{existing_content}")
                    logger.info(f" != ")
                    logger.info(f"{split_data.decode()}")

        except Exception:
            pass
        return False

    def base_destination(self, destination_file: str, split_size: int) -> str:
        """
        base destination is destination/destination_file/split/destination_file*

        Splits is used to put in separate folders, because we the split changes, we will have less files, and if we
        load by * then the old files will also be picked up unless we clean them up.

        :param split_size: The split size we are using.
        :param destination_file: the folder where to place the data.
        :return: a string representing the base destination.
        """
        split_folder = f"{split_size}/" if split_size > 0 else ""
        return f"{self.destination}/{destination_file}/{split_folder}{destination_file}"

    def content_location(self, base_destination: str, split_id: int) -> str:
        return f"{base_destination}-{self.CSV_CONTENT_POSTFIX}-{split_id}.csv"

    def crc_location(self, base_destination: str, split_id: int) -> str:
        return f"{base_destination}-{split_id}.crc"

    @backoff.on_exception(backoff.expo,
                          UploadFailedError,
                          max_tries=8,
                          jitter=None,
                          max_time=300,
                          giveup=lambda e: e.status_code not in retry_https_status_codes())
    def write_split_to_destination(self, split: dict, destination_folder: str, table: str, schema: str,
                                   columns_type: List[Column], split_keys: list) -> int:
        split_id = split['internal_split']
        split_size = split['split_size']
        expected_rows = split['cnt'] if split['cnt'] > 0 else 1
        columns = [c.name for c in columns_type]
        location = self.base_destination(destination_folder, split_size)
        content_location = self.content_location(location, split_id)
        crc_location = self.crc_location(location, split_id)

        with smart_open.open(content_location, "w") as split_destination:
            cnt = 0
            logger.info(
                f"Going to write {expected_rows} rows to {content_location}")
            writer = csv.DictWriter(split_destination, fieldnames=columns, quotechar='"')
            writer.writeheader()
            sql_view = self._generate_view_sql(table=table, schema=schema, columns=columns, split_keys=split_keys,
                                               split_size=split_size)
            sql = f"select {','.join(columns)} from splits where internal_split=?"

            sql_from_view = f"{sql_view} {sql}"
            logger.debug(f"{destination_folder}: GENERATED SQL: {sql_from_view}")
            with self.connect() as connection:
                split_res = connection.execute(sql_from_view, split_id)
                mssql_datatypes = {c.name: c.data_type for c in columns_type}
                print_msg = 10  # percent

                # Processing of split 23 took 0:03:21.707622 size=1000
                # Processing of split 23 took 0:03:30.559677 size=100
                if True:
                    batch_size = 500
                    split_rows = split_res.fetchmany(size=batch_size)
                    while split_rows:
                        for split_row in split_rows:
                            writer.writerow(self.row_to_bq(split_row, mssql_datatypes))
                            cnt += 1
                            if cnt / expected_rows * 100 >= print_msg:
                                logger.info(f"{table} {cnt} / {expected_rows} [{print_msg}%]")
                                print_msg += 10
                        split_rows = split_res.fetchmany(size=batch_size)
        with smart_open.open(crc_location, "w") as split_crc:
            logger.info(
                f"{destination_folder}: Writing CRC to destination {crc_location}")
            split_data = json.dumps(split, default=str)
            logger.info(f"{destination_folder}: crc payload = {split_data}")
            split_crc.write(split_data)
        return cnt

    def process_split(self, split: dict, columns_type: List[Column], primary_keys: List[str], table: str,
                      schema: str, destination_folder: str) -> SplitResult:
        """
        Process the split as specified by split.  in theory, the split can come from another process or machine to
        process splits distributed.  This has not been tested and probably requires some work to do properly.
        
        :param split: The split
        :param columns_type: A list of columns to read from table. 
        :param primary_keys: The primary keys for the table.
        :param table: The table to process
        :param schema: The schema where the table exists.
        :param destination_folder: The destination folder to put this.
        :return: 
        """
        split_id = split['internal_split']
        split_size = split['split_size']
        cache_hit = False
        rows = -1
        base_destination = self.base_destination(destination_file=destination_folder, split_size=split_size)
        logger.info(f"{destination_folder}: Processing split {split_id}")
        start = time()
        if self.destination_result_exists(split=split, destination_file=destination_folder):
            logger.info(f"{destination_folder}: Nothing to do here, file already exists with correct crc.")
            cache_hit = True
        else:
            rows = self.write_split_to_destination(split=split, destination_folder=destination_folder, table=table,
                                                   schema=schema, columns_type=columns_type, split_keys=primary_keys)
        end = time()
        elapsed = end - start

        logger.info(
            f"{destination_folder}: Processing of split {split_id} took {elapsed_string(elapsed=elapsed)}")

        return SplitResult(content_file=self.content_location(base_destination, split_id),
                           crc_file=self.crc_location(base_destination, split_id),
                           elapsed=elapsed,
                           cache_hit=cache_hit,
                           row_count=rows)

    def get_rows(self, table: str, schema: str) -> int:
        """
        Get number of rows from source database.

        :param table: The table
        :param schema: schema - typically dbo
        :return:
        """
        with self.connect() as connection:
            res = connection.execute(f"SELECT COUNT(*) as cnt FROM {schema}.{table}")
            count = res.first()
            return count['cnt']

    def calculate_dynamic_split(self, row_count: int) -> int:
        """
        Do a best effort to calculate the split.  We do not want to change the split to often, because that means
        that we have to copy the entire table next time we run this.

        :param row_count: The number of rows in the table.
        :return: a number that indicates the split size, or self.SPLIT_NO_SPLIT if we do not want to split the table.
        """
        wanted_splits = 10
        if row_count < (self.SPLIT_MIN_SIZE * 1.5):
            return self.SPLIT_NO_SPLIT
        min_split_jump = 1000000
        # keep minimum split jump to min_split_jump
        # 20.000.000 / 10*1000.000 = 2 * 1000.000 = 2m
        # 15.000.000 / 10*1000.000 = 1 * 1000.000 = 1m
        split_size = max(int(row_count / (wanted_splits * min_split_jump)) * min_split_jump,
                         self.SPLIT_MIN_SIZE)
        return split_size

    def copy_table(self, table: str, sql_server_schema: str, destination_folder: str, split_size: int = -1) -> CopyResult:
        """
        Copy a table from SQL server to a destination folder, containing one or more files.
        A crc will be generated for each file, and if further copies are attempted the crc will be checked before
        reading a massive volume from the database.

        A Best effort will be made to split the table up into N chunks with max size of split_size.

        :param table: the table/view to copy
        :param sql_server_schema: the schema where the table existss
        :param destination_folder: the destination folder
        :param split_size: how many splits to do.  -1 means no splits.
        :return:
        """
        start = time()
        if 0 < split_size < self.SPLIT_MIN_SIZE:
            logger.warning(
                f"Split size is set to {split_size}, which is less than the suggested minimum low of {self.SPLIT_MIN_SIZE}")
        table_rows = self.get_rows(table=table, schema=sql_server_schema)
        if split_size == self.SPLIT_DYNAMIC:
            split_size = self.calculate_dynamic_split(table_rows)
            logger.info(f"{table}: Dynamic split size set to {split_size if split_size != -1 else 'NO_SPLIT'}!")
        base_location = self.base_destination(destination_folder, split_size)
        columns_type, primary_keys = self.get_columns(tbl_name=table, tbl_schema=sql_server_schema)
        columns = [c.name for c in columns_type]
        splits = self.generate_splits(table=table, schema=sql_server_schema, columns=columns, split_keys=primary_keys,
                                      split_size=split_size)
        split_results = []
        cnt = 0
        for split_id, split in splits.items():
            res = self.process_split(split=split, columns_type=columns_type, primary_keys=primary_keys,
                                     table=table, schema=sql_server_schema,
                                     destination_folder=destination_folder)
            cnt += 1
            logger.info(f"Split {cnt} / {len(splits)} Done!")
            logger.info(f"{res}")
            split_results.append(res)
        end = time()
        elapsed = end - start
        return CopyResult(table_name=table,
                          schema_name=sql_server_schema,
                          table_rows=table_rows,
                          base_path=base_location,
                          elapsed_time=elapsed,
                          split_results=split_results,
                          column_type=columns_type)


class SqlServerToBigquery(DatabaseToBigquery):
    BIGQUERY_SCHEMA_POSTFIX = "schema"

    def __init__(self, sql_server_to_csv: SqlServerToCsv):
        self.sql_server_to_csv = sql_server_to_csv
        self.bigquery_client = bigquery.Client()

    def bigquery_schema_location(self, base_destination: str) -> str:
        return f"{base_destination}-{self.BIGQUERY_SCHEMA_POSTFIX}.json"

    def bq_type(self, sql_server_type: Column):
        conversion = {
            "DATETIME": "TIMESTAMP",
            "NUMBER": "NUMERIC",
            "DECIMAL": "FLOAT64",
            "FLOAT": "FLOAT",
            "INT": "INT64"
        }
        for sql_server_type_from, bigquery_type_to in conversion.items():
            if sql_server_type_from in sql_server_type.data_type:
                return bigquery_type_to
        return "STRING"

    def calculate_bigquery_schema(self, columns_type: List[Column]) -> list:
        """
        Convert SQL Server schema into a bigquery schema.
        If type is unknown we will default to string

        :param columns_type: a list of SQL server columns we are reading
        :return: a list of bigquery types
        """
        bigquery_schema = []
        for column in columns_type:
            bigquery_schema.append(
                bigquery.SchemaField(column.name, self.bq_type(sql_server_type=column))
            )
        return bigquery_schema

    @backoff.on_exception(backoff.expo,
                          UploadFailedError,
                          max_tries=8,
                          jitter=None,
                          max_time=300,
                          giveup=lambda e: e.status_code not in retry_https_status_codes())
    def write_bigquery_schema(self, columns_type: List[Column], bigquery_schema_location: str):
        """
        Generate a BigQuery schema definition file and write to to location, with a postfix of -ddl.json

        :param columns_type:
        :param bigquery_schema_location:

        :return: No return
        """
        logger.info(f"Writing Schema defintion to destination {bigquery_schema_location}")
        bigquery_ddl = self.calculate_bigquery_schema(columns_type)
        schema = [{'name': c.name, 'type': c.field_type, 'mode': 'NULLABLE'} for c in bigquery_ddl]
        with smart_open.open(bigquery_schema_location, "w") as bigquery_ddl_json:
            bigquery_ddl_json.write(json.dumps(schema, indent=4))

    def should_load_table(self, copy_result: CopyResult, table_id: str):
        try:
            destination_table = self.bigquery_client.get_table(table_id)
            if copy_result.is_fully_cached() and destination_table.num_rows == copy_result.table_rows:
                return False
        except:
            pass
        return True

    def ingest_table(self, sql_server_table: str, sql_server_schema: str,
                     bigquery_destination_project: str,
                     bigquery_destination_dataset: str,
                     split_size=SqlServerToCsv.SPLIT_DYNAMIC) -> IngestResult:
        """
        Ingest the sql_server_table into bigquery.
        One or more csv files will be generated and placed in gcs storage.  The export to csv tries to be smart, and
        partitions the result by key(s) into chunks that will be cached.  Cached data will not be read from the source
        until the cache is invalid.  To calculate the cache, CHECKSUM is used.  If you need a better crc, it is also
        possible to specify date fields that will be used for min/max calulations.

        the bigquery schema json will also be placed in gcs storage.

        After the data has been successfully exported to gcs, a load job will be created.  The ingestion will wait sync
        for this job to finish.

        :param sql_server_table: the sql server table name to ingest
        :param sql_server_schema: the schema where the table exists (typically dbo)
        :param bigquery_destination_project: the bigquery project id where the dataset exist.
        :param bigquery_destination_dataset: the bigquery dataset.
        :param split_size: how big the partitions should be
        :return: a result object containing the ingestion results.
        """
        start_all = time()
        result = self.sql_server_to_csv.copy_table(table=sql_server_table,
                                                   sql_server_schema=sql_server_schema,
                                                   destination_folder=sql_server_table,
                                                   split_size=split_size)
        table_id = f"{bigquery_destination_project}.{bigquery_destination_dataset}.{sql_server_table}"

        if not self.should_load_table(copy_result=result, table_id=table_id) \
                and (os.getenv("DISABLE_LOAD_CACHE", None) is None):
            logger.info(f"Skipping loading result to {table_id}, result is previously cached and rows match.")
            end = time()
            return IngestResult(copy_result=result,
                                rows_in_table=result.table_rows,
                                table_id=table_id,
                                timing_all=end - start_all,
                                timing_bigquery=0,
                                bigquery_schema_location=self.bigquery_schema_location(result.base_path))

        start_bigquery = time()
        self.write_bigquery_schema(columns_type=result.column_type,
                                   bigquery_schema_location=self.bigquery_schema_location(result.base_path))

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=self.calculate_bigquery_schema(result.column_type),
            allow_quoted_newlines=True,
        )

        uri = f"{result.base_path}-{SqlServerToCsv.CSV_CONTENT_POSTFIX}*.csv"

        logger.info(f"Importing data to BigQuery table {table_id}, with content from {uri}")
        load_job = self.bigquery_client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        logger.info("Waiting for ingestion job to finish...")
        load_job.result()  # Waits for the job to complete.
        logger.info("Ingestion job finished.")

        destination_table = self.bigquery_client.get_table(table_id)

        end = time()
        return IngestResult(copy_result=result,
                            rows_in_table=destination_table.num_rows,
                            table_id=table_id,
                            timing_all=end-start_all,
                            timing_bigquery=end-start_bigquery,
                            bigquery_schema_location=self.bigquery_schema_location(result.base_path))
