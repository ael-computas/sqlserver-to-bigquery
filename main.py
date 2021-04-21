import os
from database_to_bigquery.sql_server import SqlServerToCsv, SqlServerToBigquery
import logging
from dataclasses import dataclass
import yaml


logging.basicConfig(level=logging.ERROR,
                    format='%(levelname)s - %(module)s - %(message)s')

logger = logging.getLogger("database-to-bigquery")
logger.setLevel(logging.INFO)


@dataclass
class Config:
    username: str
    password: str
    host: str
    database: str
    bucket: str
    dataset: str
    table: str
    split_size: int = -1
    sql_server_schema: str = "dbo"


def get_env_config(override_dict) -> Config:
    username = os.getenv("DB_USERNAME", None) or override_dict.get("db_username", None)
    assert username, "Missing DB_USERNAME env variable or in config"
    password = os.getenv("DB_PASSWORD", None) or override_dict.get("db_password", None)
    assert password, "Missing DB_PASSWORD env variable or in config"
    host = os.getenv("DB_HOST", None) or override_dict.get("db_host", None)
    assert host, "Missing DB_HOST env variable or in config"
    database = os.getenv("DB_DATABASE", None) or override_dict.get("db_database", None)
    assert database, "Missing DB_DATABASE env variable or in config"

    bucket = os.getenv("GCS_BUCKET", None) or override_dict.get("gcs_bucket", None)
    assert bucket, "Missing GCS_BUCKET env variable"
    dataset = os.getenv("BQ_DATASET", None) or override_dict.get("bq_dataset", None)
    assert dataset, "Missing DATASET env variable"
    table = os.getenv("DB_TABLE", None) or override_dict.get("db_table", None)
    assert table, "Missing DB_TABLE env variable"
    split_size = int(os.getenv("SPLIT_SIZE", None) or override_dict.get("split_size", -1))
    sql_server_schema = os.getenv("SQL_SERVER_SCHEMA", None) or override_dict.get("sql_server_schema", "dbo")
    return Config(
        username=username,
        password=password,
        host=host,
        database=database,
        bucket=bucket,
        dataset=dataset,
        table=table,
        split_size=split_size,
        sql_server_schema=sql_server_schema)


def get_config() -> Config:

    if os.getenv("SECRETMANAGER_URI", None):
        from google.cloud import secretmanager

        name = os.getenv("SECRETMANAGER_URI", "")
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={"name": name})

        response_payload = response.payload.data.decode("UTF-8")
        p = yaml.load(response_payload, Loader=yaml.SafeLoader)
        return get_env_config(p)
    elif os.getenv("CONFIG_FILE", None):
        with open(os.getenv("CONFIG_FILE"), "r") as cfg:
            p = yaml.load(cfg, Loader=yaml.SafeLoader)
            return get_env_config(p)
    else:
        return get_env_config({})


if __name__ == '__main__':
    config = get_config()

    logger.info(f"Connecting to {config.username}/{config.database}@{config.host} and syncing table: {config.table} to {config.bucket}")

    sql_server_to_csv = SqlServerToCsv(username=config.username,
                                       password=config.password,
                                       host=config.host,
                                       database=config.database,
                                       destination=f"gs://{config.bucket}/sqlserver/{config.dataset}")

    bigquery = SqlServerToBigquery(sql_server_to_csv=sql_server_to_csv)

    result = bigquery.ingest_table(sql_server_table=config.table,
                                   sql_server_schema="dbo",
                                   bigquery_destination_project=os.getenv("GOOGLE_CLOUD_PROJECT"),
                                   bigquery_destination_dataset=config.dataset,
                                   split_size=config.split_size)
    logger.info(result.full_str())
