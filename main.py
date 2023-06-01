import os
from database_to_bigquery.sql_server import SqlServerToCsv, SqlServerToBigquery
import logging
from dataclasses import dataclass
import yaml


logging.basicConfig(
    level=logging.ERROR, format="%(levelname)s - %(module)s - %(message)s"
)

logger = logging.getLogger("database-to-bigquery")
logger.setLevel(logging.INFO)


@dataclass
class Config:
    db_username: str
    db_password: str
    db_host: str
    db_database: str
    gcp_bucket: str
    gcp_bq_dataset: str
    gcp_target_project: str
    db_table: str
    split_size: int = -1
    sql_server_schema: str = "dbo"
    threads: int = -1


def get_env_config(override_dict) -> Config:
    username = os.getenv("DB_USERNAME", None) or override_dict.get("db_username", None)
    assert username, "Missing DB_USERNAME env variable or in config"
    password = os.getenv("DB_PASSWORD", None) or override_dict.get("db_password", None)
    assert password, "Missing DB_PASSWORD env variable or in config"
    host = os.getenv("DB_HOST", None) or override_dict.get("db_host", None)
    assert host, "Missing DB_HOST env variable or in config"
    assert (
        ":" not in host
    ), "Port should not be specified in host, use DB_PORT env variable to override port (1433)"
    database = os.getenv("DB_DATABASE", None) or override_dict.get("db_database", None)
    assert database, "Missing DB_DATABASE env variable or in config"

    bucket = os.getenv("GCS_BUCKET", None) or override_dict.get("gcs_bucket", None)
    assert bucket, "Missing GCS_BUCKET env variable"
    dataset = os.getenv("BQ_DATASET", None) or override_dict.get("bq_dataset", None)
    assert dataset, "Missing BQ_DATASET env variable"
    target_gcp_project = os.getenv("TARGET_GCP_PROJECT", None) or override_dict.get(
        "target_gcp_project", None
    )
    assert target_gcp_project, "Missing TARGET_GCP_PROJECT env variable"
    table = os.getenv("DB_TABLE", None) or override_dict.get("db_table", None)
    assert table, "Missing DB_TABLE env variable"
    split_size = int(
        os.getenv("SPLIT_SIZE", None) or override_dict.get("split_size", -1)
    )
    sql_server_schema = os.getenv("SQL_SERVER_SCHEMA", None) or override_dict.get(
        "sql_server_schema", "dbo"
    )
    threads = int(os.getenv("THREADS", None) or override_dict.get("threads", -1))

    return Config(
        db_username=username,
        db_password=password,
        db_host=host,
        db_database=database,
        gcp_bucket=bucket,
        gcp_bq_dataset=dataset,
        gcp_target_project=target_gcp_project,
        db_table=table,
        split_size=split_size,
        sql_server_schema=sql_server_schema,
        threads=threads,
    )


def get_config(config_path: str) -> Config:
    if os.getenv("SECRETMANAGER_URI", None):
        logger.info("Reading config from SECRETMANAGER")
        from google.cloud import secretmanager

        name = os.getenv("SECRETMANAGER_URI", "")
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={"name": name})

        response_payload = response.payload.data.decode("UTF-8")
        p = yaml.load(response_payload, Loader=yaml.SafeLoader)
        return get_env_config(p)
    elif os.getenv("CONFIG_FILE", None):
        logger.info(f"Reading config from {os.getenv('CONFIG_FILE', None)} (set in env)")
        with open(os.getenv("CONFIG_FILE"), "r") as cfg:
            p = yaml.load(cfg, Loader=yaml.SafeLoader)
            return get_env_config(p)
    elif config_path is not None:
        logger.info(f"Reading config from {config_path} (program argument)")
        with open(config_path, "r") as cfg:
            p = yaml.load(cfg, Loader=yaml.SafeLoader)
            return get_env_config(p)
    else:
        logger.info(f"No config specified.")
        return get_env_config({})


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='A test program.')

    parser.add_argument("-c", "--config", help="path to yaml config file", type=str, default=None)

    args = parser.parse_args()
    config = get_config(config_path=args.config)

    logger.info(
        f"Connecting to {config.db_username}/{config.db_database}@{config.db_host} and syncing table: "
        f"{config.db_table} to {config.gcp_bucket}"
    )

    sql_server_to_csv = SqlServerToCsv(
        username=config.db_username,
        password=config.db_password,
        host=config.db_host,
        database=config.db_database,
        destination=f"gs://{config.gcp_bucket}/sqlserver/{config.gcp_bq_dataset}",
    )

    bigquery = SqlServerToBigquery(sql_server_to_csv=sql_server_to_csv)

    result = bigquery.ingest_table(
        threads=config.threads,
        sql_server_table=config.db_table,
        sql_server_schema=config.sql_server_schema,
        bigquery_destination_project=config.gcp_target_project,
        bigquery_destination_dataset=config.gcp_bq_dataset,
        split_size=config.split_size,
    )
    logger.info(result.full_str())
