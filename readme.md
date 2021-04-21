# Description
Example demonstrating *effective and scalable* read from SQL Server using *cached results* while not using CDC.

The program will attempt to convert sqlserver types to bigquery types.  Note that some types are unsupported
and will be ignored, like LOBs, for example.  You might need to update the program with custom converters
if you have a custom type.

The program can be run standalone or in a kubernetes cluster (or airflow), for example.  It even works well with 
serverless, since it has a low memory footprint and dont use local disk.

The program will attempt to read from SQL Server and do the following:

- If the table rows are > 1m rows, it will partition the table and sort it by PK
- It generates an aggregated result first, containing sum,min,max of pk fields.
- The entire result per parition gets a crc on the entire set.
- It will then check GCS if it already has the partitioned cached by comparing the last runs aggregate vc this one.
  - to do this it simply compares the content of the aggregate with the GCS stored one.
  - if a partition is the same, it is skipped.
  - NOTE: There is a slight chance that the CRC generated is not unique, in those cases you can provide additional date fields, so that you can compare max/min dates of updated_field, for example.
- Each partition is stored in GCS as CSV format.
- The files will be loaded to BigQuery for the following conditions
  - if there was at least a partition read this run
  - or if the rows in bq does not match the rows in SQL server.

## Configuration Options
- DB_USERNAME - sqlserver username
- DB_PASSWORD - sqlsever pw
- DB_HOST - sqlserver host
- DB_DATABASE - database
- GCS_BUCKET - tmp storage for data.  contains crc and schema as well
- BQ_DATASET - bq dataset to load data into
- DB_TABLE - Source table to read.  Also destination table name
- SPLIT_SIZE - defaults to -1 (dynamic, attempts to split in 20 chunks if > 1m rows)
- SQL_SERVER_SCHEMA - defaults to dbo if not set
- CONFIG_FILE - if this is set, try to read yaml file from that location.
- SECRETMANAGER_URI - if set, try to load config file from secret manager.

## Running the program
You can read from yaml config or env variables, or both.
A common scenario, can be to have db credentials in yaml file (mounted as secret, or a secret store) and have for example
table from environment.

### example command line
Example.yaml

````
---
  db_username: the_username
  db_password: the_password
  db_host: 127.0.0.1
  db_database: the_database
````

Env variables

````
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json
export GCS_BUCKET=some_google_cloud_bucket
export BQ_DATASET=yoursource_raw (you need to create this before running)
export DB_TABLE=thetable
export CONFIG_FILE=/path/to/Example.yaml
````

Then simply load main.py.

## The split concept
Lets take a simple table as example

| ID  (PK)      | data          |
| ------------- |:-------------:|
| 1             | test1         |
| 2             | test2         |
| 3             | test3         |
| 4             | test4         |

Let us use a split size of 2, meaning 2 rows in every split.
The PK always has a predictable sort, so we will sort by PK, even if it might not be the most effective in all cases

The split here will be

Split 1

| ID  (PK)      | data          |
| ------------- |:-------------:|
| 1             | test1         |
| 2             | test2         |

Split 2

| ID  (PK)      | data          |
| ------------- |:-------------:|
| 3             | test3         |
| 4             | test4         |

To know how to split it this way, we will first do a

````
WITH splits AS (SELECT (ROW_NUMBER() OVER(ORDER BY ID) / 2 +1 as internal_split, ALL_FIELDS, from table)
select 2 AS split_size
internal_split
count(*) as cnt
min(ID) as min_id, max(id) as max_id
CHECKSUM_AGG(CHECKSUM(*)) as crc 
from splits group by internal_split
 ````

Will produce the following

| split_size    | internal_split|     cnt       |      min_id   |    max_id     |      crc      |
| ------------- |:-------------:|:-------------:|:-------------:|:-------------:|:-------------:|
| 2             | 1             | 2             |      1        |      2        | 1234567       |
| 2             | 2             | 2             |      3        |      4        | -23456789     |

Note: This is not from an actual db, so CRC is faked.  mostly to show concept.
The Entire ROW will act as a crc in GCS.  

From this result its quite easy to select the data belonging to a split.

````
select WITH splits AS (SELECT (ROW_NUMBER() OVER(ORDER BY ID) / 2 +1 as internal_split, ALL_FIELDS from table)
SELECT ALL_FIELDS... from splits where internal_split=1
 ````

Then the payload can be streamed to a file.  The CRC is stored to GCS.

What happens if we now update the database to this?

| ID  (PK)      | data          |
| ------------- |:-------------:|
| 1             | test1         |
| 2             | test2         |
| 3             | test3         |
| 4             | THIS_CHANGED  |

Row number 2 in the aggregate will change..

| split_size    | internal_split|     cnt       |      min_id   |    max_id     |      crc      |
| ------------- |:-------------:|:-------------:|:-------------:|:-------------:|:-------------:|
| 2             | 1             | 2             |      1        |      2        | 1234567       |
| 2             | 2             | 2             |      3        |      4        | 1243535341434 |

And we can now just read internal_split=2, since internal_split=1 did not change.

## FAQ

### What about CDC?
Change data capture is better and more scalable.  Look into debezium, for instance, to get you started.

This program can work for a while, but a true CDC solution is better.

### Scalability
This has only been tested on small databases with 100m rows or so in the biggest tables.  Im not sure how this solution
copes with bigger datasets.

### What happens if the data changes between split group by and while reading data?

The data you read might be inconsistent, in regards to PK/FK and CRC with actual data. However, the next time you run 
the prgram, it will recognize that the crc no longer matches, and it will rewrite the payloads

### What happens if CRC is same for some datasets?

This can happen. Please see SQL server docs.

This process can handle this if you have an updated_date field, for example, then it will use that as additional min/max fields
to mitigate this rare issue.

### Some SQL Server types are unsupported
Yes, this is built solving real customer scenarios, so I guess that case was never encountered.  fork repo and do a pull request!
