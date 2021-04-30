# Description
This repository contains a package to read from SQL server and store data in Google cloud storage and possibilities to
load the data to google BigQuery.

The package tries to be effective when reading from sql server and has basic cache capabilities.

# Usage example
You need to have the google default credentials set by for example setting GOOGLE_APPLICATION_CREDENTIALS to the path
of a valid service account.

```python
from database_to_bigquery.sql_server import SqlServerToCsv, SqlServerToBigquery
sql_server_to_csv = SqlServerToCsv(username="scott",
                                   password="t1ger",
                                   host="127.0.0.1//optionalinstance_name",
                                   database="thedb",
                                   destination=f"gs://gcsbucketname")

bigquery = SqlServerToBigquery(sql_server_to_csv=sql_server_to_csv)

result = bigquery.ingest_table(sql_server_table="table_to_read",
                               sql_server_schema="dbo",
                               bigquery_destination_project="bigqueryproject",
                               bigquery_destination_dataset="bigquerydataset")
print(result.full_str())
```

# Options
## Environment variables
- DB_PORT - override default sql server port
- DB_DRIVER - override default pyodbc driver (ODBC Driver 17 for SQL Server), for example with (FreeTDS)

This originally came packaged as we dockerfile for usage in k8s environment and I created a package for convinience, 
so please look at the github repo for updated description https://github.com/ael-computas/sqlserver-to-bigquery

## The split concept
The package tries to split the Sql server data into chunks if the table contains more than 1m rows.  You can override
this in the ingest_table function.

If the sql server tables contains columns that sql server CRC does not work on it will fail.

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
