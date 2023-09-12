This repo takes a list of wikipedia page URLs that contain tables and extracts ~~, transforms, and loads them to a PostgreSQL database.~~ and loads them to an S3 bucket as parquet files.

Extraction is performed in Python ~~and Transformation and Load stages are done via combination of Python and DuckDB SQL.~~ and transformations are applied in dbt.

Create resources:
```bash
# Create bucket
awslocal s3 mb s3://datalake

# List objects in bucket
awslocal s3api list-objects --bucket datalake

# List object names only
awslocal s3api list-objects-v2 --bucket datalake --query 'Contents[].Key' --output json
```

Query an S3 parquet file:
```python
import duckdb
from src.settings import date_parts
s3_uri = "http://localhost:4566"
s3_bucket = "datalake"
s3_key = f"ufc/extract/{date_parts}/rankings_1.parquet"
query = f"""
SELECT *
FROM read_parquet('{s3_uri}/{s3_bucket}/{s3_key}')
"""
# Run query
duckdb.sql(query)
```

Run a SQL query from a SQL file:
```python
import duckdb
from src.settings import sql_path, s3_uri, date_parts
from src.utils.sql import SQLUtils
from src.utils.functions import register_functions

duckdb_util = SQLUtils.from_duckdb_connection()
register_functions(duckdb_util)
# sql_file_name = "ufc/transform/current_champions_weight_classes_and_status.sql"
# or
sql_file_path = "src/sql/ufc/transform/debuted_fighters_featherweights.sql"
replace_dict = {
    "<s3_uri>": s3_uri, "<date_parts>": date_parts
}
data = duckdb_util.run_query(
    # sql_file_name=sql_file_name,
    sql_file_path=sql_file_path,
    replace_dict=replace_dict,
)
```

SQL directory (Before) (Now: SQL queries covered in dbt):
```
src/sql
├── namespace_1
│   └── clean
│         ├── clean1.sql
│         ├── clean2.sql
│         └── cleanN.sql
│   └── transform
│         ├── transform1.sql
│         ├── transform2.sql
│         └── transformN.sql
├── namespace_2
│   └── clean
│         ├── clean1.sql
│         ├── clean2.sql
│         └── cleanN.sql
│   └── transform
│         ├── transform1.sql
│         ├── transform2.sql
│         └── transformN.sql
└── ...
```