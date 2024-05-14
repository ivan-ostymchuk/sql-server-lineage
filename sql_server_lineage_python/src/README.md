# sql-server-lineage

I worked many times on sql server data warehouses or or sql server application databases with almost   
no documentation and lots of stored procedures transforming data. At some point I wanted a tool that would   
analyse all the stored procedures and generate a lineage that I would be able to send to a Data Catalog or   
visualise immediately to get an idea of the structure and debug much more easily. So I decided to develop it.

This is a library available in Golang and Python that would enable you to do 2 things:
- Get the lineage of all the stored procedures in a database as python dictionary.
- Generate an html file with the lineage visualised (That you can share on Google Pages or Github Pages or wherever you like).

CTEs (deeply nested as well), Temp Tables, Table Variables are all handled gracefully. You'll always see the original source tables.

The resulting lineage structure is the following:
    sink_table -> stored_procedure (1 or more) -> list_of_sources

Dictionary Example:
```
{
    'db_name.schema.table_sink': {
        'schema.stored_procedure_1': [
            'schema.table_source_1',
            'schema.table_source_2',
            'schema.table_source_3',
        ],
        'schema.stored_procedure_2': [
            'schema.table_source_a',
            'schema.table_source_b',
            'schema.table_source_c',
        ],
    },
    'db_name.schema.table_sink_2': {
        'schema.stored_procedure_3': [
            'schema.table_source',
        ],
    },
}
```

Html Example:

![Sample Image](https://github.com/ivan-ostymchuk/sql-server-lineage/blob/main/lineage_example.png)

Therefore, everything is centered around the sink_table. Because most likely you want to see for each table where does the data come from. If you need it different (for a Data Catalog) you can transform the objects and adapt them to your requirement.
In the html you will see the sink table as reference in a different color (green) and you will see it again in the lineage.

I decided to generate html files instead of starting a local server (like DBT does) because I wanted to keep it simple and make it easy to host somewhere as a static website and therefore sharing it with other Data Engineers, Data Analysts, etc.

This project is named sql-server-lineage because I want to keep it specialised only on Sql Server. To generate the lineage you need to develop a custom implementation of a Sql parser. I did it for Sql Server as I've been using it recently but I do not intend to develop parsers for other sql dialects.

# DISCLAIMER
Since this library analyses stored procedures, if you transform data externally and then write it to the database there is nothing you can do for the lineage. Also if the stored procedures rely heavily on dynamic sql especially with table names passed as parameters then the library would not be able to determine the table names for the lineage.

# Implementation

All the core logic is developed in Go, then using CGO, C bindings and some adapter functions I made it available also as a Python library.
The main reason to do that was because Data Engineers work mainly in Python.

# Get Started

### Installation
```
pip install sql-server-lineage
```

### Example to generate the lineage directly from sql server
In this example we get the stored procedures definitions from sql server
but you can also read the definitions from files.

```
from sql_server_lineage import get_lineage, generate_html_lineage
from typing import Dict, List
import pyodbc

host: str = "host"
port: str = "port"
driver: str = "driver"
user: str = "user"
password: str = "password"
database: str = "database"
otherparams: str = "otherparams"

db_conn_string: str = f"DRIVER={{{driver}}};SERVER={host};PORT={port};DATABASE={database};UID={user};PWD={password};{otherparams}"
conn: pyodbc.Connection = pyodbc.connect(db_conn_string)

sql_query: str = """
    SELECT OBJECT_DEFINITION(object_id) as sp_definition
    FROM sys.procedures
    WHERE object_id not in (select major_id from sys.extended_properties);
"""

cursor = conn.cursor()
cursor.execute(sql_query)
records: List = cursor.fetchall()
stored_procedures: List[str] = [r.sp_definition for r in records]

# get lineage and then send it to a Data Catalog, or transform and then send.
result_lineage: Dict = get_lineage(stored_procedures)

# generate the html representation of the lineage
# providing the filename is optional
lineage_file: str = "lineage_generated.html"
generate_html_lineage(stored_procedures, lineage_file)
```

This project operates under the MIT License.