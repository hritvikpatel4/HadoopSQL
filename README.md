# HadoopSQL
An implementation of a SQL engine using Hadoop MapReduce

Shell- The shell.py script provides a command line interface for typing in the SQL queries.
It also handles some basic parsing and gives out some appropriate error messages. 

query_handler - Detailed parsing is done in the query_handler. It parses the query and calls
the appropriate functions which dynamically create the MapReducecode code and it also runs them
and produces the output.

MR_utils - This script contains some methods that is used for writing MapReduce code. The MapReduce code for select, project and aggregation functions exists here. It also has two other
methods for checking if a give database or a table exists.

## Instructions to run

The engine has been tested on Hadoop-3.2.0. It also needs a Python version 3.6+
To use the SQL engine, first start the Hadoop daemons using start-dfs.sh and start-yarn.sh.

Start the shell using `python3.7 shell.py` or `python3.6 shell.py`.

## Syntax for using the engine

### To create a new database 
`create database <database_name>`

### To load a table in the created database
`load <database_name/table_name.csv> as <column_name1: datatype, column_name2: datatype ...>`

### Queries that can be run on the table
`select <column_names (*)> from <database_name/table_name.csv> where <condition>`<br>
"where \<condition>" is optional in above query

`select count(<column_names>) | min(<column_names>) | max(<column_names>) from <database_name/table_name.csv> where <condition>`
