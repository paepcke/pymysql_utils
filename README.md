# The pymysql_utils module

The pymysql_utils package makes interaction with MySQL from
Python more pythonic than its underlying package `mysqlclient`
(formerly MySQL-python), or the alternative underlying package
`pymysql`. Either mysqlclient, or pymysql may be chosen as the
foundation of pymysql_utils.

Convenience methods for common MySQL operations, such as
managing tables, insertion, updates, and querying are also
available. Query results are iterators with `next()` and
`nextall()` methods

Tested on:

|  OS                 | MySQL        | Python |
| ------------------- | ------------ | ------ |
| macos               |  mysql 8.0   |   3.7  |
| macos               |  mysql 8.0   |   2.7  |
| ubuntu 16.04 Xenial |  mysql 5.7   |   3.6  |
| ubuntu 16.04 Xenial |  mysql 5.7   |   2.7  |

## Quickstart

```python
from pymysql_utils.pymysql_utils import MySQLDB

# Create a database instance. For one approach to
# dealing with password-protected databases, see
# the Tips Section below.

db = MySQLDB(user='myName', db='myDb')
mySchema = {
  'col1' : 'INT',
  'col2' : 'varchar(255)',
  }

db.createTable('myTable', mySchema)

# Fill the table:
colNames  = ['col1','col2']
colValues = [(10, 'row1'),
             (20, 'row2'),
             (30, 'row3'),             
            ]

db.bulkInsert('myTable', colNames, colValues)

# Result objects are iterators:
for result in db.query('SELECT col2 FROM myTable ORDER BY col1'):
    print(result)

# row1
# row2
# row3


```
## A Bit More Detail

The database connection is encapsulated in an instance of
`MySQLDB`. This instance can maintain multiple queries
simulataneously. Each query result is an `iterator` object
from which result tuples can be retrieved one by one,
using `next()`, or all at once using `nextall()`. Here is
an example of multiple queries interleaved. Assume the
above table `myTable` is populated in the database.

```python

query_str1 = '''
             SELECT col2
               FROM myTable
              ORDER BY col1
             '''

query_str2 = '''
             SELECT col2
               FROM myTable
              WHERE col1 = 20
                 OR col1 = 30
           ORDER BY col1
           '''
results1   = db.query(query_str1)
results2   = db.query(query_str2)

# Result objects know their total result count:
results1.result_count()
# --> 3

results2.result_count()
# --> 2

# The db object can retrieve the result count
# by the query string:
db.result_count(query_str1)
# --> 3

results1.next()
# --> 'row1'

results2.next()
# --> 'row2'

results1.next()
# --> 'row2'
results2.next()
# --> 'row3'

results1.next()
# --> 'row3'

results2.next()
# --> raises StopIteration

results2.result_count()
# --> raises ValueError: query exhausted.

```
## Tips:

* Many methods return a two-tuple that includes a list of warnings, and a
  list of errors.
* Check the [in-code documentation](http://htmlpreview.github.com/?https://github.com/paepcke/pymysql_utils/blob/gh-pages/docs/pymysql_utils.m.html) 
  for all available methods.
* A number of frequent SQL operations can conveniently be accomplised
  via dedicated methods: `close`, `createTable`, `dropTable`, `insert`,
  `bulkInsert`, `truncateTable`, and `update`.

  These, or other operations can also be accomplished by using
  `execute()` to submit arbitrary SQL
* A useful idiom for queries known to return a single result,
  such as a count:

    `db.query('...').next()`
* The underlying `mysqlclient` package does not expose the MySQL 5.7+
  *login-path* option. So the `MySQLDB()` call needs to include the
  password if one is required. One way to avoid putting passwords into
  your code is to place the password into a file in a well protected
  directory, such as `~/.ssh/mysql`. Then read the password from there.
  
## Installation


```bash
# Possibly in a virtual environment:

pip install pymysql_utils
python setup.py install

# Testing requires a bit of prep in the local MySQL:
# a database 'unittest' must be created, and a user
# 'unittest' without password must have permissions:
#
#
# CREATE DATABASE unittest;   
# CREATE USER unittest@localhost;
# GRANT SELECT, INSERT, UPDATE, DELETE,
#       CREATE, DROP, ALTER
#    ON `unittest`.* TO 'unittest'@'localhost';

# The unittests give these instructions as well.

# python setup.py test
```
## Selecting Python-only or C-Python

By default pymysql_utils uses `mysqlclient`, and therefore a C-based
API to MySQL servers. Occasionally it may be desirable to use a
Python only solution. You may force pymysql_utils to use the
`pymysql` library instead of `mysqlclient`.

One reason for forcing Python only is a known incompatibility between
openssl 1.1.1[a,b,c] and mysqlclient (as of Jul 29, 2017).

To have pymysql_utils use the Python-only pymysql library, do this:
1. Copy `pymysql_utils/pymysql_utils_SAMPLE.cnf` to
`pymysql_utils/pymysql_utils.cnf` 
2. Inside this new config file, change
```bash

       FORCE_PYTHON_NATIVE = False
   to
       FORCE_PYTHON_NATIVE = True
```

