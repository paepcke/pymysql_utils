The pymysql_utils module
========================

The pymysql_utils package makes interaction with MySQL from
Python more pythonic than its underlying package mysqlclient
(formerly MySQL-python).

Convenience methods for common MySQL operations, such as
managing tables, insertion, updates, and querying are also
available. Query results are iterators with `next()` and
`nextall()` methods

Tested on:



|  OS                 | MySQL        | Python |
| ------------------- |:------------: ------: |
| macos               |  mysql 8.0   |   3.7  |
| macos               |  mysql 8.0   |   2.7  | 
| ubuntu 16.04 Xenial |  mysql 5.7   |   3.6  |
| ubuntu 16.04 Xenial |  mysql 5.7   |   2.7  |


Quickstart
----------

```python
from pymysql_utils.pymysql_utils import MySQLDB

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
A Bit More Detail
-----------------

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
Installation
------------

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

