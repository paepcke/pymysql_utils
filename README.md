pymysql_utils
=============

The pymysql_utils package makes interaction with MySQL from
Python more pythonic than its underlying package MySQL-python.
Convenience methods for common MySQL operations, such as
managing tables, insertion, updates, and querying are also
available. Query results are iterators with `next()` and
`nextall()` methods

##Quickstart:

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

<need output>


```
##A Bit More Detail

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
               FROM unittest
              ORDER BY col1
             '''

query_str2 = '''
             SELECT col2
               FROM unittest
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
# --> 'col1'

results2.next()
# --> 'col2'

results1.next()
# --> 'col2'
results2.next()
# --> 'col3'

results1.next()
# --> 'col3'

results2.next()
# --> raises StopIteration

results2.result_count()
# --> raises ValueError: query exhausted.


##Installation

```bash
# Possibly in a virtual environment:

pip install pymysql_utils
nosetests pymysql_utils
...............
----------------------------------------------------------------------
Ran 15 tests in 0.604s

OK
```
For the unit tests to run you need a MySQL database
on your machine with the following setup:

```mysql
CREATE DATABASE unittest;   
CREATE USER unittest@localhost;
GRANT SELECT, INSERT, UPDATE, DELETE,
      CREATE, DROP, ALTER
   ON `unittest`.* TO 'unittest'@'localhost';
```