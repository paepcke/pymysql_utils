pymysql_utils
=============

The pymysql_utils package makes interaction with MySQL from
Python more Pythonic than its underlying package MySQL-python.
Convenience methods for common MySQL operations, such as
managing tables, insertion, updates, and querying are also
available.

Quickstart:

from pymysql_utils import MySQLDB, DupKeyAction
db = MySQLDB(user='myName', db='myDb')
mySchema = {
  'col1' : 'INT',
  'col2' : 'varchar(255)',
  }

db.createTable('myTable', mySchema)

colNames  = ['col1','col2']
colValues = [(10, 'row1'),
             (20, 'row2'),
             (30, 'row3'),             
            ]

db.bulkInsert('myTable', colNames, colValues)

for result in db.query('SELECT * FROM myTable'):
    print(result)


