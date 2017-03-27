'''
Created on Sep 24, 2013
Updated on Mar 26, 2017

@author: paepcke
'''

# TODO: Test calling query() multiple times with several queries and get results alternately from the iterators
# TODO: In: cmd = 'INSERT INTO %s (%s) VALUES (%s)' % (str(tblName), ','.join(colNames), ','.join(map(str, colValues)))
#                 the map removes quotes from strings: ','join(map(str,('My poem', 10)) --> (My Poem, 10) 

# To run these tests, you need a local MySQL server,
# an account called 'unittest' on that server, and a
# database called 'unittest', also on that server.
#
# User unittest must be able to log in without a password, 
# the user only needs permissions for the unittest database.
# Here are the necessary MySQL commands:

#   CREATE USER unittest@localhost;
#   GRANT SELECT, INSERT, CREATE, DROP, ALTER, DELETE, UPDATE ON unittest.* TO unittest@localhost;

from collections import OrderedDict
import socket
import subprocess
import unittest

from pymysql_utils import MySQLDB, DupKeyAction
import pymysql_utils


#*******TEST_ALL = True
TEST_ALL = False

class TestPymysqlUtils(unittest.TestCase):
    '''
| GRANT USAGE ON *.* TO 'unittest'@'localhost'                                                        |
| GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER ON `unittest`.* TO 'unittest'@'localhost' |
+-----------------------------------------------------------------------------------------------------+
    '''
  
    @classmethod
    def setUpClass(cls):
      # Ensure that a user unittest with the proper
      # permissions exists in the db:
      TestPymysqlUtils.env_ok = True
      TestPymysqlUtils.err_msg = ''
      try:
          needed_grants = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER']
          mysqldb       = MySQLDB(host='localhost', port=3306, user='unittest', db='unittest')
          grant_query   = 'SHOW GRANTS FOR unittest@localhost'
          query_it      = mysqldb.query(grant_query)
          # First row of the SHOW GRANTS response should be:
          first_grant   = "GRANT USAGE ON *.* TO 'unittest'@'localhost'"
          # Second row depends on the order in which the 
          # grants were provided. The row will look something
          # like:
          #   GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER ON `unittest`.* TO 'unittest'@'localhost'
          # Verify:
          # The [0] pull the GRANT phrases out of their tuple:
          if query_it.next()[0] != first_grant:
              TestPymysqlUtils.err_msg = '''
                  User 'unittest' is missing USAGE grant needed to run the tests.
                  Also need: %s
                  ''' % str(needed_grants)
              TestPymysqlUtils.env_ok = False
              return
          grants_str = query_it.next()[0]
          for needed_grant in needed_grants:
              if grants_str.find(needed_grant) == -1:
                  TestPymysqlUtils.err_msg = '''
                    User 'unittest' does not have the '%s' permission needed to run the tests.
                    Need: %s.
                    ''' % (needed_grant, str(needed_grants))
                  TestPymysqlUtils.env_ok = False
                  return  
      except ValueError as e:
          TestPymysqlUtils.err_msg = '''
               For unit testing, localhost MySQL server must have 
               user 'unittest' without password, and a database 
               called 'unittest'. This user needs permissions:
               %s. 
               (%s)
               ''' % (needed_grants, `e`)
          TestPymysqlUtils.env_ok = False

    def setUp(self):
        if not TestPymysqlUtils.env_ok:
            raise RuntimeError(TestPymysqlUtils.err_msg)
        try:
            self.mysqldb = MySQLDB(host='localhost', port=3306, user='unittest', db='unittest')
        except ValueError as e:
            self.fail(str(e) + " (For unit testing, localhost MySQL server must have user 'unittest' without password, and a database called 'unittest')")


    def tearDown(self):
        self.mysqldb.dropTable('unittest')
        # Make sure the test didn't set a password
        # for user unittest in the db:
        self.mysqldb.execute("SET PASSWORD FOR unittest@localhost = '';")
        self.mysqldb.close()

    # ----------------------- Table Manilupation -------------------------

    #-------------------------
    # Creating and Dropping Tables 
    #--------------
    
    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testCreateAndDropTable(self):
        mySchema = {
          'col1' : 'INT',
          'col2' : 'varchar(255)',
          'col3' : 'FLOAT',
          'col4' : 'TEXT',
          'col5' : 'JSON'
          }
        self.mysqldb.createTable('myTbl', mySchema, temporary=False)
        tbl_desc = subprocess.check_output([self.mysqldb.mysql_loc, 
                                           '-u',
                                           'unittest',
                                           'unittest',
                                           '-e',
                                           'DESC myTbl;'])
        expected = 'Field\tType\tNull\tKey\tDefault\tExtra\n' +\
                   'col4\ttext\tYES\t\tNULL\t\n' +\
                   'col5\tjson\tYES\t\tNULL\t\n' +\
                   'col2\tvarchar(255)\tYES\t\tNULL\t\n' +\
                   'col3\tfloat\tYES\t\tNULL\t\n' +\
                   'col1\tint(11)\tYES\t\tNULL\t\n'
        self.assertEqual(tbl_desc, expected)
        
        # Query mysql information schema to check for table
        # present. Use raw cursor to test independently from
        # the pymysql_utils query() method:
        
        self.mysqldb.dropTable('myTbl')
        cursor = self.mysqldb.connection.cursor()
        tbl_exists_query = '''
                  SELECT table_name 
                    FROM information_schema.tables 
                   WHERE table_schema = 'unittest' 
                     AND table_name = 'myTbl';
                     '''
        cursor.execute(tbl_exists_query)
        self.assertEqual(cursor.rowcount, 0)
        cursor.close()

    #-------------------------
    # Table Truncation 
    #--------------
    
    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testTruncate(self):
      
        # Initial test db with known num of rows:
        rows_in_test_db = self.buildSmallDb()
        cursor = self.mysqldb.connection.cursor()
        cursor.execute('SELECT * FROM unittest;')
        self.assertEqual(cursor.rowcount, rows_in_test_db)
        
        self.mysqldb.truncateTable('unittest')
        
        cursor.execute('SELECT * FROM unittest;')
        self.assertEqual(cursor.rowcount, 0)
        cursor.close()

    # ----------------------- Insertion and Update -------------------------
    
    #-------------------------
    # Insert One Row 
    #--------------
    
    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testInsert(self):
        schema = OrderedDict([('col1','INT'), ('col2','TEXT')])
        self.mysqldb.createTable('unittest', schema)
        colnameValueDict = OrderedDict([('col1',10)])
        self.mysqldb.insert('unittest', colnameValueDict)
        self.assertEqual((10,None), self.mysqldb.query("SELECT * FROM unittest").next())
        #for value in self.mysqldb.query("SELECT * FROM unittest"):
        #    print value
 
    #-------------------------
    # Insert Several Columns 
    #--------------

    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testInsertSeveralColumns(self):
        schema = OrderedDict([('col1','INT'), ('col2','TEXT')])
        self.mysqldb.createTable('unittest', schema)
        colnameValueDict = OrderedDict([('col1',10), ('col2','My Poem')])
        self.mysqldb.insert('unittest', colnameValueDict)
        res = self.mysqldb.query("SELECT * FROM unittest").next()
        self.assertEqual((10,'My Poem'), res)
    

    #-------------------------
    # Bulk Insertion 
    #--------------
    
    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testBulkInsert(self):
        # Called twice: once by the unittest engine,
        # and again by testWithMySQLPassword() to 
        # exercise the pwd-bound branch in bulkInsert().
      
        # Build test db (this already tests basic bulkinsert):
        #                  col1   col2
        #                   10,  'col1'
        #                   20,  'col2'
        #                   30,  'col3'
        self.buildSmallDb()
        self.mysqldb.execute('ALTER TABLE unittest ADD PRIMARY KEY(col1)')
        
        # Provoke a MySQL error: duplicate primary key (i.e. 10): 
        # Add another row:  10,  'newCol1':
        colNames = ['col1','col2']
        colValues = [(10, 'newCol1')]
        
        warnings = self.mysqldb.bulkInsert('unittest', colNames, colValues)
        # Expect something like:
        #    ((u'Warning', 1062L, u"Duplicate entry '10' for key 'PRIMARY'"),) 
        self.assertEqual(len(warnings), 1)
            
        # First tuple should still be (10, 'col1'):
        self.assertTupleEqual(('col1',), self.mysqldb.query('SELECT col2 FROM unittest WHERE col1 = 10').next())
        
        # Try update again, but with replacement:
        warnings = self.mysqldb.bulkInsert('unittest', colNames, colValues, onDupKey=DupKeyAction.REPLACE)
        self.assertIsNone(warnings)
        # Now row should have changed:
        self.assertTupleEqual(('newCol1',), self.mysqldb.query('SELECT col2 FROM unittest WHERE col1 = 10').next())
        
        # Insert a row with duplicate key, specifying IGNORE:
        colNames = ['col1','col2']
        colValues = [(10, 'newCol2')]
        warnings = self.mysqldb.bulkInsert('unittest', colNames, colValues, onDupKey=DupKeyAction.IGNORE)
        # Even when ignoring dup keys, MySQL issues a warning
        # for each dup key:
        self.assertEqual(len(warnings), 1)
        self.assertTupleEqual(('newCol1',), self.mysqldb.query('SELECT col2 FROM unittest WHERE col1 = 10').next())
        
    #-------------------------
    # Updates 
    #--------------

    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testUpdate(self):
      
        num_rows = self.buildSmallDb()
        cursor = self.mysqldb.connection.cursor()
        
        # Initially, col2 of row0 must be 'col1':
        cursor.execute('SELECT col2 FROM unittest WHERE col1 = 10')
        col2_row_zero = cursor.fetchone()
        self.assertTupleEqual(col2_row_zero, ('col1',))
        
        self.mysqldb.update('unittest', 'col1', 40, fromCondition='col1 = 10')
        
        # Now no col1 with value 10 should exist:
        cursor.execute('SELECT col2 FROM unittest WHERE col1 = 10')
        self.assertEqual(cursor.rowcount, 0)
        # But a row with col1 == 40 should have col2 == 'col1':
        cursor.execute('SELECT col2 FROM unittest WHERE col1 = 40')
        col2_res = cursor.fetchone()
        self.assertTupleEqual(col2_res, ('col1',))
        
        # Update *all* rows in one column:
        self.mysqldb.update('unittest', 'col1', 0)
        cursor.execute('SELECT count(*) FROM unittest WHERE col1 = 0')
        res_count = cursor.fetchone()
        self.assertTupleEqual(res_count, (num_rows,))
    
    
    # ----------------------- Queries -------------------------         

    #-------------------------
    # Query With Result Iteration 
    #--------------
    
    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testQueryIterator(self):
        self.buildSmallDb()

        for rowNum, result in enumerate(self.mysqldb.query('SELECT col1,col2 FROM unittest')):
            if rowNum == 0:
                self.assertEqual((10,'col1'), result)
            elif rowNum == 1:
                self.assertEqual((20,'col2'), result)
            elif rowNum == 2:
                self.assertEqual((30,'col3'), result)

        # Test the dict cursor
        self.mysqldb.close()
        self.mysqldb = MySQLDB(host='localhost', 
                               user='unittest', 
                               db='unittest', 
                               cursor_class=pymysql_utils.Cursors.DICT)
        
        for result in self.mysqldb.query('SELECT col1,col2 FROM unittest'):
          
            self.assertIsInstance(result, dict)
            
            if result['col1'] == 10:
                self.assertEqual(result['col2'], 'col1')
            elif result['col1'] == 20:
                self.assertEqual(result['col2'], 'col2')
            elif result['col1'] == 30:
                self.assertEqual(result['col2'], 'col3')

    #-------------------------
    # Query Unparameterized 
    #--------------
    
    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testExecuteArbitraryQuery(self):
        self.buildSmallDb()
        self.mysqldb.execute("UPDATE unittest SET col1=120")
        for result in self.mysqldb.query('SELECT col1 FROM unittest'):
            self.assertEqual((120,), result)
        
    #-------------------------
    # Query Parameterized 
    #--------------
    
    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testExecuteArbitraryQueryParameterized(self):
        self.buildSmallDb()
        myVal = 130
        self.mysqldb.executeParameterized("UPDATE unittest SET col1=%s", (myVal,))
        for result in self.mysqldb.query('SELECT col1 FROM unittest'):
            self.assertEqual((130,), result)
        
    #-------------------------
    # Reading System Variables 
    #--------------

    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testReadSysVariable(self):
        this_host = socket.gethostname()
        # The [0] gets hostname out of its
        # tuple structure:
        mysql_hostname = self.mysqldb.query('SELECT @@hostname').next()[0]
        self.assertEqual(mysql_hostname, this_host)

    #-------------------------
    # User-Level Variables 
    #--------------
    
    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testUserVariables(self):

        pre_foo = self.mysqldb.query("SELECT @foo").next()
        self.assertEqual(pre_foo, (None,))
        
        self.mysqldb.execute("SET @foo = 'new value';")
        
        post_foo = self.mysqldb.query("SELECT @foo").next()
        self.assertEqual(post_foo, ('new value',))
        
        self.mysqldb.execute("SET @foo = 'NULL';")

    #-------------------------
    # testDbName 
    #--------------

    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testDbName(self):
        self.assertEqual(self.mysqldb.dbName(), 'unittest')
    
            
    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testWithMySQLPassword(self):
        
        try:
            # Set a password for the unittest user:
            self.mysqldb.execute("SET PASSWORD FOR unittest@localhost = 'foobar';")

            self.mysqldb.close()
            
            # We should be unable to log in without a pwd:
            with self.assertRaises(ValueError):
                self.mysqldb = MySQLDB(host='localhost', user='unittest', db='unittest')
                
            # Open new pymysql_db.MySQLDb instance, supplying pwd: 
            self.mysqldb = MySQLDB(host='localhost', user='unittest', passwd='foobar', db='unittest')
            # Do a test query:
            self.buildSmallDb()
            res = self.mysqldb.query("SELECT col2 FROM unittest WHERE col1 = 10;").next()
            self.assertEqual(res, ('col1',))
            
            # Bulk insert is also different for pwd vs. none:
            self.testBulkInsert()
        finally:
            # Make sure the remove the pwd from user unittest,
            # so that other tests will run successfully:
            self.mysqldb.execute("SET PASSWORD FOR unittest@localhost = '';")
            
    #-------------------------
    # testResultCount 
    #--------------
            
    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testResultCount(self):
        self.buildSmallDb()
        query_str = 'SELECT * FROM unittest'
        self.mysqldb.query(query_str)
        self.assertEqual(self.mysqldb.result_count(query_str), 3)
    
    
    #-------------------------
    # testInterleavedQueries
    #--------------
    
    #******@unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testInterleavedQueries(self):
        
        self.buildSmallDb()
        query_str1 = 'SELECT col2 FROM unittest ORDER BY col1'
        query_str2 = 'SELECT col2 FROM unittest WHERE col1 = 20 or col1 = 30 ORDER BY col1' 
        res_it1 = self.mysqldb.query(query_str1)
        res_it2 = self.mysqldb.query(query_str2)
        
        #self.assertEqual(first, second, msg)
        
        self.assertEqual(res_it1.next(), 'col1')
        self.assertEqual(res_it2.next(), 'col2')
        
        self.assertEqual(res_it1.next(), 'col2')
        self.assertEqual(res_it2.next(), 'col3')
        
        self.assertEqual(res_it1.next(), 'col3')
        with self.assertRaises(ValueError): 
            res_it2.next()
    
                          
    # ----------------------- UTILITIES -------------------------
    def buildSmallDb(self):
        '''
        Creates a two-col, three-row table in database
        unittest. The table is called 'unittest'.
        Returns number of rows created.
            col1       col2
            ----------------
             10       'col1'
             20       'col2'
             30       'col3'
        '''
        schema = OrderedDict([('col1','INT'),('col2','TEXT')])
        self.mysqldb.dropTable('unittest')
        self.mysqldb.createTable('unittest', schema)
        colNames = ['col1','col2']
        colValues = [(10, 'col1'),(20,'col2'),(30,'col3')]
        warnings = self.mysqldb.bulkInsert('unittest', colNames, colValues)
        self.assertIsNone(warnings)
        return 3

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testQuery']
    unittest.main()