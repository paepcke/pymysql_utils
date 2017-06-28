'''
Created on Sep 24, 2013

@author: paepcke


Modifications:
  - Dec 30, 2013: Added closing of connection to close() method
  - Mar 26, 2017: Major overhaul; fixed bulk insert.
  - Jun 27, 2017: Changed method that had undefined returns, such
                  as bulkInsert() to return a 2-tuple: (errors, warnings).
                  Both are lists, or None. Error-free execution returns
                  (None, None).

For usage details, see `Github README <https://github.com/paepcke/pymysql_utils>`_.
This module is designed for MySQL 5.6 and 5.7. 
 
'''

from contextlib import contextmanager
import csv
import os
import re
import socket
import subprocess
import tempfile
from warnings import filterwarnings, resetwarnings

import MySQLdb
from MySQLdb import Warning as db_warning
from MySQLdb.cursors import DictCursor as DictCursor
from MySQLdb.cursors import SSCursor as SSCursor
from MySQLdb.cursors import SSDictCursor as SSDictCursor

from _mysql_exceptions import ProgrammingError, OperationalError


class DupKeyAction:
    PREVENT = 0
    IGNORE  = 1
    REPLACE = 2

# Cursor classes as per: 
#    http://mysql-python.sourceforge.net/MySQLdb-1.2.2/public/MySQLdb.cursors.BaseCursor-class.html
# Used to pass to query() method if desired:   

class Cursors:
    BASIC   			  = None
    DICT     			  = DictCursor
    SS_CURSOR       = SSCursor
    SS_DICT_CURSOR  = SSDictCursor
                      
class MySQLDB(object):
    '''
    Shallow interface to MySQL databases. Some niceties nonetheless.
      - The query() method is an iterator. So::
            for result in db.query('SELECT * FROM foo'):
               print result
      - Table manipulations (create/drop/...)
      - Insertion/update
    '''

    NON_ASCII_CHARS_PATTERN = re.compile(r'[^\x00-\x7F]+')

    # ----------------------- Top-Level Housekeeping -------------------------

    #-------------------------
    # Constructor
    #--------------

    def __init__(self, 
                 host='127.0.0.1', 
                 port=3306, 
                 user='root', 
                 passwd='', 
                 db='mysql',
                 cursor_class=None):
        '''
        Creates connection to the underlying MySQL database.
        This connection is maintained until method close()
        is called. The optional cursor_class controls the
        format in which query results are returned. The 
        Values must be one of Cursors.DICT, Cursors.SS_CURSOR,
        etc. See definition of class Cursors above. 
        
        :param host: MySQL host
        :type host: string
        :param port: MySQL host's port
        :type port: int
        :param user: user to log in as
        :type user: string
        :param passwd: password to use for given user
        :type passwd: string
        :param db: database to connect to within server
        :type db: string
        :param cursor_class: choice of how rows are returned
        :type cursor_class: Cursors
    
        '''
        
        # If all arguments are set to None, we are unittesting:
        if all(arg is None for arg in (host,port,user,passwd,db)):
            return
        
        if cursor_class is not None:
            # Ensure we caller passed a valid cursor class:
            if cursor_class not in [DictCursor, SSCursor, SSDictCursor]:
                raise ValueError("Non-existing cursor class '%s'" % str(cursor_class))
        
        self.user = user
        self.pwd  = passwd
        self.db   = db
        self.name = db
        # Will hold querySt->cursor for two purposes:
        # Ability to retrieve number of results in SELECT,
        # Ensure that all cursors are closed (see query()):
        self.cursors = {}
        self.most_recent_query = None
        
        # Find location of mysql client program:
        try:
            # The following doesn't work when running in Eclipse (gives usage for 'which'),
            # because it is running in the system env, not the bash env which
            # its usually enriched PATH var.
            self.mysql_loc = subprocess.check_output(['which', 'mysql']).strip()
        except subprocess.CalledProcessError as e:
            # Last resort: try /usr/local/bin/mysql:
            if os.path.exists('/usr/local/bin/mysql'):
                self.mysql_loc = '/usr/local/bin/mysql'
            else:
                raise RuntimeError("MySQL client not found on this machine (%s)" % socket.gethostname())

        try:
            if cursor_class is None:
                self.connection = MySQLdb.connect(host=host,
                                                  port=port, 
                                                  user=user, 
                                                  passwd=passwd, 
                                                  db=db,
                                                  charset='utf8')
            else:
                self.connection = MySQLdb.connect(host=host, 
                                                  port=port, 
                                                  user=user, 
                                                  passwd=passwd, 
                                                  db=db,
                                                  charset='utf8',
                                                  cursorclass=cursor_class)
        
        except OperationalError as e:
            pwd = '...............' if len(passwd) > 0 else '<no password>'
            raise ValueError('Cannot reach MySQL server with host:%s, port:%s, user:%s, pwd:%s, db:%s (%s)' %
                             (host, port, user, pwd, db, `e`))

    #-------------------------
    # dbName 
    #--------------
        
    def dbName(self):
        '''
        Return name of database to which this MySQLDB is connected.
        
        :return: name of database within MySQL server to which MySQLDB instance is connected.
        :rtype: String
        '''
        return self.db
    
    #-------------------------
    # close 
    #--------------

    def close(self):
        '''
        Close all cursors that are currently still open.
        '''
        for cursor in self.cursors.values():
            try:
                cursor.close()
            except:
                pass
        try:
            self.connection.close()
        except:
            pass

    # ----------------------- Table Management -------------------------

    #-------------------------
    # createTable
    #--------------
    
    def createTable(self, tableName, schema, temporary=False):
        '''
        Create new table, given its name, and schema.
        The schema is a dict mappingt column names to 
        column types. Example: {'col1' : 'INT', 'col2' : 'TEXT'}

        :param tableName: name of new table
        :type tableName: String
        :param schema: dictionary mapping column names to column types
        :type schema: Dict<String,String>
        :raise: ValueError: if table to truncate does not exist or permissions issues.        
        '''
        colSpec = ''
        for colName, colVal in schema.items():
            colSpec += str(colName) + ' ' + str(colVal) + ','
        cmd = 'CREATE %s TABLE IF NOT EXISTS %s (%s) ' % (
            'TEMPORARY' if temporary else '',
            tableName, 
            colSpec[:-1]
            )
        cursor = self.connection.cursor()
        try:
            cursor.execute(cmd)
            self.connection.commit()
        finally:
            cursor.close()

    #-------------------------
    # dropTable
    #--------------

    def dropTable(self, tableName):
        '''
        Delete table safely. No errors

        :param tableName: name of table
        :type tableName: String
        :raise: ValueError: if table to drop does not exist or permissions issues.        
        '''
        cursor = self.connection.cursor()
        try:
            # Suppress warning about table not existing:
            with no_warn_no_table():
                cursor.execute('DROP TABLE IF EXISTS %s' % tableName)
            self.connection.commit()
        except OperationalError as e:
            raise ValueError("In pymysql_utils dropTable(): %s" % `e`)
        except ProgrammingError as e:
            raise ValueError("In pymysql_utils dropTable(): %s" % `e`)
        finally:
            cursor.close()

    #-------------------------
    # truncateTable 
    #--------------

    def truncateTable(self, tableName):
        '''
        Delete all table rows. No errors

        :param tableName: name of table
        :type tableName: String
        :raise: ValueError: if table to truncate does not exist or permissions issues.
        '''
        cursor = self.connection.cursor()
        try:
            try:
                cursor.execute('TRUNCATE TABLE %s' % tableName)
            except OperationalError as e:
                raise ValueError("In pymysql_utils truncateTable(): %s." % `e`)
            except ProgrammingError as e:
                raise ValueError("In pymysql_utils truncateTable(): %s." % `e`)
            self.connection.commit()
        finally:
            cursor.close()

    # ----------------------- Insertion and Updates -------------------------

    #-------------------------
    # insert 
    #--------------

    def insert(self, tblName, colnameValueDict):
        '''
        Given a dictionary mapping column names to column values,
        insert the data into a specified table

        :param tblName: name of table to insert into
        :type tblName: String
        :param colnameValueDict: mapping of column name to column value
        :type colnameValueDict: Dict<String,Any>
        :return (None,None) if all ok, else tuple (errorList, warningsList)
        '''
        colNames, colValues = zip(*colnameValueDict.items())
        cursor = self.connection.cursor()
        try:
            wellTypedColValues = self._ensureSQLTyping(colValues)
            cmd = 'INSERT INTO %s (%s) VALUES (%s)' % (str(tblName), ','.join(colNames), wellTypedColValues)
            with no_db_warnings():
                try:
                    cursor.execute(cmd)
                except Exception:
                    # The following show_warnings() will
                    # reveal the error:
                    pass
            mysql_warnings = self.connection.show_warnings()
            if len(mysql_warnings) > 0:
                warnings   = [warning_tuple for warning_tuple in mysql_warnings if warning_tuple[0] == 'Warning']
                errors     = [error_tuple for error_tuple in mysql_warnings if error_tuple[0] == 'Error']
        finally:
            self.connection.commit()
            cursor.close()
            return (None,None) if len(mysql_warnings) == 0 else (errors, warnings)            
    
    #-------------------------
    # bulkInsert 
    #--------------
    
    def bulkInsert(self, tblName, colNameTuple, valueTupleArray, onDupKey=DupKeyAction.PREVENT):
        '''
        Inserts large number of rows into given table. The
        rows must be provided as an array of tuples. Caller's
        choice whether duplicate keys should cause a warning,
        omitting the incoming row, omit the incoming row without
        a warning, or replace the existing row.
        
        Returns None if all went well, else a list of error
        tuples.
        
        Strategy: 
        
        write the values to a temp file, then generate a 
        ``LOAD LOCAL INFILE...`` MySQL command and execute it in MySQL. 
        Returns `None` if no errors/warnings, else returns the tuple
        of tuples with the warnings.
                
        :param tblName: table into which to insert
        :type  tblName: string
        :param colNameTuple: tuple containing column names in proper order, i.e. 
               corresponding to valueTupleArray orders.
        :type  colNameTuple: (str[,str[...]])
        :param valueTupleArray: array of n-tuples, which hold the values. Order of
               values must correspond to order of column names in colNameTuple.
        :type  valueTupleArray: `[(<MySQLVal> [,<MySQLval>,...]])`
        :param onDupKey: determines action when incoming row would duplicate an existing row's
        	   unique key. If set to DupKeyAction.IGNORE, then the incoming tuple is
        	   skipped. If set to DupKeyAction.REPLACE, then the incoming tuple replaces
        	   the existing tuple. By default, each attempt to insert a duplicate key
        	   generates a warning.
        :type  onDupKey: DupKeyAction
        :return: None is no warnings occurred, else 
            a tuple with (errorList, warningsList).
        		Warning reflects MySQL's output of "show warnings;"
        		Example::
        		((u'Warning', 1062L, u"Duplicate entry '10' for key 'PRIMARY'"),)
        				    
        :rtype: {None | ((str),(str))}
        :raise: ValueError if bad parameter.
        
        '''
        tmpCSVFile = tempfile.NamedTemporaryFile(dir='/tmp',prefix='bulkInsert',suffix='.csv')
        # Allow MySQL to read this tmp file:
        os.chmod(tmpCSVFile.name, 0644)
        self.csvWriter = csv.writer(tmpCSVFile, 
                                    dialect='excel-tab', 
                                    lineterminator='\n', 
                                    delimiter=',', 
                                    quotechar='"', 
                                    quoting=csv.QUOTE_MINIMAL)
        # Can't use csvWriter.writerows() b/c some rows have 
        # weird chars: self.csvWriter.writerows(valueTupleArray)        
        for row in valueTupleArray:
            # Convert each element in row to a string,
            # including mixed-in Unicode Strings:
            self.csvWriter.writerow([rowElement for rowElement in self._stringifyList(row)])
        tmpCSVFile.flush()
        
        # Create the MySQL column name list needed in the LOAD INFILE below.
        # We need '(colName1,colName2,...)':
        if len(colNameTuple) == 0:
            colSpec = '()'
        else:
            colSpec = '(' + colNameTuple[0]
            for colName in colNameTuple[1:]:
                colSpec += ',' + colName
            colSpec += ')'

        # For warnings from MySQL:
        mysql_warnings = ''
        try:
            if onDupKey == DupKeyAction.PREVENT:
                dupAction = '' # trigger the MySQL default
            elif onDupKey == DupKeyAction.IGNORE:
                dupAction = 'IGNORE'
            elif onDupKey == DupKeyAction.REPLACE:
                dupAction = 'REPLACE'
            else:
                raise ValueError("Parameter onDupKey to bulkInsert method must be of type DupKeyAction; is %s" % str(onDupKey))
            
            # Remove quotes from the values inside the colNameTuple's:
            mySQLCmd = ("LOAD DATA LOCAL INFILE '%s' %s INTO TABLE %s FIELDS TERMINATED BY ',' " +\
                        "OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' %s"
                        ) %  (tmpCSVFile.name, dupAction, tblName, colSpec)
            cursor = self.connection.cursor()
            #with no_warn_dup_key():
            with no_db_warnings():
                try:
                    cursor.execute(mySQLCmd)
                except Exception:
                    # The following show_warnings() will
                    # reveal the error:
                    pass
            mysql_warnings = self.connection.show_warnings()
            if len(mysql_warnings) > 0:
                warnings   = [warning_tuple for warning_tuple in mysql_warnings if warning_tuple[0] == 'Warning']
                errors     = [error_tuple for error_tuple in mysql_warnings if error_tuple[0] == 'Error']
                if len(warnings) == 0:
                    warnings = None
                if len(errors) == 0:
                    errors = None
                
        finally:
            tmpCSVFile.close()
            self.execute('commit;')
            return (None,None) if len(mysql_warnings) == 0 else (errors, warnings)            
    
    #-------------------------
    # update 
    #--------------
    
    def update(self, tblName, colName, newVal, fromCondition=None):
        '''
        Update one column with a new value.

        :param tblName: name of table in which update is to occur
        :type tblName: String
        :param colName: column whose value is to be changed
        :type colName: String
        :param newVal: value acceptable to MySQL for the given column 
        :type newVal: type acceptable to MySQL for the given column 
        :param fromCondition: optionally condition that selects which rows to update.
                      if None, the named column in all rows are updated to
                      the given value. Syntax must conform to what may be in
                      a MySQL FROM clause (don't include the 'FROM' keyword)
        :type fromCondition: String
        :return (None,None) if all ok, else tuple (errorList, warningsList)
        '''
        cursor = self.connection.cursor()
        try:
            if fromCondition is None:
                cmd = "UPDATE %s SET %s = '%s';" % (tblName,colName,newVal)
            else:
                cmd = "UPDATE %s SET %s = '%s' WHERE %s;" % (tblName,colName,newVal,fromCondition)
            with no_db_warnings():
                try:
                    cursor.execute(cmd)
                except Exception:
                    # The following show_warnings() will
                    # reveal the error:
                    pass
            mysql_warnings = self.connection.show_warnings()
            if len(mysql_warnings) > 0:
                warnings   = [warning_tuple for warning_tuple in mysql_warnings if warning_tuple[0] == 'Warning']
                errors     = [error_tuple for error_tuple in mysql_warnings if error_tuple[0] == 'Error']

        finally:
            self.connection.commit()            
            cursor.close()
            return (None,None) if len(mysql_warnings) == 0 else (errors, warnings)            

    # ----------------------- Queries -------------------------
        
    #-------------------------
    # query 
    #--------------
    
    def query(self, queryStr):
        '''
        Query iterator. Given a query, return one result for each
        subsequent call. When all results have been retrieved,
        the iterator returns None.
        
        IMPORTANT: what is returned is an *iterator*. So you
        need to call next() to get the first result, even if
        there is only one.

        :param queryStr: the query to submit to MySQL
        :type queryStr: String
        :return: iterator of query results
        :rtype: iterator of tuple
        '''

        queryStr = queryStr.encode('UTF-8')
        
        # For if caller never exhausts the results by repeated calls,
        # and to find cursor by query to get (e.g.) num results:
        cursor = self.connection.cursor()
        
        # Ability to find an active cursor by query 
        # via result_count(queryString)...
        self.cursors[queryStr] = cursor
        
        #... or find it via  result_count()...
        self.most_recent_query = queryStr

        try:        
            cursor.execute(queryStr)
        except ProgrammingError as e:
            raise ValueError(`e`)
        return QueryResult(cursor, queryStr, self)
        
    #-------------------------
    # result_count 
    #--------------

    def result_count(self, queryStr=None):
        '''
        Given a query string, after that string served
        as query in a call to query(), and before the result iterator
        has been exhausted: return number of SELECT results.
        
        If queryStr is left to None, the value of 
        self.most_recent_query is used.
        
        :param queryStr: query that was used in a prior call to query()
        :type queryStr: {None | string}
        :return: number of records in SELECT result.
        :rtype: int
        :raise: ValueError if no prior query is still active.
        '''
        
        try:
            if queryStr is None:
                if self.most_recent_query is not None:
                    return self.cursors[self.most_recent_query].rowcount
                else:
                    raise(ValueError("No previous query available."))
            else:
                return self.cursors[queryStr].rowcount
        except KeyError:
            raise ValueError("Query '%s' is no longer active." % queryStr)

    
    #-------------------------
    # execute 
    #--------------
            
    def execute(self,query, doCommit=True):                                                                                   
        '''
        Execute an arbitrary query, including
        MySQL directives. Return value is undefined.
        For SELECT queries or others that return
        results from MySQL, use the query() method. 
        
        Note: For some funky directives you get the MySQL error:
        
            Commands out of sync; you can't run this command now 
            
        For those, turn doCommit to False;
        
        :param query: query or directive
        :type query: String
        :return: (None,None) if all went well, else a tuple:
            (listOrErrors, listOfWarnings)
        '''
        
        cursor=self.connection.cursor()                                                                        
        try:
            with no_db_warnings():
                try:                                                                                                   
                    cursor.execute(query)
                except Exception:
                    # The following show_warnings() will
                    # reveal the error:
                    pass
            mysql_warnings = self.connection.show_warnings()
            if len(mysql_warnings) > 0:
                warnings   = [warning_tuple for warning_tuple in mysql_warnings if warning_tuple[0] == 'Warning']
                errors     = [error_tuple for error_tuple in mysql_warnings if error_tuple[0] == 'Error']
        finally:                                                                                               
            if doCommit:                                                                              
                self.connection.commit()                                                                           
            cursor.close()                                                                                     
            return (None,None) if len(mysql_warnings) == 0 else (errors, warnings)

    #-------------------------
    # executeParameterized
    #--------------
    
    def executeParameterized(self,query,params):                                                                      
        '''
        Executes arbitrary query that is parameterized
        as in the Python string format statement. Ex:
        executeParameterized('SELECT %s FROM myTable', ('col1', 'col3'))
        
        Return value is undefined. For SELECT queries or others that return
        results from MySQL, use the query() method.        

        Note: params must be a tuple. Example: to update a column:: 
        mysqldb.executeParameterized("UPDATE myTable SET col1=%s", (myVal,))
        the comma after 'myVal' is mandatory; it indicates that the 
        expression is a tuple.

        :param   query: query with parameter placeholder
        :type    query: string
        :param   params: tuple of actuals for the parameters.
        :type    params: (<any>)
        :return: (None,None) if all ok, else tuple: (errorList, warningsList)
                
        '''
        cursor=self.connection.cursor()                                                                        
        try:                                                                                                   
            with no_db_warnings():
                try:
                    cursor.execute(query,params)
                except Exception:
                    # The following show_warnings() will
                    # reveal the error:
                    pass
            mysql_warnings = self.connection.show_warnings()
            if len(mysql_warnings) > 0:
                warnings   = [warning_tuple for warning_tuple in mysql_warnings if warning_tuple[0] == 'Warning']
                errors     = [error_tuple for error_tuple in mysql_warnings if error_tuple[0] == 'Error']
        finally:
            self.connection.commit()                                                                                                                                                                          
            cursor.close()
            return (None,None) if len(mysql_warnings) == 0 else (errors, warnings)

    # ----------------------- Utilities -------------------------                    


    #-------------------------
    # query_exhausted 
    #--------------

    def query_exhausted(self, cursor):

        # Delete the cursor's entry in the 
        # cursors { queryStr --> cursor } dict.
        # Since there will be few entries, the
        # following dictionary scan isn't a problem:
    
        for query_str, the_cursor in self.cursors.items():
            if the_cursor == cursor:
                del self.cursors[query_str]
        cursor.close()
    
    #-------------------------
    # _ensureSQLTyping
    #--------------
    
    def _ensureSQLTyping(self, colVals):
        '''
        Given a list of items, return a string that preserves
        MySQL typing. Example: (10, 'My Poem') ---> '10, "My Poem"'
        Note that ','.join(map(str,myList)) won't work:
        (10, 'My Poem') ---> '10, My Poem'

        :param colVals: list of column values destined for a MySQL table
        :type colVals: <any>
        :return: string of string-separated, properly typed column values
        :rtype: string
        '''

        resList = []
        for el in colVals:
            if isinstance(el, basestring):
                try:
                    # If value is not already UTF-8, encode it:
                    cleanStr = unicode(el, 'UTF-8', 'replace')
                except TypeError:
                    # Value was already in Unicode, so all is well:
                    cleanStr = el
                resList.append('"%s"' % cleanStr)
            elif el is None:
                resList.append('null')
            elif isinstance(el, (list, dict, set)):
                resList.append('"%s"' % str(el))
            else: # e.g. numbers
                resList.append(el)
        try:
            return ','.join(map(unicode,resList))
        except UnicodeEncodeError as e:
            print('Unicode related error: %s' % `e`)
            
    #-------------------------
    # _stringifyList
    #--------------
    
    def _stringifyList(self, iterable):
        '''
        Goes through the iterable. For each element, tries
        to turn into a string, part of which attempts encoding
        with the 'ascii' codec. Then encountering a unicode
        char, that char is UTF-8 encoded.
        
        Acts as an iterator! Use like:
        for element in _stringifyList(someList):
            print(element)

        :param iterable: mixture of items of any type, including Unicode strings.
        :type iterable: [<any>]
        '''
        for element in iterable:
            try:
                yield(str(element))
            except UnicodeEncodeError:
                yield element.encode('UTF-8','ignore')

# ----------------------- Class QueryResult -------------------------

class QueryResult(object):
    '''
    Iterator for query results. Given an instance
    of this class I, the following works as expected:

        for res in I:
            print(res)
            
    Instances of this class are returned by MySQLDB's 
    query() method. Use next() and nextall() to get
    one result at a time, or all at once.
    '''
  
    def __init__(self, cursor, query_str, cursor_owner_obj):
        self.mysql_cursor = cursor
        self.cursor_owner = cursor_owner_obj
        self.the_query_str    = query_str
        self.exhausted    = False
      
    def __iter__(self):
        return self

    def next(self):
        '''
        Return the next result in a query, 
        or raise StopIteration if no results
        remain.
        
        Note: Returns the zeroeth element
        of the result. So:
        `('foo',)  --> 'foo'` 
        and 
        `({'col1' : 10},) ==> {'col1' : 10}`

        This convention is in contrast to what
        cursor.fetconeh() does.
        
        '''
  
        res = self.mysql_cursor.fetchone()
        if res is None:
            self.cursor_owner.query_exhausted(self.mysql_cursor)
            self.exhausted = True
            raise StopIteration()            
        else:
            if len(res) == 1:
                return res[0]
            else:
                return res
          
    def nextall(self):
        '''
        Returns the remaining query results as a 
        tuple of tuples.
        
        :return: all remaining tuples inside a wrapper tuple, or empty tuple
                 if no results remain.
        :rtype: ((str))
        
        '''
        all_remaining = self.mysql_cursor.fetchall()
        # We exhausted the query, so clean up:
        self.cursor_owner.query_exhausted(self.mysql_cursor)
        self.exhausted = True
        return all_remaining

    def query_str(self):
        '''
        Provide query that led to this result.
        '''
        return self.the_query_str

    def result_count(self):
        '''
        Return the number of results in this result object.
        '''
        if self.exhausted:
            raise ValueError("Query '%s' is no longer active." % self.query_str())
        return self.mysql_cursor.rowcount
      
      
# ----------------------- Context Managers -------------------------    

# Ability to write:
#    with no_warn_no_table():
#       ... DROP TABLE IF NOT EXISTS ...
# without annoying Python-level warnings that the
# table did not exist:

@contextmanager
def no_warn_no_table():
    filterwarnings('ignore', message="Unknown table", category=db_warning)
    yield
    resetwarnings()

@contextmanager
def no_warn_dup_key():
    filterwarnings('ignore', message="Duplicate entry", category=db_warning)
    yield
    resetwarnings()

@contextmanager
def no_db_warnings():
    filterwarnings('ignore', category=db_warning)
    yield
    resetwarnings()
