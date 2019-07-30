'''
Created on Sep 24, 2013

@author: Andreas Paepcke

Wrapper for the mysqlclient Python MySQL access library. Replaces
the cursor notion with query result iterators, adds methods for 
frequent database operations.

For usage details, see `Github README <https://github.com/paepcke/pymysql_utils>`
or the PyPi project description. HTML documentation for the code below
is available in the docs subdirectory.

This module is designed for MySQL 5.6, 5.7, and 8.0. Both, Python 2.7,
and Python 3.6+ are supported. See documentation for the combinations
that have been tested. 
 
'''

from collections import OrderedDict
from contextlib import contextmanager
import csv
import os
import re
import socket
import subprocess
import tempfile
from warnings import filterwarnings, resetwarnings


# Find out whether we are to use the C-based MySQLdb
# (mysqlclient) package, or the Python-only pymysql package.
# MySQLdb is the default. If FORCE_PYTHON_NATIVE is False
# or not defined in pymysql_utils_config.py, or if that file
# is unavailable, use the default:

try:
    import pymysql_utils_config
    FORCE_PYTHON_NATIVE = pymysql_utils_config.FORCE_PYTHON_NATIVE
except (ImportError, AttributeError):
    FORCE_PYTHON_NATIVE = False

# The mysql_api will either be 'pymysql'
# or MySQLdb, depending on whether we will
# use the Python native underlying library,
# or the MySQLdb:

mysql_api = None

if FORCE_PYTHON_NATIVE:
    try:
        import pymysql
        from pymysql import Warning as db_warning
        from pymysql.err import ProgrammingError, OperationalError
        from pymysql.cursors import DictCursor as DictCursor
        from pymysql.cursors import SSCursor as SSCursor
        from pymysql.cursors import SSDictCursor as SSDictCursor
        mysql_api = pymysql
    except ImportError:
        raise ImportError("Import directive FORCE_PYTHON_NATIVE specified in pymysql_utils_config.py, but pymysql library not available.")
else:    
    import MySQLdb
    from MySQLdb import Warning as db_warning
    from MySQLdb._exceptions import ProgrammingError, OperationalError
    from MySQLdb.cursors import DictCursor as DictCursor
    from MySQLdb.cursors import SSCursor as SSCursor
    from MySQLdb.cursors import SSDictCursor as SSDictCursor
    mysql_api = MySQLdb

# To check for variable being a string in both Python 2.7 and 3.x:
try:
    eval('basestring')
except NameError:
    # We are running Python 3:
    basestring = str

class DupKeyAction:
    PREVENT = 0
    IGNORE  = 1
    REPLACE = 2

# Cursor classes as per: 
#    http://mysql-python.sourceforge.net/MySQLdb-1.2.2/public/MySQLdb.cursors.BaseCursor-class.html
# Used to pass to query() method if desired:   

class Cursors:
    BASIC           = None
    DICT            = DictCursor
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
        
        @param host: MySQL host
        @type host: string
        @param port: MySQL host's port
        @type port: int
        @param user: user to log in as
        @type user: string
        @param passwd: password to use for given user
        @type passwd: string
        @param db: database to connect to within server
        @type db: string
        @param cursor_class: choice of how rows are returned
        @type cursor_class: Cursors
        @raise ValueError
        @raise RuntimeError if mysql client program not found.
    
        '''
        
        # If all arguments are set to None, we are unittesting:
        if all(arg is None for arg in (host,port,user,passwd,db)):
            return
        
        # However, cannot have any of the connection parms None.
        # Get a list of parameter names whose values are None:
        potential_offenders = OrderedDict({'host' : host,
                                           'db' : db, 
                                           'user' : user, 
                                           'passwd' : passwd, 
                                           'port' : port}
                                          )
        bad_parms = [none_parms for none_parms in potential_offenders.keys() if potential_offenders[none_parms] is None]
        # If 'host' and 'user' are None, we now have: ['host', 'user']
        # If none of the above parms is None, non_parms will be empty:
        if len(bad_parms) > 0:
            raise ValueError('None value(s) for %s; none of host,port,user,passwd or db must be None' % bad_parms)
        
        if cursor_class is not None:
            # Ensure we caller passed a valid cursor class:
            if cursor_class not in [DictCursor, SSCursor, SSDictCursor]:
                raise ValueError("Non-existing cursor class '%s'" % str(cursor_class))
        
        # Ensure proper data types: all but port must be strings:
        del potential_offenders['port']
        
        type_offenders = [bad_type_parms for bad_type_parms in potential_offenders.keys() 
                          if type(potential_offenders[bad_type_parms]) != str]
        
        # OK for port to be an int:
        if len(type_offenders) > 0:
            raise ValueError('Value(s) %s have bad type;host,user,passwd, and db must be strings; port must be int.' 
                             % type_offenders)
        
        if type(port) != int:
            raise ValueError('Port must be an integer; was %s.' % type(port))
        
        
        self.user = user
        self.pwd  = passwd
        self.db   = db
        self.name = db
        # Will hold querySt->cursor for two purposes:
        # Ability to retrieve number of results in SELECT,
        # Ensure that all cursors are closed (see query()):
        self.cursors = {}
        self.most_recent_query = None
        self.connection = None
        
        # Find location of mysql client program.
        # Will raise error if not found.
        MySQLDB.find_mysql_path()
        
        try:
            if cursor_class is None:
                self.connection = mysql_api.connect(host=host,
                                                    port=port, 
                                                    user=user, 
                                                    passwd=passwd, 
                                                    db=db,
                                                    charset='utf8',
                                                    local_infile=1)
            else:
                self.connection = mysql_api.connect(host=host, 
                                                    port=port, 
                                                    user=user, 
                                                    passwd=passwd, 
                                                    db=db,
                                                    charset='utf8',
                                                    cursorclass=cursor_class,
                                                    local_infile=1)
          
        except OperationalError as e:
            pwd = '...............' if len(passwd) > 0 else '<no password>'
            raise ValueError('Cannot reach MySQL server with host:%s, port:%s, user:%s, pwd:%s, db:%s (%s)' %
                             (host, port, user, pwd, db, repr(e)))
        # Unanticipated errors:
        except Exception as e:
            pwd = '...............' if len(passwd) > 0 else '<no password>'
            raise RuntimeError('Error when connecting to database with host:%s, port:%s, user:%s, pwd:%s, db:%s (%s)' %
                             (host, port, user, pwd, db, repr(e)))
      

    #-------------------------
    # dbName 
    #--------------
        
    def dbName(self):
        '''
        Return name of database to which this MySQLDB is connected.
        
        @return: name of database within MySQL server to which MySQLDB instance is connected.
        @rtype: String
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

    #-------------------------
    # isOpen
    #--------------

    def isOpen(self):
        '''
        Returns True if the connection to the databases
        is open. 
        '''
        if self.connection is not None:
            return self.connection.open != 0
        else:
            return False
      
    # ----------------------- Table Management -------------------------

    #-------------------------
    # createTable
    #--------------
    
    def createTable(self, tableName, schema, temporary=False):
        '''
        Create new table, given its name, and schema.
        The schema is a dict mappingt column names to 
        column types. Example: {'col1' : 'INT', 'col2' : 'TEXT'}

        @param tableName: name of new table
        @type tableName: String
        @param schema: dictionary mapping column names to column types
        @type schema: Dict<String,String>
        @raise: ValueError: if table to truncate does not exist or permissions issues.        
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

        @param tableName: name of table
        @type tableName: String
        @raise: ValueError: if table to drop does not exist or permissions issues.        
        '''
        cursor = self.connection.cursor()
        try:
            # Suppress warning about table not existing:
            with no_warn_no_table():
                cursor.execute('DROP TABLE IF EXISTS %s' % tableName)
            self.connection.commit()
        except OperationalError as e:
            raise ValueError("In pymysql_utils dropTable(): %s" % repr(e))
        except ProgrammingError as e:
            raise ValueError("In pymysql_utils dropTable(): %s" % repr(e))
        finally:
            cursor.close()

    #-------------------------
    # truncateTable 
    #--------------

    def truncateTable(self, tableName):
        '''
        Delete all table rows. No errors

        @param tableName: name of table
        @type tableName: String
        @raise: ValueError: if table to truncate does not exist or permissions issues.
        '''
        cursor = self.connection.cursor()
        try:
            try:
                cursor.execute('TRUNCATE TABLE %s' % tableName)
            except OperationalError as e:
                raise ValueError("In pymysql_utils truncateTable(): %s." % repr(e))
            except ProgrammingError as e:
                raise ValueError("In pymysql_utils truncateTable(): %s." % repr(e))
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

        @param tblName: name of table to insert into
        @type tblName: String
        @param colnameValueDict: mapping of column name to column value
        @type colnameValueDict: Dict<String,Any>
        @return (None,None) if all ok, else tuple (errorList, warningsList)
        @rtype: {(None,None) | ({ (str) | None},{ (str) | None})}        
        '''

        errors   = []
        warnings = []

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
                if len(warnings) == 0:
                    warnings = None
                if len(errors) == 0:
                    errors = None                
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
                
        @param tblName: table into which to insert
        @type  tblName: string
        @param colNameTuple: tuple containing column names in proper order, i.e. 
               corresponding to valueTupleArray orders.
        @type  colNameTuple: (str[,str[...]])
        @param valueTupleArray: array of n-tuples, which hold the values. Order of
               values must correspond to order of column names in colNameTuple.
        @type  valueTupleArray: `[(<MySQLVal> [,<MySQLval>,...]])`
        @param onDupKey: determines action when incoming row would duplicate an existing row's
               unique key. If set to DupKeyAction.IGNORE, then the incoming tuple is
               skipped. If set to DupKeyAction.REPLACE, then the incoming tuple replaces
               the existing tuple. By default, each attempt to insert a duplicate key
               generates a warning.
        @type  onDupKey: DupKeyAction
        @return: (None,None) is no errors or warnings occurred, else 
                a tuple with (errorList, warningsList).
                Elements of warningsList reflect MySQL's output of "show warnings;"
                Example::
                ((u'Warning', 1062L, u"Duplicate entry '10' for key 'PRIMARY'"),)
                            
        @rtype: {(None,None) | ({ (str) | None},{ (str) | None})}
        @raise: ValueError if bad parameter.
        
        '''

        errors   = []
        warnings = []

        tmpCSVFile = tempfile.NamedTemporaryFile(dir='/tmp',prefix='bulkInsert',suffix='.csv',mode='w')
        # Allow MySQL to read this tmp file:
        os.chmod(tmpCSVFile.name, 0o644)
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
            clean_row = [rowElement for rowElement in self._stringifyList(row)]
            self.csvWriter.writerow(clean_row)
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

        @param tblName: name of table in which update is to occur
        @type tblName: String
        @param colName: column whose value is to be changed
        @type colName: String
        @param newVal: value acceptable to MySQL for the given column 
        @type newVal: type acceptable to MySQL for the given column 
        @param fromCondition: optionally condition that selects which rows to update.
                      if None, the named column in all rows are updated to
                      the given value. Syntax must conform to what may be in
                      a MySQL FROM clause (don't include the 'FROM' keyword)
        @type fromCondition: String
        @return (None,None) if all ok, else tuple (errorList, warningsList)
        @rtype {(None,None) | ([str],[str])}
        '''

        errors   = []
        warnings = []

        cursor = self.connection.cursor()
        try:
            if fromCondition is None:
                if newVal is None:
                    # The double-% causes the '%s' to be retained
                    # in the update string. The underlying mysqldb
                    # package will substitute that later for None.
                    # I.e.: create a two-tuple:
                    #
                    #   ('UPDATE unittest SET col1 = %s', (None,))
                    #
                    cmd = ("UPDATE %s SET %s = %%s" % (tblName,colName), (None,))
                else:
                    cmd = "UPDATE %s SET %s = '%s';" % (tblName,colName,newVal)
            else:
                if newVal is None:
                    # See above for explanation of double-%:
                    cmd = ("UPDATE %s SET %s = %%s WHERE %s;" % (tblName,colName,fromCondition), (None,))
                else:
                    cmd = "UPDATE %s SET %s = '%s' WHERE %s;" % (tblName,colName,newVal,fromCondition)
            with no_db_warnings():
                try:
                    # If setting to None, cmd will be a tuple
                    # with the %s-notation UPDATE string first,
                    # and the values second:
                    if newVal is None:
                        cursor.execute(cmd[0], cmd[1])
                    else:
                        cursor.execute(cmd)
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

        @param queryStr: the query to submit to MySQL
        @type queryStr: String
        @return: iterator of query results
        @rtype: iterator of tuples
        @raise ValueError on MySQL errors.
        '''

        # The str/byte/unicode type mess between
        # Python 2.7 and 3.x. We want as 'normal'
        # a string as possible. Surely there is a
        # more elegant way:
        
        queryStr = self.convert_to_string(queryStr)
       
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
            raise ValueError(repr(e))
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
        
        @param queryStr: query that was used in a prior call to query()
        @type queryStr: {None | string}
        @return: number of records in SELECT result.
        @rtype: int
        @raise: ValueError if no prior query is still active.
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
        
        @param query: query or directive
        @type query: String
        @return: (None,None) if all went well, else a tuple:
            (listOrErrors, listOfWarnings)
        @rtype {(None,None) | ([str],[str])}
        '''
        
        errors   = []
        warnings = []
        cursor=self.connection.cursor()                                                                        
        try:
            with no_db_warnings():
                try:                                                                                                   
                    cursor.execute(query)
                    # Eat any query results and discard them:
                    cursor.fetchall()
                except Exception:
                    # The following show_warnings() will
                    # reveal the error:
                    pass
        finally:                                                                                               
            if doCommit:
                self.connection.commit()
            try:
                # If the above MySQL call had an error,
                # it will generate an exception when 
                # close the cursor. But that error info
                # will be caught in the show_warnings()
                # below; so ignore it here:
                cursor.close()
            except Exception:
                pass

        mysql_warnings = self.connection.show_warnings()

        if len(mysql_warnings) > 0:
            warnings   = [warning_tuple for warning_tuple in mysql_warnings if warning_tuple[0] == 'Warning']
            errors     = [error_tuple for error_tuple in mysql_warnings if error_tuple[0] == 'Error']
            if len(warnings) == 0:
                warnings = None
            if len(errors) == 0:
                errors = None
        return(None,None) if len(mysql_warnings) == 0 else (errors, warnings)

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

        @param   query: query with parameter placeholder
        @type    query: string
        @param   params: tuple of actuals for the parameters.
        @type    params: (<any>)
        @return: (None,None) if all ok, else tuple: (errorList, warningsList)
        @rtype: {(None,None) | ([str],[str])}  
        '''

        errors   = []
        warnings = []
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
                if len(warnings) == 0:
                    warnings = None
                if len(errors) == 0:
                    errors = None
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
    
        cursors_copy = self.cursors.copy()
        for query_str, the_cursor in cursors_copy.items():
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

        @param colVals: list of column values destined for a MySQL table
        @type colVals: <any>
        @return: string of string-separated, properly typed column values
        @rtype: string
        '''

        resList = []
        for el in colVals:
            if isinstance(el, basestring):
                try:
                    # If value is not already UTF-8, encode it:
                    cleanStr = str(el, 'UTF-8', 'replace')
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
            return ','.join(map(str,resList))
        except UnicodeEncodeError as e:
            print('Unicode related error: %s' % repr(e))
            
    #-------------------------
    # convert_to_string
    #--------------
    
    def convert_to_string(self, strLike):
        '''
        The str/byte/unicode type mess between
        Python 2.7 and 3.x. We want as 'normal'
        a string as possible. Surely there is a
        more elegant way.
        
        @param strLike: a Python 3 str (i.e. unicode string), a Python 3 binary str.
            a Python 2.7 unicode string, or a Python 2.7 str.
        @type strLike: {str|unicode|byte}
        '''
        
        try:
            if type(strLike) == eval('unicode'):
                # Python 2.7 unicode --> str:
                strLike = strLike.encode('UTF-8')
        except NameError:
            pass
        
        try:
            if type(strLike) == eval('bytes'):
                # Python 3 byte string:
                strLike = strLike.decode('UTF-8')
        except NameError:
            pass
        
        return strLike

    
    #-------------------------
    # _stringifyList
    #--------------
    
    def _stringifyList(self, iterable):
        '''
        Goes through the iterable. For each element, tries
        to turn into a string, part of which attempts encoding
        with the 'ascii' codec. Then encountering a unicode
        char, that char is UTF-8 encoded. The special value
        None is *not* turned into a string. It will be turned
        into a NULL instead.
        
        Acts as an iterator! Use like:
        for element in _stringifyList(someList):
            print(element)

        @param iterable: mixture of items of any type, including Unicode strings.
        @type iterable: [<any>]
        '''
        for element in iterable:
            try:
                if element is None:
                    yield('NULL')
                else:
                    yield(str(element))
            except UnicodeEncodeError:
                yield element.encode('UTF-8','ignore')
    
    #-------------------------
    # find_mysql_path 
    #--------------
    
    @classmethod            
    def find_mysql_path(cls):
        '''
        Finds the location of the mysql client program
        on the current machine. Needed for testing. So
        we make sure the method works when running outside
        of Eclipse as well as in.
        
        Stores path in MySQLDB.mysql_loc.
        
        @param cls: class instance of MySQLDB
        @type cls: MySQLDB
        @return: path to mysql client application
        @rtype: str
        '''
        
        # The 'command -v mysql...' idiom always returns an empty
        # string when probed from Eclipse. To facilitate debugging
        # we find an alternative method for running in Eclipse.  
        
        mysql_loc = None
        # Eclipse puts extra info into the env:
        eclipse_indicator = os.getenv('XPC_SERVICE_NAME')
        
        # If the indicator is absent, or it doesn't include
        # the eclipse info, then we are not in Eclipse; the usual
        # case, of course:
        
        if eclipse_indicator is None or \
           eclipse_indicator == '0' or \
           eclipse_indicator.find('eclipse') == -1:
            # Not running in Eclipse; use reliable method to find mysql:
            mysql_loc = subprocess.check_output(
                                        "command -v mysql; exit 0",
                                        stderr=subprocess.STDOUT,
                                        shell=True,
                                        env=os.environ).strip()
            if len(mysql_loc) == 0:  
                raise RuntimeError("MySQL client not found on this machine (%s)" % socket.gethostname())
        else:
            # We are in Eclipse:
            possible_paths = ['/usr/local/bin/mysql',
                              '/usr/local/mysql/bin/mysql',
                              '/usr/bin/mysql',
                              '/bin/mysql']
            for path in possible_paths:
                if os.path.exists(path):
                    mysql_loc = path
                    break
            if mysql_loc is None:
                raise RuntimeError("MySQL client not found on this machine (%s)" % socket.gethostname())
        
        MySQLDB.mysql_loc = mysql_loc
        return mysql_loc
        

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
        
        @raise StopIteration
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
         
    __next__ = next
    
    def nextall(self):
        '''
        Returns the remaining query results as a 
        tuple of tuples.
        
        @return: all remaining tuples inside a wrapper tuple, or empty tuple
                 if no results remain.
        @rtype: ((str))
        
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
        
        @return number of query results
        @rtype int
        @raise ValueError if query no longer active.
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
    filterwarnings('ignore', message=r".*Unknown table.*", category=db_warning)
    yield
    resetwarnings()

@contextmanager
def no_warn_dup_key():
    filterwarnings('ignore', message=r".*Duplicate entry.*", category=db_warning)
    yield
    resetwarnings()

@contextmanager
def no_db_warnings():
    filterwarnings('ignore', category=db_warning)
    yield
    resetwarnings()
