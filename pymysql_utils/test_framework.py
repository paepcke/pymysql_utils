import unittest
import os

class MyTestRunner(unittest.TextTestRunner):
    '''
    Subclass the Python unittest TextTestRunner.
    Purpose: to execute the test suite in 
    test_pymysql_utils.py twice. Once for the 
    mysqlclient substrate, and once for pymysql.
    
    This test runner must be requested in setup.py
    via setup() argument. Notice the <package>:<module>
    notation:

        test_runner      = 'pymysql_utils:test_framework.MyTestRunner',
        
    '''

    curr_dir = os.path.dirname(__file__)

    #-------------------------
    # get_test_suite
    #--------------

    def get_test_suite(self):
        '''
        Construct a unittest Test Suite by recursively
        finding Python modules whose name starts with 'test_'.
        
        @return: a test suite object ready to run
        @rtype: unittest.TestLoader
        '''
        # Use the ready-made instance provided by unittest
        # as the loader:
        test_loader = unittest.defaultTestLoader
        # Starting in directory pymysql_utils, find test modules:
        test_suite = test_loader.discover('pymysql_utils', pattern='test_*.py')
        return test_suite

    #-------------------------
    # read_config_file
    #--------------

    def read_config_file(self):
        '''
        Read and return content of pymysql_utils_config.py
        
        @return: content of pymysql_utils_config.py
        @rtype: str
        '''
        config_file_name = os.path.join(MyTestRunner.curr_dir, 'pymysql_utils_config.py')
        with open(config_file_name, 'r') as fd:
            return fd.read() 

    #-------------------------
    # write_config_file 
    #--------------
    
    def write_config_file(self, content):
        '''
        Given the contents of pymysql_utils_config.py, write
        that content to that file.
        
        @param content: configuration information as used in pymysql_utils_config.py
        @type content: str
        '''
        config_file_name = os.path.join(MyTestRunner.curr_dir, 'pymysql_utils_config.py')
        with open(config_file_name, 'w') as fd:
            return fd.write(content) 

    #-------------------------
    # run
    #--------------

    def run(self, *args, **kwargs):
        '''
        Override the default unittest loader's 'run' method.
        We read the pymysql_utils_config.py content so we can
        restore it later. We then set the configuration to use
        mysqlclient (i.e. MySQLdb) as substrate, and run the 
        test suite.
        
        We then change pymysql_utils_config.py to request running
        Python only (i.e. with pymysql as substrate). The same
        tests are repeated.
        
        Finally, the original content of pymysql_utils_config.py
        is restored.
        '''

        # Remember current state of config file:
        config_file_orig = self.read_config_file()
        
        try:
            # First set of testing: using MySQLdb (i.e. mysqlclient)
            # as underlying lib:
            self.write_config_file('FORCE_PYTHON_NATIVE = False\n')
            
            print("****** Testing over mysqlclient (MySQLdb) substrate...")
            # Just use the default test running logic:      
            result1 = super().run(self.get_test_suite())
            print("****** Done testing over mysqlclient (MySQLdb) substrate.")        
            
            # Run same tests again, but with pymysql as underlying lib:
            self.write_config_file('FORCE_PYTHON_NATIVE = True\n')    
            
            print("****** Testing over pymysql (Python-only mysql client) substrate...")
            # Just use the default test running logic:        
            _result2 = super().run(self.get_test_suite())
            print("****** Done testing over pymysql (Python-only mysql client) substrate.")        
        finally:
            # Restore original configuration:
            print("****** Restoring original pymysql_utils_config.py...")
            self.write_config_file(config_file_orig)
            print("****** Done restoring original pymysql_utils_config.py.")
        return result1
