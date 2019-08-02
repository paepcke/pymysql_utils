import unittest
import os

from pymysql_utils.utils_config_parser import UtilsConfigParser

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
    # read_config_file_content
    #--------------

    def read_config_file_content(self):
        '''
        Read and return entire content of pymysql_utils.cnf
        file without any parsing. Used for save/restore.
        
        @return: content of pymysql_utils.cnf
        @rtype: str
        '''
        config_file_name = os.path.join(MyTestRunner.curr_dir, 'pymysql_utils.cnf')
        try:
            with open(config_file_name, 'r') as fd:
                return fd.read()
        except:
            # No config file exists:
            return None 

    #-------------------------
    # write_config_file_content 
    #--------------
    
    def write_config_file_content(self, content):
        '''
        Given contents for pymysql_utils.cnf, write
        that content to that file.
        
        @param content: configuration information as used in pymysql_utils.cnf
        @type content: str
        '''
        config_file_name = self.get_config_file_name()
        if content is None:
            # No config file existed at the start of testing.
            # So remove the file we created:
            os.remove(config_file_name)
        else:
            with open(config_file_name, 'w') as fd:
                fd.write(content)
                fd.flush() 

    #-------------------------
    # create_configuration 
    #--------------

    def create_configuration(self):
        '''
        Create a temporary config file with just the
        python substrate instruction. Write that out to
        pymysql_utils.cnf. Make sure you call read_config_file_content()
        first, to save the content of a possibly existing
        pymysql_utils.cfg file, or mv the existing file
        somewhere. 
        
        Sets self.config_parser to an instance of UtilsConfigParser
        loaded with the new configuration. That configuration is
        also written out to pymysql_utils.cfg
        '''
        config = "[substrate]\n" + "FORCE_PYTHON_NATIVE = False\n"

        config_file_name = self.get_config_file_name()
        with open(config_file_name, 'w') as fd:
            fd.write(config)
            fd.flush()
        
        self.config_parser = UtilsConfigParser([config_file_name])

        
    #-------------------------
    # get_config_file_name 
    #--------------
    
    def get_config_file_name(self):
        return os.path.join(MyTestRunner.curr_dir, 'pymysql_utils.cnf')

    #-------------------------
    # set_python_substrate
    #--------------
    
    def set_python_substrate(self, force_python_native):
        '''
        Given the configuration option True/False for 
        the FORCE_PYTHON_NATIVE config key, set that key
        in the existing UtilsConfigParser, which is expected
        in self.config_parser. The new configuration is written
        to pymysql_utils.cnf.
        
        @param force_python_native: whether or not to use pymysql substrate
        @type force_python_native: bool
        '''
        self.config_parser['substrate']['FORCE_PYTHON_NATIVE'] = str(force_python_native)
        self.config_parser.write()

    #-------------------------
    # run
    #--------------

    def run(self, *args, **kwargs):
        '''
        Override the default unittest loader's 'run' method.
        We read the pymysql_utils.cnf content so we can
        restore it later. We then set the configuration to use
        mysqlclient (i.e. MySQLdb) as substrate, and run the 
        test suite.
        
        We then change pymysql_utils.cnf to request running
        Python only (i.e. with pymysql as substrate). The same
        tests are repeated.
        
        Finally, the original content of pymysql_utils.cnf
        is restored.
        '''

        # Remember current state of config file. If return is
        # None, no config file existed:
        config_file_orig = self.read_config_file_content()
        
        # Create a very simple configuration, just with FORCE_PYTHON_NATIVE.
        # The method also initializes self.config_parser with 
        # a UtilsConfigParser that holds the simple configuration, 
        # enables changes via set_python_substrate(), and writing out
        # those changes:
        
        self.create_configuration()
        
        try:
            # First set of testing: using MySQLdb (i.e. mysqlclient)
            # as underlying lib:
            self.set_python_substrate(False)
            
            print("****** Testing over mysqlclient (MySQLdb) substrate...")
            # Just use the default test running logic:      
            result1 = super().run(self.get_test_suite())
            print("****** Done testing over mysqlclient (MySQLdb) substrate.")        
            
            # Run same tests again, but with pymysql as underlying lib:
            self.set_python_substrate(True)            
            
            print("****** Testing over pymysql (Python-only mysql client) substrate...")
            # Just use the default test running logic:        
            _result2 = super().run(self.get_test_suite())
            print("****** Done testing over pymysql (Python-only mysql client) substrate.")        
        finally:
            # Restore original configuration:
            print("****** Restoring original pymysql_utils.cnf...")
            self.write_config_file_content(config_file_orig)
            print("****** Done restoring original pymysql_utils.cnf.")
        return result1
