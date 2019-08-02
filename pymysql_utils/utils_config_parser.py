'''
Created on Aug 1, 2019

@author: paepcke
'''
import configparser, os

class UtilsConfigParser(configparser.ConfigParser):
    '''
    Container for a singleton configuration parser.
    Uses delegation to have the Pytyhon 3 built-in
    ConfigParser do the work. But knows where to look
    for pymysql_utils config files.
    '''
    config_parser = None

    #-------------------------
    # __new__ 
    #--------------

    def __new__(cls, *args):
        '''
        Create an instance of configparser.ConfigParser,
        i.e. of the superclass. Let the superclass call
        the __init__() method.
        
        @param cls: newly minted instance of UtilsConfigParser 
        @type cls: UtilsConfigParser
        @return: instance superclass
        @rtype: configparser.ConfigParser
        '''
        if cls.config_parser is not None:
            return cls.config_parser

        # Create a pure configparser.ConfigParser instance:
        obj = super().__new__(cls)
        # And do the superclass-level initialization:
        super().__init__(obj)

        # Remember that instance for reuse
        cls.config_parser = obj
        # Return the partially initialized instance.
        # The UtilsConfigParser class' __init__() will
        # by called after this return.
        return obj

    #-------------------------
    # constructor 
    #--------------

    def __init__(self, config_files=None):
        '''
        Find all places where pymysql_util config files
        are stored. Remember that list, and load this 
        configparser.ConfigParser instance with the configurations.
        
        By default we assume config files to be either in 
        the current directory (of this script), or in $HOME/.pymysql_utilsrc
        
        @param config_files: if provided, a list of alternative
            configuration file full paths
        @type config_files: str
        '''
        
        curr_dir = os.path.dirname(__file__)
        HOME     = os.getenv('HOME')

        if config_files is None:
            self.config_file_locs = [os.path.join(HOME, '.pymysql_utilsrc'),
                                          os.path.join(curr_dir, 'pymysql_utils.cnf')
                                          ]
        else:
            self.config_file_locs = config_files
        
        self._initialize_data()

    #-------------------------
    # write 
    #--------------

    def write(self):
        '''
        Call super's write() method with the 
        original config file list.
        '''
        for config_file in self.config_file_locs:
            with open(config_file, 'w') as fd:
                super().write(fd)
        
    #-------------------------
    # refresh 
    #--------------
        
    def refresh(self):
        self._initialize_data()

    # --------------------------------- Private Utilities -------------

    #-------------------------
    # _initialize_data 
    #--------------

    def _initialize_data(self):
        '''
        Assume that self.config_file_locs contain the list of 
        config files (full paths). Load data from this list into 
        this configparser.ConfigParser instance.
        '''
        
        # Look in all locations where we allow the config file,
        # and remember which we read. The read() method returns
        # a list of locations:
        self.config_file_locs = UtilsConfigParser.config_parser.read(self.config_file_locs)    

    #-------------------------
    # _clear 
    #--------------

    def _clear(self):
        '''
        Destroy the ConfigParser singleton instance.
        Instantiations following this call will create
        a new singleton.
        
        Used only for unittesting. 
        '''
        UtilsConfigParser.config_parser = None
        self.config_file_locs = None