'''
Created on Aug 1, 2019

@author: paepcke
'''
from tempfile import NamedTemporaryFile
import unittest
from utils_config_parser import UtilsConfigParser
from IPython.external.decorators._decorators import skipif

TEST_ALL = True
#TEST_ALL = False

class TestUtilConfigParser(unittest.TestCase):

    #------------------- Setup -----------------

    #-------------------------
    # setUp
    #--------------

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.tmp_file1 = NamedTemporaryFile(prefix='config_paser_tmp1', suffix='.cnf')
        self.tmp_file2 = NamedTemporaryFile(prefix='config_paser_tmp2', suffix='.cnf')
        self.make_config1()
        self.make_config2()

    #-------------------------
    # tearDown 
    #--------------

    def tearDown(self):
        self.tmp_file1.close()
        self.tmp_file2.close()
        
    #------------------------ Tests -----------------
    
    #-------------------------
    # testReadFromOneConfigFile
    #--------------

    @unittest.skipIf(TEST_ALL != True, 'temporary skip')
    def testReadFromOneConfigFile(self):
        cp = UtilsConfigParser([self.tmp_file1.name])
        self.assertEqual(len(cp['Default']), 0)  # Upper case empty section
        # Section names are case sensitive:
        with self.assertRaises(KeyError):
            len(cp['default'])
        
        self.assertEqual(cp.getint('Section1', 'sec_1_key1'), 10)
        self.assertEqual(cp['Section1']['sec_1_key2'], '20')
        
        self.assertEqual(cp.sections(), ['Default', 'Section1'])
        
        # No section2 in config 1:
        with self.assertRaises(KeyError):
            cp['Section2']

        # Destroy the instance
        self.assertEqual(cp.config_file_locs, [self.tmp_file1.name])
        self.assertTrue(isinstance(cp.config_parser, UtilsConfigParser))
        cp._clear()
        self.assertIsNone(cp.config_file_locs)
        self.assertIsNone(cp.config_parser)
        
    #-------------------------
    # testReadFromTwoConfigFiles 
    #--------------
    
    @unittest.skipIf(TEST_ALL != True, 'temporary skip')
    def testReadFromTwoConfigFiles(self):    
        cp = UtilsConfigParser([self.tmp_file1.name, self.tmp_file2.name])
        self.assertEqual(len(cp['Default']), 0)  # Upper case empty section
        # Section names are case sensitive:
        with self.assertRaises(KeyError):
            len(cp['default'])
        
        # Second config overwrites the sec_1_key1 value from 10 to 30:
        self.assertEqual(cp['Section1']['sec_1_key1'], '30')
        self.assertEqual(cp['Section1']['sec_1_key2'], '20')
        self.assertEqual(cp.getint('section2', 'sec_2_key1'), 40)
        
        self.assertEqual(cp.sections(), ['Default', 'Section1', 'section2'])
        
        cp._clear()
        
    #-------------------------
    # testWriteLocallyAndToFile 
    #--------------
    
    @unittest.skipIf(TEST_ALL != True, 'temporary skip')
    def testWriteLocallyAndToFile(self):    
        cp = UtilsConfigParser([self.tmp_file1.name])
        
        self.assertEqual(cp.getint('Section1', 'sec_1_key1'), 10)
        cp['Section1']['sec_1_key1'] = '50'
        self.assertEqual(cp.getint('Section1', 'sec_1_key1'), 50)
        
        # Write back to config file:
        cp.write()
        
        # Get a new parser to re-read
        cp._clear()
        cp = UtilsConfigParser([self.tmp_file1.name])
        # Should have the new value:
        cp['Section1']['sec_1_key1'] = '50'
        
    #-------------------------
    # testRefresh 
    #--------------

    @unittest.skipIf(TEST_ALL != True, 'temporary skip')
    def testRefresh(self):
        cp = UtilsConfigParser([self.tmp_file1.name])
        
        self.assertEqual(cp.getint('Section1', 'sec_1_key1'), 10)
        cp['Section1']['sec_1_key1'] = '50'
        self.assertEqual(cp.getint('Section1', 'sec_1_key1'), 50)

        cp.refresh()
        # Should revert:
        self.assertEqual(cp.getint('Section1', 'sec_1_key1'), 10)

    #-------------------------
    # testTypeSpecificRead 
    #--------------

    @unittest.skipIf(TEST_ALL != True, 'temporary skip')
    def testTypeSpecificRead(self):
        cp = UtilsConfigParser([self.tmp_file1.name])
        
    # ---------------------- Utilities ------------
    def make_config1(self):
        conf = b'''
        [Default]
        [Section1]
        sec_1_key1 = 10
        sec_1_key2 : 20
        '''
        self.tmp_file1.write(conf)
        self.tmp_file1.flush()
        
    def make_config2(self):
        conf = b'''
        [Default]
        [Section1]
        sec_1_key1 = 30
        sec_1_key2 : 20
        [section2]
        sec_2_key1 = 40
        '''
        self.tmp_file2.write(conf)
        self.tmp_file2.flush()
        
    def make_config3(self):
        conf = b'''
        [Default]
        key1 = 30
        key2 : 'My Bonny lies over the ocean.'
        key3 : True
        key4 : 1
        key5 : 'True'
        key6 : 'true'
        key7 : 3.14159
        '''
        self.tmp_file2.write(conf)
        self.tmp_file2.flush()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testReadFromOneConfigFile']
    unittest.main()