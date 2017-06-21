import logging
from nose.tools import assert_equals, assert_not_equals, assert_raises, assert_true, assert_false
from swiftcontents.swiftfs import SwiftFS, HTTPError

# list of dirs to make
# note, directory names must end with a /
testDirectories = ['temp/',
                   'temp/bar/',
                   'temp/bar/temp/',
                   'temp/bar/temp/bar/',
                   'temp/bar/temp/bar/foo/',
                   'temp/bar/temp/bar/foo/bar/',
                   'temp/baz/',
                   'temp/baz/temp/',
                   'temp/baz/temp/bar/',
                   'temp/baz/temp/bar/foo/',
                   'temp/baz/temp/bar/foo/bar/']
testFileName = 'hello.txt'
testFileContent = 'Hello world'

log = logging.getLogger('TestSwiftFS')

class Test_SwiftNoFS(object):
    def __init__(self):
        self.swiftfs = SwiftFS()

    def guess_type(self,gtype):
        log.info('testing guess_type %s',gtype)
        
    def test_guess_type_notebook(self):
        log.info('testing guess_type notebook')
        assert_equals (self.swiftfs.guess_type('foo.ipynb'), 'notebook')
        assert_not_equals (self.swiftfs.guess_type('foo/'), 'notebook')
        assert_not_equals (self.swiftfs.guess_type('foo'), 'notebook')
        assert_not_equals (self.swiftfs.guess_type('foo/', allow_directory=False), 'notebook')

    def test_guess_type_directory(self):
        log.info('testing guess_type directory')
        assert_not_equals (self.swiftfs.guess_type('foo.ipynb'), 'directory')
        assert_equals (self.swiftfs.guess_type('foo/'), 'directory')
        assert_not_equals (self.swiftfs.guess_type('foo'), 'directory')
        assert_not_equals (self.swiftfs.guess_type('foo/', allow_directory=False), 'directory')
        
    def test_guess_type_file(self):
        log.info('testing guess_type directory')
        assert_not_equals (self.swiftfs.guess_type('foo.ipynb'), 'file')
        assert_not_equals (self.swiftfs.guess_type('foo/'), 'file')
        assert_equals (self.swiftfs.guess_type('foo'), 'file')
        assert_equals (self.swiftfs.guess_type('foo/', allow_directory=False), 'file')

    def test_clean_path(self):
        log.info('testing clean path')
        assert_equals (self.swiftfs.clean_path('foo/'), 'foo/')
        assert_equals (self.swiftfs.clean_path('foo'), 'foo')
        assert_equals (self.swiftfs.clean_path('/foo/'), 'foo/')
        assert_equals (self.swiftfs.clean_path('/foo'), 'foo')

    def test_do_error(self):
        log.info('test do_error')
        assert_raises(HTTPError,self.swiftfs.do_error,"test error")
        
    def test_directory(self):
        log.info('test creating a directory')
        p = 'a_test_dir/'
        self.swiftfs.mkdir(p)
        log.info('test directory exists')
        assert_true (self.swiftfs.isdir(p))
        assert_false(self.swiftfs.isfile(p))
        log.info('test directory can be deleted')
        self.swiftfs.rm(p)
        log.info('test directory is gone')
        assert_false (self.swiftfs.isdir(p))

    def test_file(self):
        log.info('test create a file')
        p = 'a_test_file.txt'
        self.swiftfs._do_write(p, testFileContent)
        assert_false (self.swiftfs.isdir(p))
        assert_true (self.swiftfs.isfile(p))
        log.info('test file can be deleted')
        self.swiftfs.rm(p)
        log.info('test file is gone')
        assert_false (self.swiftfs.isfile(p))
        
class Test_SwiftFS(object):
    def __init__(self):
        self.swiftfs = SwiftFS()

    def setup(self):
        log.info('setting up directory structure')
        for d in testDirectories:
            self.swiftfs.mkdir(d)
        log.info('create a bunch of files')
        for d in testDirectories:
            p = d+testFileName
            self.swiftfs._do_write(p, testFileContent)

    def teardown(self):
        log.info('tidy up directory structure')
        self.swiftfs.rm(testDirectories[0],recursive=True)
        assert_false(self.swiftfs.isdir(testDirectories[0]))

    def test_setup(self):
        log.info('check all directories exist')
        for d in testDirectories:
            assert_true(self.swiftfs.isdir(d))
            assert_false(self.swiftfs.isfile(d))
        log.info('check all test files exist')
        for d in testDirectories:
            p = d+testFileName
            assert_true(self.swiftfs.isfile(p))
