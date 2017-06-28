import logging
from nose.tools import assert_equals, assert_not_equals, assert_raises, assert_true, assert_false,assert_set_equal, assert_not_in
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

# construct a dictionary containing all directories and their entries
testTree = {}
for d in testDirectories:
    pcomponents = d[:-1].split('/')
    for i in range(len(pcomponents)):
        parent = '/'.join(pcomponents[:i])+'/'
        child = pcomponents[i]+'/'
        if parent not in testTree:
            testTree[parent] = set()
        for pc in [parent+child,parent+testFileName]:
            if pc.startswith('/'):
                pc = pc[1:]
            testTree[parent].add(pc)


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
        log.info('test various variations including slashes in directory name')
        assert_true (self.swiftfs.isdir('a_test_dir'))
        assert_true (self.swiftfs.isdir('/a_test_dir'))
        assert_true (self.swiftfs.isdir('/a_test_dir/'))
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

    def test_read_write(self):
        log.info('test reading from and writing to a file')
        testString = "hello, world - magi was here"
        p = testDirectories[0] + testFileName
        self.swiftfs.write(p,testString)
        result = self.swiftfs.read(p)
        assert_equals(testString,result)

    def test_write_directory(self):
        log.info('test writing to a directory')
        p = 'a_test_dir/'
        self.swiftfs.mkdir(p)
        assert_raises(HTTPError,self.swiftfs.write,p,testFileContent)
        self.swiftfs.rm(p)

    def test_read_directory(self):
        log.info('test reading from a directory')
        p = 'a_test_dir/'
        self.swiftfs.mkdir(p)
        assert_raises(HTTPError,self.swiftfs.read,p)
        self.swiftfs.rm(p)

class Test_SwiftFS(object):
    def __init__(self):
        self.swiftfs = SwiftFS()

    def setup(self):
        log.info('setting up directory structure')
        for d in testDirectories:
            self.swiftfs.mkdir(d)
        log.info('create a bunch of files')
        self.swiftfs._do_write(testFileName, testFileContent)
        for d in testDirectories:
            p = d+testFileName
            self.swiftfs._do_write(p, testFileContent)

    def teardown(self):
        log.info('tidy up directory structure')
        self.swiftfs.rm(testDirectories[0],recursive=True)
        assert_false(self.swiftfs.isdir(testDirectories[0]))
        self.swiftfs.rm(testFileName)
        assert_false(self.swiftfs.isfile(testFileName))
        self.swiftfs.remove_container()

    def test_setup(self):
        log.info('check all directories exist')
        for d in testDirectories:
            assert_true(self.swiftfs.isdir(d))
            assert_false(self.swiftfs.isfile(d))
        log.info('check all test files exist')
        for d in testDirectories:
            p = d+testFileName
            assert_true(self.swiftfs.isfile(p))

    def test_listdir_normalmode(self):
        log.info('check listdir in normal mode')
        for d in testTree:
            results = set()
            for r in self.swiftfs.listdir(d):
                results.add(r['name'])
            assert_set_equal(results,testTree[d])

    def test_listdir_allfiles(self):
        log.info('check listdir returning all files')
        results = set()
        expected = set()
        for r in self.swiftfs.listdir(testDirectories[0],this_dir_only=False):
            results.add(r['name'])
        expected.add(testFileName)
        for d in testDirectories[1:]:
            expected.add(d)
            expected.add(d+testFileName)

    def test_listdir_allroot(self):
        log.info('check listdir returning all files starting at root')
        results = set()
        expected = set()
        for r in self.swiftfs.listdir('/',this_dir_only=False):
            results.add(r['name'])
        expected.add(testFileName)
        for d in testDirectories:
            expected.add(d)
            expected.add(d+testFileName)
        assert_set_equal(results,expected)

    def test_listdir_dirnames(self):
        log.info('check listdir can handle the various variations of directories with slashes')

        for d in ['/temp','/temp/','temp','temp/']:
            results = set()
            for r in self.swiftfs.listdir(d):
                results.add(r['name'])
            assert_set_equal(results,testTree['temp/'])

    def test_copy_file(self):
        log.info('test copying a file')
        fName = testDirectories[0]+testFileName
        cName = fName+'_b'
        assert_false (self.swiftfs.isfile(cName))
        self.swiftfs.cp(fName,cName)
        assert_true (self.swiftfs.isfile(cName))

    def test_delete_file(self):
        log.info('test deleting a file')
        fName = testDirectories[0]+testFileName
        assert_true (self.swiftfs.isfile(fName))
        self.swiftfs.rm(fName)
        assert_false (self.swiftfs.isfile(fName))
        log.info('check deleted file is no longer in directory')
        results = set()
        for r in self.swiftfs.listdir(testDirectories[0]):
            results.add(r['name'])
        assert_not_in(fName,results)

    def test_move_file(self):
        log.info('test moving a file')
        fName = testDirectories[0]+testFileName
        cName = fName+'_b'
        assert_false (self.swiftfs.isfile(cName))
        self.swiftfs.mv(fName,cName)
        assert_true (self.swiftfs.isfile(cName))
        assert_false (self.swiftfs.isfile(fName))

    def test_copy_directory(self):
        log.info('test copying a directory')
        source = 'temp/bar/'
        destination = 'temp/copy_of_bar/'

        expected = set()
        for d in testDirectories:
            expected.add(d)
            expected.add(d+testFileName)
            if d.startswith(source):
                nd = d.replace(source,destination,1)
                expected.add(nd)
                expected.add(nd+testFileName)
        expected.add(testFileName)

        self.swiftfs.cp(source,destination)
        results = set()
        for r in self.swiftfs.listdir('/',this_dir_only=False):
            results.add(r['name'])
        assert_set_equal(results,expected)

    def test_move_directory(self):
        log.info('test moving a directory')
        source = 'temp/bar/'
        destination = 'temp/copy_of_bar/'

        expected = set()
        for d in testDirectories:
            if d.startswith(source):
                nd = d.replace(source,destination,1)
                expected.add(nd)
                expected.add(nd+testFileName)
            else:
                expected.add(d)
                expected.add(d+testFileName)
        expected.add(testFileName)

        self.swiftfs.mv(source,destination)
        results = set()
        for r in self.swiftfs.listdir('/',this_dir_only=False):
            results.add(r['name'])
        assert_set_equal(results,expected)

    def test_delete_directory(self):
        log.info("check that deleting a non-empty directory fails")

        p = 'temp/bar/temp/bar/foo/bar/'
        assert_raises(HTTPError,self.swiftfs.rm,p)
        self.swiftfs.rm(p+testFileName)
        assert_true(self.swiftfs.rm(p))
