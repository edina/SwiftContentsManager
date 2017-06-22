import os
import shutil
from pprint import pprint

from swiftcontents.ipycompat import TestContentsManager

from swiftcontents import SwiftContentsManager
from tempfile import TemporaryDirectory
from tornado.web import HTTPError

class SwiftContentsManagerTestCase(TestContentsManager):

    _temp_dir = TemporaryDirectory()

    # Some reference data:
    # list of dirs to make
    all_dirs = ['temp/',
                'temp/bar/',
                'temp/bar/temp/',
                'temp/bar/temp/bar/',
                'temp/bar/temp/bar/foo/',
                'temp/bar/temp/bar/foo/bar/']
    # subdirectory for each dir above
    all_dirs_sub = {'temp/': 'bar/',
                    'temp/bar/': 'temp/',
                    'temp/bar/temp/': 'bar/',
                    'temp/bar/temp/bar/': 'foo/',
                    'temp/bar/temp/bar/foo/': 'bar/',
                    'temp/bar/temp/bar/foo/bar/': ''}
    # name of text file
    test_filename = 'hello.txt'
    # content of text file
    content = 'Hello world'

    def setUp(self):
        """
        Note: this test requires a bunch of environment variables set, and
        is written to work in a Docker image, against the UofE 'Horizon' server
        """
        self.contents_manager = SwiftContentsManager()

    def tearDown(self):
        # Delete objects from the store
        files = self.contents_manager.swiftfs.listdir()
        for f in files:
          for g in f:
            pass #self.contents_manager.delete_file( g['name'] )

        # Delete files from the local file-store
        files = os.listdir('/tmp')
        for f in files:
          if f != self._temp_dir:
            pass #shutil.rmtree(f)

        self.contents_manager.log.info("--------------------------------------")

    # clear all the objects that include 'path' in the "filename"
    def clear_store(self, path):
        cm = self.contents_manager
        cm.log.info("test suite create clean working space")
        cm.swiftfs.rm( path, recursive=True )

    # Build the data as defined in self.all_dirs et al
    def make_data(self):
        cm = self.contents_manager
        cm.log.info("test suite make_data")

        # test mkdir
        #for _dir in self.all_dirs:
        #    cm.log.info("test_swiftfs test mkdir for '%s'", _dir)
        #    cm.swiftfs.mkdir(_dir)
        path = self.all_dirs[-1]
        cm.log.info("test_swiftfs test mkdir for '%s'", path)
        cm.swiftfs.mkdir(path)

        # put a file in each directory
        for _dir in self.all_dirs:
            path = _dir + self.test_filename
            cm.log.info("test_swiftfs create file '%s'", path)
            cm.swiftfs._do_write(path, self.content)

    # returns all the directories & files created by self.made_data
    def get_list_of_files(self):
        files = set()
        for _dir in self.all_dirs:
            if self.all_dirs_sub[_dir]:
                files.add(_dir + self.all_dirs_sub[_dir])
            files.add(_dir + self.test_filename)
        return files

    def test_local_clear_store(self):
        self.make_data()
        self.clear_store(self.all_dirs[0])
        raise Exception

    # simple stuff that doesn't touch the filestore
    def test_swiftfs_nonfs(self):
        cm = self.contents_manager
        cm.log.info("test_swiftfs starting")

        cm.log.info("test_swiftfs test guess_type")
        self.assertIs(cm.swiftfs.guess_type('foo.ipynb'), 'notebook')
        self.assertIs(cm.swiftfs.guess_type('foo/'), 'directory')
        self.assertIs(cm.swiftfs.guess_type('foo'), 'file')
        self.assertIs(cm.swiftfs.guess_type('foo/', allow_directory=False), 'file')
        cm.log.info("test_swiftfs test clean_path")
        self.assertEquals(cm.swiftfs.clean_path('foo/'), 'foo/')
        self.assertEquals(cm.swiftfs.clean_path('foo'), 'foo')
        self.assertEquals(cm.swiftfs.clean_path('foo.ipynb'), 'foo.ipynb')
        self.assertEquals(cm.swiftfs.clean_path('/foo/'), 'foo/')
        self.assertEquals(cm.swiftfs.clean_path('/foo'), 'foo')
        cm.log.info("test_swiftfs test do_error")
        self.assertRaises(HTTPError, lambda: cm.swiftfs.do_error("Lorem ipsum dolor sit amet, consectetur adipiscing elit"))

    # Test dirs are dirs, and files are files, and listdir works as it should
    def test_swiftfs_istests(self):
        cm = self.contents_manager
        self.clear_store(self.all_dirs[0])
        self.make_data()

        # test isdir & is_file on dirs
        for _dir in self.all_dirs:
            cm.log.info("test_swiftfs test isdir & isfile for '%s'", _dir)
            self.assertIs(cm.swiftfs.isdir(_dir), True)
            self.assertIs(cm.swiftfs.isfile(_dir), False)

        # test isdir & is_file on files
        for _dir in self.all_dirs:
            path = _dir + self.test_filename
            cm.log.info("test_swiftfs test isdir & isfile for '%s'", path)
            self.assertIs(cm.swiftfs.isdir(path), False)
            self.assertIs(cm.swiftfs.isfile(path), True)

        # test listdir (normal mode)
        # gets just the sub-dir & the data-file
        for _dir in self.all_dirs:
            files = set()
            if self.all_dirs_sub[_dir]:
                files.add(_dir + self.all_dirs_sub[_dir])
            files.add(_dir + self.test_filename)
            cm.log.info("test_swiftfs test listdir (normal-mode for '%s': should return %s", _dir, files)
            returns = set(map(lambda x: x['name'], cm.swiftfs.listdir(_dir)))
            self.assertFalse(files ^ returns) # there should be no difference

        # test listdir in all files mode
        path = self.all_dirs[0]
        reference = list(map( lambda x: x, self.all_dirs))
        reference.extend(list(map( lambda x: x + self.test_filename, self.all_dirs)))
        reference = set(reference)
        cm.log.info("test_swiftfs test listdir (full-mode) for '%s': should return %s", path, reference)
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir(path, this_dir_only=False) ) )
        self.assertFalse(reference ^ returns) # there should be no difference

        # test listdir can cope with a path without the '/' on it
        returns2 = set(map( lambda x: x['name'], cm.swiftfs.listdir('temp', this_dir_only=False) ) )
        self.assertFalse(returns2 ^ returns) # there should be no difference

        # test listdir copes with root dir
        path = ''
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir(path) ) )
        self.assertTrue(len(returns) > 0)

        self.clear_store(self.all_dirs[0])

    # rename a directory with known sub-objects
    def test_swiftfs_manipulate_files(self):
        cm = self.contents_manager
        self.clear_store(self.all_dirs[0])
        self.make_data()

        # copying a file
        from_path = self.all_dirs[-1] + self.test_filename
        to_path = self.all_dirs[-1] + self.test_filename + '_b'
        cm.log.info("test_swiftfs test copying '%s' to '%s'", from_path, to_path)
        cm.swiftfs.cp(from_path, to_path)
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir(self.all_dirs[-1]) ) )
        self.assertTrue(from_path in returns)
        self.assertTrue(to_path in returns)

        # and removing a file
        cm.log.info("test_swiftfs test deleting '%s'", to_path)
        cm.swiftfs.rm(to_path)
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir(self.all_dirs[-1]) ) )
        self.assertTrue(from_path in returns)
        self.assertFalse(to_path in returns)

        # now lets try moving a file
        cm.log.info("test_swiftfs test move '%s' to '%s'", from_path, to_path)
        cm.swiftfs.mv(from_path, to_path)
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir(self.all_dirs[-1]) ) )
        self.assertFalse(from_path in returns)
        self.assertTrue(to_path in returns)

        # .... and back again
        cm.log.info("test_swiftfs test move '%s' to '%s'", to_path, from_path)
        cm.swiftfs.mv(to_path, from_path)
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir(self.all_dirs[-1]) ) )
        self.assertFalse(to_path in returns)
        self.assertTrue(from_path in returns)

        # Now if we delete the single file, the dir should be empty
        cm.swiftfs.rm(from_path)
        cm.log.info("listdir looking no files in " + self.all_dirs[-1])
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir(self.all_dirs[-1])))

        self.clear_store(self.all_dirs[0])
        self.assertFalse(len(returns))

    def test_swiftfs_rename_dir(self):
        cm = self.contents_manager
        self.clear_store(self.all_dirs[0])
        self.make_data()
        from_path = self.all_dirs[1]
        to_path = self.all_dirs[1]
        to_path = to_path.rstrip('/') + 'l/'

        # test mv_dir renames a directory, and all sub-objects
        cm.log.info("test_swiftfs move " + from_path  + " to " + to_path + ": should modify all 'sub-directory' objects")
        expected = set()
        for f in self.get_list_of_files():
            if f.startswith(from_path) and f != from_path:
                expected.add(f.replace(from_path, to_path, 1))
        cm.swiftfs.mv(from_path, to_path)
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir(to_path, this_dir_only=False) ) )
        self.assertFalse(expected ^ returns) # there should be no difference
        self.assertFalse(cm.swiftfs.listdir(from_path)) # Old dir has disappeared

        self.clear_store(self.all_dirs[0])
        raise Exception

    # does deleting dirs handle non-empty dirs properly
    def test_swiftfs_deletes(self):
        cm = self.contents_manager
        self.clear_store(self.all_dirs[0])
        self.make_data()

        path = self.all_dirs[-1]
        self.assertFalse(cm.swiftfs.rm(path))
        self.assertTrue(cm.swiftfs.rm(path + self.test_filename) is None)
        self.assertTrue(cm.swiftfs.rm(path) is None)

        self.clear_store(self.all_dirs[0])

    # Does making a file some way down a tree make all the intermediate "directories"?
    # (first we need to get rid of the existing temp tree)
    def test_swiftfs_tree_test(self):
        cm = self.contents_manager
        self.clear_store(self.all_dirs[0])
        from_path = self.all_dirs[-1] + self.test_filename

        cm.log.info("test_swiftfs create '%s' (and all intermediate sub_dirs)", from_path)
        cm.swiftfs.write(from_path, self.content)

        files = cm.swiftfs.listdir(self.all_dirs[1])
        self.assertTrue(list(files)[0]['name'] == self.all_dirs[1] + all_dirs_sub[self.all_dirs[1]] )

        self.clear_store(self.all_dirs[0])

    # Test the central error-handler
    def test_swiftmanager_do_error(self):
        cm = self.contents_manager
        cm.log.info("test_swiftmanager_do_error starting")
        self.assertRaises(HTTPError, lambda: cm.do_error('error_text', 501))
        # really? Oh, OK then...
        self.assertRaises(HTTPError, lambda: cm.do_error('error_text', 999))

    # make the swiftfs test tree, then test it
    def test_swiftmanager_dir_exists(self):
        cm = self.contents_manager
        cm.log.info("test_swiftmanager_exists starting")
        self.clear_store(self.all_dirs[0])
        self.make_data()
        exists_dir = self.all_dirs[-1]
        exists_file = exists_dir + self.test_filename
        self.assertTrue(cm.dir_exists(exists_dir))
        self.assertFalse(cm.dir_exists(exists_file))
        self.clear_store(self.all_dirs[0])

    # make the swiftfs test tree, then test it
    def test_swiftmanager_file_exists(self):
        cm = self.contents_manager
        cm.log.info("test_swiftmanager_exists starting")
        self.clear_store(self.all_dirs[0])
        self.make_data()
        exists_dir = self.all_dirs[-1]
        exists_file = exists_dir + self.test_filename
        self.assertTrue(cm.file_exists(exists_file))
        self.assertFalse(cm.dir_exists(exists_file))
        self.clear_store(self.all_dirs[0])

    # is_hidden is hard-coded to be false
    def test_swiftmanager_dir_exists(self):
        cm = self.contents_manager
        cm.log.info("test_swiftmanager_exists starting")
        self.clear_store(self.all_dirs[0])
        self.make_data()
        exists_dir = self.all_dirs[-1]
        exists_file = exists_dir + self.test_filename
        self.assertFalse(cm.is_hidden(exists_file))
        self.assertFalse(cm.is_hidden(exists_dir))
        self.clear_store(self.all_dirs[0])

    # tests make_dir (both success & fail)
    def test_swiftmanager_make_dir(self):
        cm = self.contents_manager
        cm.log.info("test_swiftmanager_make_dir starting")
        path = self.all_dirs[0]
        self.clear_store(path)
        cm.make_dir(path)
        files = cm.swiftfs.listdir()
        success=False
        for f in files:
            if f['name'] == path:
                success = True
                break
        self.assertTrue(success)
        # can't make a directory if it exists aleady
        self.assertRaises(HTTPError, lambda: cm.make_dir(path))
        self.clear_store(path)

    # fails on an unknown type
    def test_swiftmanager_get_unknown(self):
        cm = self.contents_manager
        cm.log.info("test_swiftmanager_get_unknown starting")
        path = self.all_dirs[0]
        self.assertRaises(HTTPError, lambda: cm.get(path, type='random_text', content=None) )

    # make a file through swiftFS, then get it using the higher-level function
    def test_swiftmanager_get_directory(self):
        cm = self.contents_manager
        cm.log.info("test_swiftmanager_get starting")
        path = '' # self.all_dirs[0]
        #cm.log.info("test_swiftfs create file '%s'", path)
        #cm.swiftfs.mkdir(path)
        data = cm.get(path, type='directory', content=False)
        self.assertTrue( data['content'] == None)
        data = cm.get(path, type='directory', content=True)
        self.assertTrue( len(data['content']) > 0)
        raise Exception

    def test_swiftmanager_save(self):
        cm = self.contents_manager
        cm.log.info("test_swiftmanager_save starting")
        pass
    def test_swiftmanager_rename(self):
        cm = self.contents_manager
        cm.log.info("test_swiftmanager_rename starting")
        pass
    def test_swiftmanager_delete(self):
        cm = self.contents_manager
        cm.log.info("test_swiftmanager_delete starting")
        pass
    def test_swiftmanager_error_handling(self):
        cm = self.contents_manager
        cm.log.info("test_swiftmanager_error_handling starting")
        pass



    def assertRaisesHTTPError(self, status, msg=None):
        msg = msg or "Ian's own Should have raised HTTPError(%i)" % status
        try:
            yield
        except HTTPError as e:
            self.assertEqual(e.status_code, status)
        else:
            self.fail(msg)

# This needs to be removed or else we'll run the main IPython tests as well.
del TestContentsManager
