import os
import shutil
from pprint import pprint

from swiftcontents.ipycompat import TestContentsManager

from swiftcontents import SwiftContentsManager
from tempfile import TemporaryDirectory
from tornado.web import HTTPError

class SwiftContentsManagerTestCase(TestContentsManager):

    _temp_dir = TemporaryDirectory()

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

    def test_swiftfs_ian(self):
        cm = self.contents_manager
        cm.log.debug("test_swiftfs_ian starting")

        # Some reference data:
        # list of dirs to make
        all_dirs = ['temp', 'temp/bar', 'temp/bar/foo', 'temp/bar/foo/bar']
        # subdirectory for each dir above
        all_dirs_sub = {'temp': 'bar', 'temp/bar': 'foo', 'temp/bar/foo': 'bar', 'temp/bar/foo/bar': ''}
        # name of text file
        filename = 'hello.txt'
        # content of text file
        content = 'Hello world'

        # simple stuff that doesn't touch the filestore
        cm.log.debug("test_swiftfs_ian test guess_type")
        self.assertIs(cm.swiftfs.guess_type('foo.ipynb'), 'notebook')
        self.assertIs(cm.swiftfs.guess_type('foo/'), 'directory')
        self.assertIs(cm.swiftfs.guess_type('foo'), 'file')
        self.assertIs(cm.swiftfs.guess_type('foo/', allow_directory=False), 'file')
        cm.log.debug("test_swiftfs_ian test clean_path")
        self.assertEquals(cm.swiftfs.clean_path('foo/'), 'foo/')
        self.assertEquals(cm.swiftfs.clean_path('foo'), 'foo')
        self.assertEquals(cm.swiftfs.clean_path('foo.ipynb'), 'foo.ipynb')
        self.assertEquals(cm.swiftfs.clean_path('/foo/'), 'foo/')
        self.assertEquals(cm.swiftfs.clean_path('/foo'), 'foo')
        cm.log.debug("test_swiftfs_ian test do_error")
        self.assertRaises(HTTPError, lambda: cm.swiftfs.do_error("Lorem ipsum dolor sit amet, consectetur adipiscing elit"))



        cm.log.debug("test_swiftfs_ian create clean working space")
        files = cm.swiftfs.listdir('temp/', this_dir_only=False)
        for f in files:
            cm.swiftfs.rm( f['name'] )

        # test mkdir
        for _dir in all_dirs:
            cm.log.debug("test_swiftfs_ian test mkdir for '%s'", _dir)
            cm.swiftfs.mkdir(_dir)
        # test isdir & is_file on dirs
        for _dir in all_dirs:
            cm.log.debug("test_swiftfs_ian test isdir & isfile for '%s'", _dir)
            self.assertIs(cm.swiftfs.isdir(_dir), True)
            self.assertIs(cm.swiftfs.isfile(_dir), False)

        # put a file in each directory
        for _dir in all_dirs:
            path = _dir + '/' + filename
            cm.log.debug("test_swiftfs_ian create file '%s'", path)
            cm.swiftfs._do_write(path, content)

        # test isdir & is_file on files
        for _dir in all_dirs:
            path = _dir + '/' + filename
            cm.log.debug("test_swiftfs_ian test isdir & isfile for '%s'", path)
            self.assertIs(cm.swiftfs.isdir(path), False)
            self.assertIs(cm.swiftfs.isfile(path), True)

        # test listdir (normal mode)
        for _dir in all_dirs:
            directory = _dir+ '/'
            files = []
            if all_dirs_sub[_dir]:
                files.append(directory + all_dirs_sub[_dir] + '/')
            files.append(directory + filename)
            files = set(files)
            returns = set(map(lambda x: x['name'], cm.swiftfs.listdir(_dir)))
            cm.log.debug("test_swiftfs_ian test listdir (normal-mode for '%s': should return %s", _dir, files)
            self.assertFalse(files ^ returns) # there should be no difference

        # test listdir in all files mode
        reference = list(map( lambda x: x + '/', all_dirs))
        reference.extend(list(map( lambda x: x + '/' + filename, all_dirs)))
        reference = set(reference)
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir('temp/', this_dir_only=False) ) )
        cm.log.debug("test_swiftfs_ian test listdir (full-mode) for '%s': should return %s", path, reference)
        self.assertFalse(reference ^ returns) # there should be no difference

        # test listdir can cope with a path without the '/' on it
        returns2 = set(map( lambda x: x['name'], cm.swiftfs.listdir('temp', this_dir_only=False) ) )

        self.assertFalse(returns2 ^ returns) # there should be no difference

        # copying a file
        from_path = all_dirs[-1] + '/' + filename
        to_path = all_dirs[-1] + '/' + filename + '_b'
        cm.log.debug("test_swiftfs_ian test copying '%s' to '%s'", from_path, to_path)
        cm.swiftfs.cp(from_path, to_path)
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir(all_dirs[-1], this_dir_only=False) ) )
        self.assertTrue(from_path in returns)
        self.assertTrue(to_path in returns)

        # and removing a file
        cm.log.debug("test_swiftfs_ian test deleting '%s'", to_path)
        cm.swiftfs.rm(to_path)
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir(all_dirs[-1], this_dir_only=False) ) )
        self.assertTrue(from_path in returns)
        self.assertFalse(to_path in returns)

        # now lets try moving a file
        cm.log.debug("test_swiftfs_ian test move '%s' to '%s'", from_path, to_path)
        cm.swiftfs.mv(from_path, to_path)
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir(all_dirs[-1], this_dir_only=False) ) )
        self.assertFalse(from_path in returns)
        self.assertTrue(to_path in returns)

        # .... and back again
        cm.log.debug("test_swiftfs_ian test move '%s' to '%s'", to_path, from_path)
        cm.swiftfs.mv(to_path, from_path)
        returns = set(map( lambda x: x['name'], cm.swiftfs.listdir(all_dirs[-1], this_dir_only=False) ) )
        self.assertFalse(to_path in returns)
        self.assertTrue(from_path in returns)

        # Does making a file some way down a tree make all the intermediate "directories"?
        # (first we need to get rid of the existing temp tree)
        #files = cm.swiftfs.listdir('temp/')
        #for f in files:
        #    cm.delete_file( f['name'] )


        # bodge to force an error
        self.assertIs('True', False)
        cm.log.debug("test_swiftfs_ian ending")

    def test_rename_ian(self):
        cm = self.contents_manager
        cm.log.debug("test_rename_ian creating new notebook")
        # Create a new notebook
        nb, name, path = self.new_notebook()

        # Rename the notebook
        cm.log.debug("test_rename_ian renaming said notebook")
        cm.rename(path, "changed_path")

        # Attempting to get the notebook under the old name raises an error
        cm.log.debug("test_rename_ian renaming said notebook (should work)")
        self.assertRaises(HTTPError, cm.get, path)
        # Fetching the notebook under the new name is successful
        assert isinstance(cm.get("changed_path"), dict)

        # Ported tests on nested directory renaming from pgcontents
        all_dirs = ['foo', 'bar', 'foo/bar', 'foo/bar/foo', 'foo/bar/foo/bar']
        unchanged_dirs = all_dirs[:2]
        changed_dirs = all_dirs[2:]

        for _dir in all_dirs:
            cm.log.debug("test_rename_ian creating new directory '%s'" % _dir)
            self.make_populated_dir(_dir)
            cm.log.debug("test_rename_ian checking new directory '%s'" % _dir)
            self.check_populated_dir_files(_dir)
            cm.log.debug("test_rename_ian onto next directory?")

        # Renaming to an existing directory should fail
        for src, dest in combinations(all_dirs, 2):
            cm.log.debug("test_rename_ian Renaming to an existing directory should fail '%s' -> '%s'", src, dest)
            with self.assertRaisesHTTPError(409):
                cm.rename(src, dest)

        # Creating a notebook in a non_existant directory should fail
        cm.log.debug("test_rename_ian Creating a notebook in a non_existant directory should fail (foo/bar_diff)")
        with self.assertRaisesHTTPError(404):
            cm.new_untitled("foo/bar_diff", ext=".ipynb")

        cm.log.debug("test_rename_ian rename foo/bar to foo/bar_diff")
        cm.rename("foo/bar", "foo/bar_diff")

        # Assert that unchanged directories remain so
        for unchanged in unchanged_dirs:
            cm.log.debug("test_rename_ian check files in dir '%s'", unchanged)
            self.check_populated_dir_files(unchanged)

        # Assert changed directories can no longer be accessed under old names
        for changed_dirname in changed_dirs:
            cm.log.debug("test_rename_ian check files in dir '%s' (should fail)", unchanged)
            with self.assertRaisesHTTPError(404):
                cm.get(changed_dirname)

            new_dirname = changed_dirname.replace("foo/bar", "foo/bar_diff", 1)

            cm.log.debug("test_rename_ian check files in dir '%s' (should work)", new_dirname)
            self.check_populated_dir_files(new_dirname)

        # Created a notebook in the renamed directory should work
        cm.log.debug("test_rename_ian create notebook in foo/bar_diff should work")
        cm.new_untitled("foo/bar_diff", ext=".ipynb")    
        cm.log.debug("test_rename_ian test_ends")


    def check_populated_dir_file_s(self, api_path):
        dir_model = self.contents_manager.get(api_path)
        self.assertEqual(dir_model['path'], api_path)
        self.assertEqual(dir_model['type'], "directory")

        self.contents_manager.log.debug("check_populated_dir_files was given path '%s'", api_path)
        self.contents_manager.log.debug("check_populated_dir_files looping over content")
        for entry in dir_model['content']:
            self.contents_manager.log.debug("this_entry: %s" % dir_model)
            if entry['type'] == "directory":
                self.contents_manager.log.debug("check_populated_dir_files entry type is 'directory', ignoring")
                continue
            elif entry['type'] == "file":
                self.contents_manager.log.debug("check_populated_dir_files comparing file: %s <-> %s", entry['name'], "file.txt")
                self.assertEqual(entry['name'], "file.txt")
                complete_path = "/".join([api_path, "file.txt"])
                self.contents_manager.log.debug("check_populated_dir_files comparing file: %s <-> %s", entry['path'], complete_path)
                self.assertEqual(entry["path"], complete_path)

            elif entry['type'] == "notebook":
                self.contents_manager.log.debug("check_populated_dir_files comparing notebook: %s <-> %s", entry['name'], "nb.ipynb")
                self.assertEqual(entry['name'], "nb.ipynb")
                complete_path = "/".join([api_path, "nb.ipynb"])
                self.contents_manager.log.debug("check_populated_dir_files comparing notebook: %s <-> %s", entry['path'], complete_path)
                self.assertEqual(entry["path"], complete_path)

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
