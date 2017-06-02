import os
import shutil
from pprint import pprint

from swiftcontents.ipycompat import TestContentsManager

from swiftcontents import SwiftContentsManager
from tempfile import TemporaryDirectory

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

    def check_populated_dir_files(self, api_path):
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

# This needs to be removed or else we'll run the main IPython tests as well.
del TestContentsManager
