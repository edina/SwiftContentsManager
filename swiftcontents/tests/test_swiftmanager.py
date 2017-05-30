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
        #    self.contents_manager.delete_file( g['name'] )
            pass

    def teardown_class(self):
        # Delete files from the local file-store    
        files = os.listdir('/tmp')
        for f in files:
          if ( (f != self._temp_dir) and (f != 'å b') ):
        #    shutil.rmtree(f)
            pass

# This needs to be removed or else we'll run the main IPython tests as well.
del TestContentsManager
