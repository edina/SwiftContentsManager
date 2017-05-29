import os
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


    # NOTE This is specifically leaving a file behind whilst I test shit
    def tearDown_class(self):
        files = self.contents_manager.swiftfs.listdir()
        for f in files:
          for g in f:
            self.contents_manager.delete_file( g['name'] )
    # Overwrites from TestContentsManager

    #def make_dir(self, api_path):
        #self.contents_manager.new(
        #    model={"type": "directory"},
        #    path=api_path,)


# This needs to be removed or else we'll run the main IPython tests as well.
del TestContentsManager
