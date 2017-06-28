import os
import shutil
from pprint import pprint

from swiftcontents.ipycompat import TestContentsManager

from swiftcontents import SwiftContentsManager
from tempfile import TemporaryDirectory
from tornado.web import HTTPError

# Basic concept:
# Each file is an object
# Each directory is an object
# Each object's "path" is the full directory-path for that object (thus faking
#   a directory structure
# When an object is created, we ensure all intermediate directory objects exist
#   and if they don't, we create them
# When we delete an object, we ensure that that path lists only one object (ie
#   that it is a file, or an empty directory
# When we rename a file, we can rename that file
# When we rename a directory, we need to rename every object that contains that
#   path-part
class Test_SwiftContentsManager(TestContentsManager):

    _temp_dir = TemporaryDirectory()

    def setUp(self):
        self.contents_manager = SwiftContentsManager()

    def tearDown(self):
        pass

    def make_dir(self, api_path):
        self.contents_manager.make_dir(api_path)

# This needs to be removed or else we'll run the main IPython tests as well.
del TestContentsManager
