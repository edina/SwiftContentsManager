
from swiftcontents.ipycompat import TestContentsManager

from swiftcontents import SwiftContentsManager


class SwiftContentsManagerTestCase(TestContentsManager):

    def setUp(self):
        """
        Note: this test requires a bunch of environment variables set, and
        is written to work in a Docker image, against the UofE 'Horizon' server
        """
        self.contents_manager = SwiftContentsManager()

    def tearDown(self):
        return True
    # Overwrites from TestContentsManager

    def make_dir(self, api_path):
        self.contents_manager.new(
            model={"type": "directory"},
            path=api_path,)


# This needs to be removed or else we'll run the main IPython tests as well.
del TestContentsManager
