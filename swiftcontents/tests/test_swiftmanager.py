import os

from swiftcontents.ipycompat import TestContentsManager

from swiftcontents import SwiftContentsManager

class SwiftContentsManagerTestCase(TestContentsManager):

    def setUp(self):
        """
        Note: this test requires a bunch of environment variables set, and
        is written to work in a Docker image, against the UofE 'Horizon' server
        """
        print( "OS_AUTH_URL is %S" , os.environ['OS_AUTH_URL'])
        print( "OS_USERNAME is %S" , os.environ['OS_USERNAME'])
        print( "OS_PASSWORD is %S" , os.environ['OS_PASSWORD'])
        print( "OS_USER_DOMAIN_NAME is %S" , os.environ['OS_USER_DOMAIN_NAME'])
        print( "OS_PROJECT_NAME is %S" , os.environ['OS_PROJECT_NAME'])
        self.contents_manager = SwiftContentsManager()

    def tearDown(self):
        pass
    # Overwrites from TestContentsManager

    #def make_dir(self, api_path):
        #self.contents_manager.new(
        #    model={"type": "directory"},
        #    path=api_path,)


# This needs to be removed or else we'll run the main IPython tests as well.
del TestContentsManager
