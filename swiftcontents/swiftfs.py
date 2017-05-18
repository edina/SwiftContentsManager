"""
Utilities to make Swift look like a regular file system
"""
import six
import os
import swiftclient
from swiftclient.service import SwiftService, SwiftError
from keystoneauth1 import session
from keystoneauth1.identity import v3
from traitlets import default, HasTraits, Unicode, Any, Instance
from pprint import pprint

class SwiftFS(HasTraits):

    container = Unicode(os.environ.get('CONTAINER', 'demo'))
    storage_url = Unicode(
        help="The base URL for containers",
        default_value='http://example.com').tag(
          config = True
        )

    swift_connection = Instance(
        klass = 'swiftclient.client.Connection'
        )

    delimiter = Unicode("/", help="Path delimiter", config=True)

    root_dir = Unicode("./", config=True)

    def __init__(self, log, **kwargs):
        super(SwiftFS, self).__init__(**kwargs)
        self.log = log
        self.log.debug("SwiftContents[SwiftFS] container: `%s`", self.container)

        # With the python swift client, the connection is automagically
        # created using environment variables (I know... horrible or what?)

        # Ensure there's a container for this user
        with SwiftService() as swift:
          try:
            stat_it = swift.stat( container=self.container )
            if stat_it["success"]:
              self.log.debug("SwiftContents[SwiftFS] container `%s` exists", self.container)
          except SwiftError as e:
            self.log.error("SwiftFS.listdir %s", e.value)
            auth = v3.Password(auth_url=os.environ['OS_AUTH_URL'],
                               username=os.environ['OS_USERNAME'],
                               password=os.environ['OS_PASSWORD'],
                               user_domain_name=os.environ['OS_USER_DOMAIN_NAME'],
                               project_name=os.environ['OS_PROJECT_NAME'],
                               project_domain_name=os.environ['OS_USER_DOMAIN_NAME'])
            keystone_session = session.Session(auth=auth)
            self.swift_connection = swiftclient.client.Connection(session=keystone_session)
            self.log.debug("SwiftContents[SwiftFS] container `%s` does not exist, making it", self.container)
            self.swift_connection.put_container(self.container)
            self.log.debug("SwiftContents[SwiftFS] container `%s` made", self.container)

        #if self.prefix:
        #    self.mkdir("")

    # see 'list' at https://docs.openstack.org/developer/python-swiftclient/service-api.html
    # Returns a list of dictionaries
    def listdir(self, path=""):
        self.log.debug("SwiftFS.listdir Listing directory: `%s`", path)
        files = []
        with SwiftService() as swift:
          try:
            dir_listing = swift.list(container=container, prefix=path)
            for page in dir_listing:  # each page is up to 10,000 items
              if page["success"]:
                files.append( page["listing"] )
              else:
                raise page["error"]
          except SwiftError as e:
            self.log.error("SwiftFS.listdir %s", e.value)
        return files

    # We can 'stat' files, but not directories
    def isfile(self, path):
        self.log.debug("SwiftFS.isfile Checking if `%s` is a file", path)
        with SwiftService() as swift:
          try:
            stat_it = swift.stat( container=self.container, objects=[path] )
            for stat_res in stat_it:
              if stat_res['success']:
                return True
              else:
                self.log.error(
                  'Failed to retrieve stats for %s' % stat_res['object']
                )
                return False
          except SwiftError as e:
            self.log.error("SwiftFS.isfile %s", e.value)
        return False

    # We can 'list' direcotries, but not 'stat' them
    def isdir(self, path):
        self.log.debug("SwiftFS.isdir Checking if `%s` is a directory", path)

        # Root directory checks
        if path == "":  # effectively root directory
          return True
        if not path.endswith(self.delimiter):
            path = path + self.delimiter
        if path == "":
            return True

        count = 0
        with SwiftService() as swift:
          try:
            options = { 'prefix' : path }
            response = swift.list( container=self.container, options=options )
            for page in response:
              if page['success']:
                count = 1
                pprint(page['listing'])
          except SwiftError as e:
            self.log.error("SwiftFS.isdir %s", e.value)
        return count



    def mv(self, old_path, new_path):
        self.cp(old_path, new_path)
        self.rm(old_path)

    def cp(self, old_path, new_path):
        self.log.debug("SwiftFS.copy `%s` to `%s`", old_path, new_path)

    def rm(self, path):
        self.log.debug("SwiftFS.rm `%s`", path)

    def mkdir(self, path):
        self.log.debug("SwiftFS.mkdir `%s`", path)

    def read(self, path):
        self.log.debug("SwiftFS.read `%s`", path)
        return 'Hello world'

    def write(self, path, content):
        self.log.debug("SwiftFS.write `%s`", path)
        


class SwiftFSError(Exception):
    pass


class NoSuchFile(SwiftFSError):

    def __init__(self, path, *args, **kwargs):
        super(NoSuchFile, self).__init__(*args, **kwargs)
        self.path = path
