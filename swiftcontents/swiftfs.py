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

        # With the python swift client, the connection is automagically
        # created using environment variables (I know... horrible or what?)
        self.log.debug("SwiftContents[SwiftFS] container: `%s`", self.container)

        # Ensure there's a container for this user
        with SwiftService() as swift:
            stat_it = swift.stat( container=self.container )
            if not stat_it["success"]:
                auth = v3.Password(auth_url=os.environ['OS_AUTH_URL'],
                                   username=os.environ['OS_USERNAME'],
                                   password=os.environ['OS_PASSWORD'],
                                   user_domain_name=os.environ['OS_USER_DOMAIN_NAME'],
                                   project_name=os.environ['OS_PROJECT_NAME'],
                                   project_domain_name=os.environ['OS_USER_DOMAIN_NAME'])
                keystone_session = session.Session(auth=auth)
                self.swift_connection = swiftclient.client.Connection(session=keystone_session)
                self.swift_connection.put_container(self.container)

        #if self.prefix:
        #    self.mkdir("")

    # see 'list' at https://docs.openstack.org/developer/python-swiftclient/service-api.html
    # Returns a list of dictionaries
    def listdir(self, path=""):
        self.log.debug("SwiftContents[SwiftFS] Listing directory: `%s`", path)
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
            self.log.error(e.value)
        return files

    def isfile(self, path):
        self.log.debug("SwiftContents[SwiftFS] Checking if `%s` is a file", path)
        if self._check_exists(path):
          if path.endswith('/'):
            return False
          else:
            return True
        else:
          return False

    def isdir(self, path):
        self.log.debug("SwiftContents[SwiftFS] Checking if `%s` is a directory", path)
        if self._check_exists(path):
          if path.endswith('/'):
            return True 
          else:
            return False
        else:
          return False

    def mv(self, old_path, new_path):
        self.cp(old_path, new_path)
        self.rm(old_path)

    def cp(self, old_path, new_path):
        self.log.debug("SwiftContents[SwiftFS] Copy `%s` to `%s`", old_path, new_path)

    def rm(self, path):
        self.log.debug("SwiftContents[SwiftFS] Deleting: `%s`", path)

    def mkdir(self, path):
        self.log.debug("SwiftContents[SwiftFS] Making dir: `%s`", path)

    def read(self, path):
        return path #text

    def write(self, path, content):
        key = self.as_key(path)

    def _check_exists(self, path):
        self.log.debug("SwiftContents[SwiftFS] _check_exists looking for `%s`", path)
        success = True
        if path is None:
          path = 'test/'
        with SwiftService() as swift:
            stat_it = swift.stat( container=self.container, objects=[path] )
            for stat_res in stat_it:
                if stat_res['success']:
                    header_data[stat_res['object']] = stat_res['headers']
                else:
                    self.log.error(
                        'Failed to retrieve stats for %s' % stat_res['object']
                    )
                    success=False
        return success



class SwiftFSError(Exception):
    pass


class NoSuchFile(SwiftFSError):

    def __init__(self, path, *args, **kwargs):
        super(NoSuchFile, self).__init__(*args, **kwargs)
        self.path = path
