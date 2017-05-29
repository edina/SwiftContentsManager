"""
Utilities to make Swift look like a regular file system
"""
import six
import os
import swiftclient
import io
import re
from swiftclient.service import SwiftService, SwiftError, SwiftCopyObject, SwiftUploadObject
from swiftclient.multithreading import OutputManager
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

    root_dir = Unicode("/", config=True)

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
        path = self.clean_path(path)
        with SwiftService() as swift:
          try:
            _opts = {'prefix' : path}
            dir_listing = swift.list(container=self.container, options = _opts )
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
        path = self.clean_path(path)

        self.log.debug("SwiftFS.isfile path truncated to `%s`", path)
        with SwiftService() as swift:
          try:
            stat_it = swift.stat( container=self.container, objects=[path] )
            for stat_res in stat_it:
              if stat_res['success']:
                self.log.debug("SwiftFS.isfile returning True")
                return True
              else:
                self.log.error(
                  'Failed to retrieve stats for %s' % stat_res['object']
                )
                self.log.debug("SwiftFS.isfile returning False")
                return False
          except SwiftError as e:
            self.log.error("SwiftFS.isfile %s", e.value)
        self.log.debug("SwiftFS.isfile returning False")
        return False

    # We can 'list' direcotries, but not 'stat' them
    def isdir(self, path):
        self.log.debug("SwiftFS.isdir Checking if `%s` is a directory", path)
        path = self.clean_path(path)

        # directories mush have a trailing slash on them.
        # The core code seems to remove any trailing slash, so lets add it back on
        path = path.rstrip( self.delimiter)
        path = path + self.delimiter

        # Root directory checks
        if path == self.delimiter:  # effectively root directory
          self.log.debug("SwiftFS.isdir found root dir - returning True")
          return True

        count = 0
        with SwiftService() as swift:
          try:
            _opts = {}
            if re.search('\w', path):
              _opts = { 'prefix' : path }
            response = swift.list( container=self.container, options=_opts )
            for r in response:
              if r['success']:
                count = 1
          except SwiftError as e:
            self.log.error("SwiftFS.isdir %s", e.value)
        self.log.debug("SwiftFS.isdir returning the number '%s'", count)
        return count



    def mv(self, old_path, new_path):
        self.cp(old_path, new_path)
        self.rm(old_path)

    def cp(self, old_path, new_path):
        self.log.debug("SwiftFS.copy `%s` to `%s`", old_path, new_path)
        old_path = self.clean_path(old_path)
        new_path = self.clean_path(new_path)
        with SwiftService() as swift:
          try:
            _obj = SwiftCopyObject(old_path, 
                                   {"Destination": self.delimiter + self.container + new_path})
            for i in swift.copy(
                self.container,
                [_obj]):
              if i["success"]:
                if i["action"] == "copy_object":
                    self.log.debug(
                        "object %s copied from /%s/%s" %
                        (i["destination"], i["container"], i["object"])
                    )
                if i["action"] == "create_container":
                    self.log.debug(
                        "container %s created" % i["container"]
                    )
              else:
                if "error" in i and isinstance(i["error"], Exception):
                    raise i["error"]
          except SwiftError as e:
            logger.error(e.value)

    def rm(self, path):
        self.log.debug("SwiftFS.rm `%s`", path)
        path = self.clean_path(path)

        if path in ["", self.delimiter]:
          self.do_error('Cannot delete root directory') 

        with SwiftService() as swift:
          try:
            response = swift.delete( container=self.container, objects=[path] )
            for r in response:
              self.log.debug("SwiftFS.rm action: `%s` success: `%s`", r['action'], r['success'])
          except SwiftError as e:
            self.log.error("SwiftFS.rm %s", e.value)


    # Directories are just objects that have a trailing '/'
    def mkdir(self, path):
        self.log.debug("SwiftFS.mkdir `%s`", path)
        path = self.clean_path(path)
        path = path.rstrip( self.delimiter )
        path = path + self.delimiter
        self.write(path, None)


    ## This works by downloading the file to disk then reading the contents of
    ## that file into memory, before deleting the file
    ## NOTE this is reading text files!
    ## NOTE this really only works with files in the local direcotry, but given local filestore will disappear when the docker ends, I'm not too bothered.
    def read(self, path):
        self.log.debug("SwiftFS.read `%s`", path)
        path = self.clean_path(path)
        content = ''
        with SwiftService() as swift:
          try:
            response = swift.download(container=self.container, objects=[path])
            for r in response:
              if r['success']:
                filename = open( r['path'] )
                content = filename.read()
                os.remove( r['path'] )
          except SwiftError as e:
            self.log.error("SwiftFS.read %s", e.value)
        return content 

    # Write is 'upload' and 'upload' needs a "file" it can read from
    # We use io.StringIO for this
    def write(self, path, content):
        self.log.debug("SwiftFS.write `%s`", path)
        path = self.clean_path(path)
        _opts = {'object_uu_threads' : 20}
        with SwiftService( options = _opts ) as swift, OutputManager() as out_manager:
          try:
            type = self.guess_type(path)
            things = []
            if type == "directory":
              self.log.debug("SwiftFS.write create directory")
              things.append( SwiftUploadObject(None, object_name=path) )
            else: 
              self.log.debug("SwiftFS.write create file/notebook from '%s'", content)
              output = io.BytesIO( content.encode('utf-8') )
              things.append( SwiftUploadObject( output, object_name=path) )

            # Now do the upload
            response = swift.upload(self.container, things)
            for r in response:
              self.log.debug("SwiftFS.write action: '%s', response: '%s'", r['action'], r['success'])
          except SwiftError as e:
            self.log.error("SwiftFS.write swift-error: %s", e.value)
          except ClientException as e:
            self.log.error("SwiftFS.write client-error: %s", e.value)

    def guess_type(self, path, allow_directory=True):
        """
        Guess the type of a file.
        If allow_directory is False, don't consider the possibility that the
        file is a directory.

        Parameters
        ----------
            obj: s3.Object or string
        """
        if path.endswith(".ipynb"):
            return "notebook"
        elif allow_directory and self.dir_exists(path):
            return "directory"
        else:
            return "file"

    def clean_path(self, path):
        # strip of any leading '/'
        path = path.lstrip( self.delimiter )
        #path = self.delimiter + path
        return path

    def do_error(self, msg, code=500):
        raise HTTPError(code, msg)

class SwiftFSError(Exception):
    def do_error(self, msg, code=500):
        raise HTTPError(code, msg)
    pass


class NoSuchFile(SwiftFSError):

    def __init__(self, path, *args, **kwargs):
        super(NoSuchFile, self).__init__(*args, **kwargs)
        self.path = path
