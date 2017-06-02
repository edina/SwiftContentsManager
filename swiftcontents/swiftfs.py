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
from tornado.web import HTTPError
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
    # Returns a list of all objects that start with the prefix given
    # Of course, in a proper heirarchical file-system, list-dir only returns the files
    # in that dir, so we need to filter the list to me ONLY those objects where the
    # 'heirarchical' bit of the name stops at the path given
    # The method has 2 modes: 1 when the list of names is returned with the full
    # path-name, and one where the name is just the "file name"
    def listdir(self, path="", with_prefix=False):
        self.log.debug("SwiftFS.listdir Listing directory: `%s`", path)
        files = []
        path = self.clean_path(path)

        # Get all objects that match the known path
        with SwiftService() as swift:
          try:
            _opts = {'prefix' : path}
            dir_listing = swift.list(container=self.container, options = _opts )
            for page in dir_listing:  # each page is up to 10,000 items
              if page["success"]:
                files.extend( page["listing"] )
              else:
                raise page["error"]
          except SwiftError as e:
            self.log.error("SwiftFS.listdir %s", e.value)

        # So - finding all the files in the given "directory"
        # 1) get the count of "levels" in the heirarchy [foo/bar is /foo/bar/, and is 3]
        # 2) for each file found, remove any trailing slash, and then count the number of levels
        #    - if they match, it counts.

# idea 2: for every "name", lstrip "path", then split on delimiter.
# if list[1] is not None, add it to the list of returned files


        self.log.debug("SwiftFS.listdir path: `%s`", path)
        #if path is None or path == '':
        #  path_count = 1
        #else:
        #  path_list = re.split( self.delimiter, path.rstrip( self.delimiter ) + self.delimiter)
        #  path_count = len( path_list )
        files_in_dir = []
        for f in files:
          if re.match( re.escape(path), f['name'] ):  # path in name
            local_name = f['name'].rstrip( self.delimiter )
            short_name = re.sub( re.escape(path), '', local_name, count = 1)
            short_name = short_name.lstrip( self.delimiter )
            if len( re.split( self.delimiter, f['name'] ) ):  # 1+ if in subdirectory
              files_in_dir.append(f)
        files = files_in_dir
        self.log.debug("SwiftFS.listdir returning: `%s`" % files)
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
              self.log.debug("SwiftFS.isdir setting prefix to '%s'", path)
            response = swift.list( container=self.container, options=_opts )
            for r in response:
              if r['success']:
                self.log.debug("SwiftFS.isdir '%s' is a directory", path)
                count = 1
              else:
                self.log.debug("SwiftFS.isdir '%s' is NOT a directory", path)
          except SwiftError as e:
            self.log.error("SwiftFS.isdir %s", e.value)
        if count:
          self.log.debug("SwiftFS.isdir returning True")
          return True
        else:
          self.log.debug("SwiftFS.isdir returning False")
          return False


    # We need to determine if the old_path is a file or a directory.
    # If it's a file, we copy it & remove the file
    # If it's a directory, then we need to do that process for every object that matches that prefix.
    def mv(self, old_path, new_path):
        self.log.debug("SwiftFS.mv `%s` to `%s`", old_path, new_path)
        if self.guess_type( old_path ) == 'directory':
          self.mv_dir(old_path, new_path)
        else:
          self.cp(old_path, new_path)
          self.rm(old_path)

    
    # We need to determine if the old_path is a file or a directory.
    # If it's a file, we copy it & remove the file
    # If it's a directory, then we need to do that process for every object that matches that prefix.
    def mv_dir(self, old_path, new_path):
        self.log.debug("SwiftFS.mv_dir `%s` to `%s`", old_path, new_path)
        if self.guess_type( old_path ) != 'directory':
          self.mv(old_path, new_path)
        else:
          old_path = old_path.rstrip(self.delimiter) + self.delimiter
          new_path = new_path.rstrip(self.delimiter) + self.delimiter
          files = self.listdir(old_path)
          for f in files:
            old_file = f['name']
            # substitution returns the new string, it doesn't modify the given string
            new_file = re.sub(re.escape(old_path), new_path, old_file, count=1)
            self.log.debug("SwiftFS.mv_dir `%s' -> `%s` => `%s` -> `%s`", old_path, new_path, old_file, new_file)
            self.cp( old_file, new_file)
            self.rm( old_file )

    def cp(self, old_path, new_path):
        self.log.debug("SwiftFS.copy `%s` to `%s`", old_path, new_path)
        old_path = self.clean_path(old_path)
        new_path = self.clean_path(new_path)
        with SwiftService() as swift:
          try:
            response = swift.copy( self.container, [old_path],
                { 'destination': self.delimiter + self.container + self.delimiter + new_path })
            for r in response:
              if r["success"]:
                if r["action"] == "copy_object":
                    self.log.debug(
                        "object %s copied from /%s/%s" %
                        (r["destination"], r["container"], r["object"])
                    )
                if r["action"] == "create_container":
                    self.log.debug(
                        "container %s created" % r["container"]
                    )
              else:
                if "error" in r and isinstance(r["error"], Exception):
                    raise r["error"]
          except SwiftError as e:
            self.log.error(e.value)

    def rm(self, path):
        self.log.debug("SwiftFS.rm `%s`", path)
        path = self.clean_path(path)

        if path in ["", self.delimiter]:
          self.do_error('Cannot delete root directory', code=400) 

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
            path: string
        """
        self.log.error("Swiftfs.guess_type given path: %s", path)
        _type = ''
        if path.endswith(".ipynb"):
            _type = "notebook"
        elif allow_directory and self.isdir(path):
            _type = "directory"
        else:
            _type = "file"
        self.log.error("Swiftfs.guess_type asserting: %s", _type)
        return _type

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
