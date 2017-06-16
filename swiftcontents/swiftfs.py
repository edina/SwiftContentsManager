"""
Utilities to make Swift look like a regular file system
"""
import os
import swiftclient
import io
import re
from swiftclient.service import SwiftService, SwiftError, SwiftUploadObject
from swiftclient.multithreading import OutputManager
from swiftclient.exceptions import ClientException
from keystoneauth1 import session
from keystoneauth1.identity import v3
from tornado.web import HTTPError
from traitlets import default, HasTraits, Unicode, Any, Instance
# from pprint import pprint


class SwiftFS(HasTraits):

    container = Unicode(os.environ.get('CONTAINER', 'demo'))
    storage_url = Unicode(
        help="The base URL for containers",
        default_value='http://example.com',
        config=True
        )

    delimiter = Unicode("/", help="Path delimiter", config=True)

    root_dir = Unicode("/", config=True)

    def __init__(self, log, **kwargs):
        super(SwiftFS, self).__init__(**kwargs)
        self.log = log
        self.log.info("SwiftContents[SwiftFS] container: `%s`", self.container)

        # With the python swift client, the connection is automagically
        # created using environment variables (I know... horrible or what?)

        # open connection to swift container
        self.swift = SwiftService()

        # make sure container exists
        try:
            result = self.swift.post(container=self.container)
        except SwiftError as e:
            self.log.error("creating container %s", e.value)
            raise HTTPError(404,e.value)
            
        if not result["success"]:
            msg = "could not create container %s"%self.container
            self.log.error(msg)
            raise HTTPError(404,msg)
            
            
        self.log.info("using container `%s`", self.container)

    # see 'list' at https://docs.openstack.org/developer/python-swiftclient/service-api.html
    # Returns a list of all objects that start with the prefix given
    # Of course, in a proper heirarchical file-system, list-dir only returns the files
    # in that dir, so we need to filter the list to me ONLY those objects where the
    # 'heirarchical' bit of the name stops at the path given
    # The method has 2 modes: 1 when the list of names is returned with the full
    # path-name, and one where the name is just the "file name"
    def listdir(self, path="", with_prefix=False, this_dir_only=True):
        """
        list all the "files" in the "directory" for the given path.

        If the 'this_dir_only' is False (it is True by default), then
        the full list of all objects in that path are returned (needed for a
        rename, for example)

        returns a list of dictionaries for each object:
            {'bytes': 11,
             'hash': '3e25960a79dbc69b674cd4ec67a72c62',
             'last_modified': '2017-06-06T08:55:36.473Z',
             'name': 'foo/bar/thingamy.bob'}
        """
        self.log.info("SwiftFS.listdir Listing directory: `%s`", path)
        files = []
        path = self.clean_path(path)

        # Get all objects that match the known path
        try:
            _opts = {'prefix': path}
            dir_listing = self.swift.list(container=self.container,
                                     options=_opts)
            for page in dir_listing:  # each page is up to 10,000 items
                if page["success"]:
                    files.extend(page["listing"])
                else:
                    raise page["error"]
        except SwiftError as e:
            self.log.error("SwiftFS.listdir %s", e.value)

        if this_dir_only:
            files = self.restrict_to_this_dir(path, files)

        self.log.info("SwiftFS.listdir returning: `%s`" % files)
        return files

    # Restrict the given list of files to just the ones in the current "dir"
    def restrict_to_this_dir(self, path, files):
        self.log.info("SwiftFS.restrict_to_this_dir has path: `%s`", path)
        self.log.info("SwiftFS.restrict_to_this_dir has list: `%s`" % files)

        if len(files) == 0:
            return

        files_in_dir = []
        # make up the pattern to compile into our regex engine
        # The path given (sans delimiter), followed by the delimiter,
        # followed by anything that's NOT a delimiter, possibly followed by the
        # delimiter, and that's the end oof the string!
        pattern = re.escape(path.rstrip(self.delimiter))
        pattern = pattern + '(?:(?:' + self.delimiter + ')?[^' + self.delimiter + ']+'
        pattern = pattern + self.delimiter + '?)?'
        pattern = pattern + '$'
        self.log.debug("Swiftfs.restrict_to_this_dir pattern is: `%s`", pattern)
        regex = re.compile(pattern, re.UNICODE)
        for f in files:
            self.log.debug("Swiftfs.restrict_to_this_dir checking: `%s`",
                           f['name'])
            # do we want this file?
            if regex.match(f['name']) and (f['name'] != path.rstrip(self.delimiter) + self.delimiter):
                self.log.debug("Swiftfs.restrict_to_this_dir keeping")
                files_in_dir.append(f)
            else:
                self.log.debug("Swiftfs.restrict_to_this_dir ignoring")
        return files_in_dir

    # We can 'stat' files, but not directories
    def isfile(self, path):
        self.log.info("SwiftFS.isfile Checking if `%s` is a file", path)
        path = self.clean_path(path)

        if path is None or path == '':
            self.log.debug("SwiftFS.isfile has no path, returning False")
            return False

        self.log.debug("SwiftFS.isfile path truncated to `%s`", path)
        try:
            stat_it = self.swift.stat(container=self.container, objects=[path])
            for stat_res in stat_it:
                if stat_res['success']:
                    self.log.debug("SwiftFS.isfile returning True")
                    return True
                else:
                    self.log.error(
                      'Failed to retrieve stats for %s' % stat_res['object']
                    )
            self.log.info("SwiftFS.isfile returning False")
            return False
        except SwiftError as e:
            self.log.error("SwiftFS.isfile %s", e.value)
        self.log.info("SwiftFS.isfile failed, False")
        return False

    # We can 'list' direcotries, but not 'stat' them
    def isdir(self, path):
        self.log.info("SwiftFS.isdir Checking if `%s` is a directory", path)
        path = self.clean_path(path)

        # directories mush have a trailing slash on them.
        # The core code seems to remove any trailing slash, so lets add it back
        # on
        path = path.rstrip(self.delimiter)
        path = path + self.delimiter

        # Root directory checks
        if path == self.delimiter:  # effectively root directory
            self.log.info("SwiftFS.isdir found root dir - returning True")
            return True

        count = 0
        try:
            _opts = {}
            if re.search('\w', path):
                _opts = {'prefix': path}
                self.log.debug("SwiftFS.isdir setting prefix to '%s'", path)
            response = self.swift.list(container=self.container, options=_opts)
            for r in response:
                if r['success']:
                    self.log.debug("SwiftFS.isdir '%s' is a directory",
                                   path)
                    count = 1
                else:
                    self.log.debug("SwiftFS.isdir '%s' is NOT a directory",
                                   path)
        except SwiftError as e:
            self.log.error("SwiftFS.isdir %s", e.value)
        if count:
            self.log.info("SwiftFS.isdir returning True")
            return True
        else:
            self.log.info("SwiftFS.isdir returning False")
            return False

    # We need to determine if the old_path is a file or a directory.
    # If it's a file, we copy it & remove the file
    # If it's a directory, handle it differently.
    def mv(self, old_path, new_path):
        self.log.info("SwiftFS.mv `%s` to `%s`", old_path, new_path)
        if self.guess_type(old_path) == 'directory':
            self.mv_dir(old_path, new_path)
        else:
            self.cp(old_path, new_path)
            self.rm(old_path)

    # We need to determine if the old_path is a file or a directory.
    # If it's a directory, then we need to do that process for every object that
    # matches that prefix.
    def mv_dir(self, old_path, new_path):
        self.log.info("SwiftFS.mv_dir `%s` to `%s`", old_path, new_path)
        if self.guess_type(old_path) != 'directory':
            self.mv(old_path, new_path)
        else:
            old_path = old_path.rstrip(self.delimiter) + self.delimiter
            new_path = new_path.rstrip(self.delimiter) + self.delimiter
            files = self.listdir(old_path, this_dir_only=False)
            for f in files:
                old_file = f['name']
                # substitution returns the new string, it doesn't modify the
                # given string
                new_file = re.sub(re.escape(old_path), new_path,
                                  old_file, count=1)
                self.log.debug("SwiftFS.mv_dir `%s' -> `%s` => `%s` -> `%s`",
                               old_path, new_path, old_file, new_file)
                self.cp(old_file, new_file)
                self.rm(old_file)

    def cp(self, old_path, new_path):
        self.log.info("SwiftFS.copy `%s` to `%s`", old_path, new_path)
        old_path = self.clean_path(old_path)
        new_path = self.clean_path(new_path)
        try:
            response = self.swift.copy(self.container, [old_path],
                                  {'destination': self.delimiter +
                                   self.container +
                                   self.delimiter +
                                   new_path})
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
        self.log.info("SwiftFS.rm `%s`", path)
        path = self.clean_path(path)

        if path in ["", self.delimiter]:
            self.do_error('Cannot delete root directory', code=400)
            return False
        #  ####### Need to add code to not delete a "dir" object that has
        #  sub-dirs or files "under" it.
        files = self.listdir(path, this_dir_only=False)

        try:
            response = self.swift.delete(container=self.container,
                                    objects=[path])
            for r in response:
                self.log.debug("SwiftFS.rm action: `%s` success: `%s`",
                               r['action'], r['success'])
        except SwiftError as e:
            self.log.error("SwiftFS.rm %s", e.value)

    # Directories are just objects that have a trailing '/'
    def mkdir(self, path):
        self.log.info("SwiftFS.mkdir `%s`", path)
        path = self.clean_path(path)
        path = path.rstrip(self.delimiter)
        path = path + self.delimiter
        self._do_write(path, None)

    # This works by downloading the file to disk then reading the contents of
    # that file into memory, before deleting the file
    # NOTE this is reading text files!
    # NOTE this really only works with files in the local direcotry, but given
    # local filestore will disappear when the docker ends, I'm not too bothered.
    def read(self, path):
        self.log.info("SwiftFS.read `%s`", path)
        path = self.clean_path(path)
        content = ''
        try:
            response = self.swift.download(container=self.container,
                                      objects=[path])
            for r in response:
                if r['success']:
                    filename = open(r['path'])
                    content = filename.read()
                    os.remove(r['path'])
        except SwiftError as e:
            self.log.error("SwiftFS.read %s", e.value)
        return content

    # Write is 'upload' and 'upload' needs a "file" it can read from
    # We use io.StringIO for this
    def write(self, path, content):
        self.log.info("SwiftFS.write `%s`", path)
        path = self.clean_path(path)
        # If we can't make the directory path, then we can't make the file!
        success = self._make_intermedate_dirs(path)
        if success:
            self._do_write(path, content)

    def _make_intermedate_dirs(self, path):
        self.log.info("SwiftFS._make_intermedate_dirs `%s`", path)
        # we loop over the path, checking for an object at every level
        # of the hierachy, except the last item (which may be a file,
        # or a directory itself
        path_parts = re.split(self.delimiter, path)
        current_path = ''
        for p in path_parts[:-1]:
            this_path = current_path + p + self.delimiter
            if self.isfile(this_path):
                self.log.debug(
                    "SwiftFS._make_intermedate_dirs failure: dir exists at path `%s`"
                    % this_path)
                return False
            if not self.isdir(this_path):
                self.log.debug("SwiftFS._make_intermedate_dirs making directory")
                self._do_write(this_path, None)
            current_path = this_path
        self.log.info("SwiftFS._make_intermedate_dirs finished")
        return True

    def _do_write(self, path, content):
        self.log.info("SwiftFS._do_write `%s`", path)
        path = self.clean_path(path)
        _opts = {'object_uu_threads': 20}
        with SwiftService(options=_opts) as swift, OutputManager() as out_manager:
            try:
                type = self.guess_type(path)
                things = []
                if type == "directory":
                    self.log.debug("SwiftFS._do_write create directory")
                    things.append(SwiftUploadObject(None, object_name=path))
                else:
                    self.log.debug("SwiftFS._do_write create file/notebook from '%s'", content)
                    output = io.BytesIO(content.encode('utf-8'))
                    things.append(SwiftUploadObject(output, object_name=path))

                # Now do the upload
                response = swift.upload(self.container, things)
                for r in response:
                    self.log.debug("SwiftFS._do_write action: '%s', response: '%s'",
                                   r['action'], r['success'])
            except SwiftError as e:
                self.log.error("SwiftFS._do_write swift-error: %s", e.value)
            except ClientException as e:
                self.log.error("SwiftFS._do_write client-error: %s", e.value)

    def guess_type(self, path, allow_directory=True):
        """
        Guess the type of a file.
        If allow_directory is False, don't consider the possibility that the
        file is a directory.

        Parameters
        ----------
            path: string
        """
        self.log.info("Swiftfs.guess_type given path: %s", path)
        _type = ''
        if path.endswith(".ipynb"):
            _type = "notebook"
        elif allow_directory and path.endswith(self.delimiter):
            _type = "directory"
        else:
            _type = "file"
        self.log.info("Swiftfs.guess_type asserting: %s", _type)
        return _type

    def clean_path(self, path):
        # strip of any leading '/'
        path = path.lstrip(self.delimiter)
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
