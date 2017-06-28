import os
import json
import mimetypes
import logging
from datetime import datetime
from dateutil.parser import parse
from pprint import pprint
from tornado.web import HTTPError
from traitlets import default, Unicode, List
from base64 import b64decode

from swiftcontents.swiftfs import SwiftFS, SwiftFSError, NoSuchFile
from swiftcontents.ipycompat import ContentsManager
from swiftcontents.ipycompat import reads, from_dict
from swiftcontents.callLogging import *

DUMMY_CREATED_DATE = datetime.now( )
NBFORMAT_VERSION = 4

class SwiftContentsManager(ContentsManager):
    # Initialise the instance
    def __init__(self, *args, **kwargs):
        super(SwiftContentsManager, self).__init__(*args, **kwargs)
        self.swiftfs = SwiftFS(log=self.log)

    @LogMethodResults()
    def make_dir(self, path):
        """Create a directory
        """
        if self.file_exists(path) or self.dir_exists(path):
            self.already_exists(path)
        else:
            self.swiftfs.mkdir(path)

    @LogMethodResults()
    def get(self, path, content=True, type=None, format=None):
        """Retrieve an object from the store, named in 'path'

        named parameters
        ----------
            content : boolean. whether we want the actual content or not
            type: ['notebook', 'directory', 'file'] specifies what type of object this is
            format: /dunno/
        """

        if type is None:
            # need to check if path is a directory
            if self.swiftfs.isdir(path):
                type="directory"
            else:
                type = self.swiftfs.guess_type(path)
        if type not in ["directory","notebook","file"]:
            msg = "Unknown type passed: '{}'".format(type)
            self.do_error(msg)

        # construct accessor name from type
        # eg file => _get_file
        func = getattr(self,'_get_'+type)
        metadata = self.swiftfs.listdir(path)

        # now call the appropriate function, with the parameters given    
        response = func(path=path, content=content, format=format, metadata=metadata)
        return response

    @LogMethodResults()
    def save(self, model, path):
        """Save a file or directory model to path.
        """
        if "type" not in model:
            self.do_error("No model type provided", 400)
        if "content" not in model and model["type"] != "directory":
            self.do_error("No file content provided", 400)

        if model["type"] not in ("file", "directory", "notebook"):
            self.do_error("Unhandled contents type: %s" % model["type"], 400)

        try:
            if model["type"] == "notebook":
                validation_message = self._save_notebook(model, path)
            elif model["type"] == "file":
                validation_message = self._save_file(model, path)
            else:
                validation_message = self._save_directory(path)
        except Exception as e:
            self.log.error("swiftmanager.save Error while saving file: %s %s", path, e, exc_info=True)
            self.do_error("Unexpected error while saving file: %s %s" % (path, e), 500)

        # Read back content to verify save
        self.log.debug("swiftmanager.save getting file to validate: `%s`, `%s`", path, model["type"] )
        returned_model = self.get(path, type=model["type"], content=False)
        if validation_message is not None:
            returned_model["message"] = validation_message
        return returned_model

    @LogMethod()
    def delete_file(self, path):
        """Delete the file or directory at path.
        """
        if self.file_exists(path) or self.dir_exists(path):
            self.swiftfs.rm(path)
        else:
            self.no_such_entity(path)

    @LogMethod()
    def rename_file(self, old_path, new_path):
        """Rename a file or directory.

        NOTE: This method is unfortunately named on the base class.  It
        actually moves a file or a directory.
        """
        if self.file_exists(new_path) or self.dir_exists(new_path):
            self.already_exists(new_path)
        elif self.file_exists(old_path) or self.dir_exists(old_path):
            self.log.debug("swiftmanager.rename_file: Actually renaming '%s' to '%s'", old_path,
                           new_path)
            self.swiftfs.mv(old_path, new_path)
        else:
            self.no_such_entity(old_path)

    @LogMethodResults()
    def file_exists(self, path):
        return self.swiftfs.isfile(path)

    @LogMethodResults()
    def dir_exists(self, path):
        return self.swiftfs.isdir(path)

    # Swift doesn't do "hidden" files, so this always returns False
    @LogMethodResults()
    def is_hidden(self, path):
        """Is path a hidden directory or file?
        """
        return False

    @LogMethod()
    def list_checkpoints(self, path):
        pass

    @LogMethodResults()
    def delete(self, path):
        self.delete_file(path)

    # We can rename_file, or mv directories
    @LogMethod()
    def rename(self, old_path, new_path):
        self.rename_file( old_path, new_path )

    @LogMethod()
    def do_error(self, msg, code=500):
        raise HTTPError(code, msg)

    @LogMethod()
    def no_such_entity(self, path):
        self.do_error("swiftmanager.no_such_entity called: [{path}]".format(path=path), 404)

    @LogMethod()
    def already_exists(self, path):
        thing = "File" if self.file_exists(path) else "Directory"
        note = "{thing} already exists: [{path}]".format(thing=thing, path=path)
        self.do_error(note, 409)

    @LogMethodResults()
    def _get_directory(self, path, content=True, format=None, metadata={}):
        return self._directory_model_from_path(path, content=content, metadata=metadata)

    @LogMethodResults()
    def _get_notebook(self, path, content=True, format=None, metadata={}):
        return self._notebook_model_from_path(path, content=content, format=format, metadata=metadata)

    @LogMethodResults()
    def _get_file(self, path, content=True, format=None, metadata={}):
        return self._file_model_from_path(path, content=content, format=format, metadata=metadata)

    @LogMethodResults()
    def _directory_model_from_path(self, path, content=False, metadata={}):
        model = base_directory_model(path)
        if content:
            if not self.dir_exists(path):
                self.no_such_entity(path)
            model["format"] = "json"
            model["content"] = self._convert_file_records(metadata)
        return model

    @LogMethodResults()
    def _notebook_model_from_path(self, path, content=False, format=None, metadata={} ):
        """
        Build a notebook model from database record.
        """
        # path = to_api_path(record['parent_name'] + record['name'])
        model = base_model(path)
        model['type'] = 'notebook'
        if 'last_modified' in metadata :
            model['last_modified'] = model['created'] = parse(metadata['last_modified'])
        else:
            model['last_modified'] = model['created'] = DUMMY_CREATED_DATE
        if content:
            if not self.swiftfs.isfile(path):
                self.no_such_entity(path)
            file_content = self.swiftfs.read(path)
            nb_content = reads(file_content, as_version=NBFORMAT_VERSION)
            self.mark_trusted_cells(nb_content, path)
            model["format"] = "json"
            model["content"] = nb_content
            self.validate_notebook_model(model)
        return model

    @LogMethodResults()
    def _file_model_from_path(self, path, content=False, format=None, metadata={}):
        """
        Build a file model from object.
        """
        model = base_model(path)
        model['type'] = 'file'

        if 'last_modified' in metadata :
            model['last_modified'] = model['created'] = parse(metadata['last_modified'])
        else:
            model['last_modified'] = model['created'] = DUMMY_CREATED_DATE
        if content:
            try:
                content = self.swiftfs.read(path)
            except NoSuchFile as e:
                self.no_such_entity(e.path)
            except SwiftFSError as e:
                self.do_error(str(e), 500)
            model["format"] = format or "text"
            model["content"] = content
            model["mimetype"] = mimetypes.guess_type(path)[0] or "text/plain"
            if format == "base64":
                model["format"] = format or "base64"
                model["content"] = b64decode(content)
        return model

    @LogMethodResults()
    def _convert_file_records(self, records):
        """
        Applies _notebook_model_from_swift_path or _file_model_from_swift_path to each entry of `paths`,
        depending on the result of `guess_type`.
        """
        ret = []
        for r in records:
            self.log.debug("swiftmanager._convert_file_records iterating: '%s'" % r)
            type_ = ""

            # For some reason, "path" sometimes comes through as a dictionary like:
            # {'hash': 'd41d8cd98f00b204e9800998ecf8427e', 'bytes': 0, 'last_modified': '2017-05-31T09:20:22.224Z', 'name': 'foo/file.txt'}
            # in which case, we 
            if not isinstance(r, str):
                path = r['name']

            type_ = self.swiftfs.guess_type( path )

            self.log.debug("swiftmanager._convert_file_records type is: '%s' [ %s]" % (type_, r) )
            if type_ == "notebook":
                ret.append(self._notebook_model_from_path(path, content=False, metadata=r))
            elif type_ == "file":
                ret.append(self._file_model_from_path(path, content=False, metadata=r))
            elif type_ == "directory":
                ret.append(self._directory_model_from_path(path, content=False, metadata=r))
            else:
                self.do_error("Unknown file type %s for file '%s'" % (type_, path), 500)
        return ret

    @LogMethodResults()
    def _save_notebook(self, model, path):
        nb_contents = from_dict(model['content'])
        self.check_and_sign(nb_contents, path)
        file_contents = json.dumps(model["content"])
        self.swiftfs.write(path, file_contents)
        self.validate_notebook_model(model)
        return model.get("message")

    @LogMethod()
    def _save_file(self, model, path):
        file_contents = model["content"]
        self.swiftfs.write(path, file_contents)

    @LogMethod()
    def _save_directory(self, path):
        self.swiftfs.mkdir(path)

    @LogMethodResults()
    def _get_os_path(self, path):
        """A method for converting an object path into File System Path.
        As we only need local file-system copies for 'get' calls to download into, all files are
        dumped into /tmp
        """
        return '/tmp/' + path

@LogMethod()
def base_model(path):
    p = path.split('/')
    name = p.pop()
    if len(name)==0 and len(p)>0:
        name = p.pop()+'/'

    print("\n\n\n\nmagi_debug path %s\n\n\n\n\n"%path)

    return {
        "name": name,
        "path": path.lstrip('/'), 
        "writable": True,
        "last_modified": None,
        "created": None,
        "content": None,
        "format": None,
        "mimetype": None,
    }


@LogMethodResults()
def base_directory_model(path):
    delimiter = '/'
    model = base_model(path)
    model.update(
        type="directory",
        last_modified=DUMMY_CREATED_DATE,
        created=DUMMY_CREATED_DATE,
        path = model['path'].strip(delimiter),
        name = model['name'].strip(delimiter) 
    )
    return model
