import os
import json
import mimetypes
from datetime import datetime
from pprint import pprint
from tornado.web import HTTPError
from traitlets import default, Unicode, List

from swiftcontents.swiftfs import SwiftFS, SwiftFSError, NoSuchFile
from swiftcontents.ipycompat import ContentsManager
from swiftcontents.ipycompat import reads, from_dict

DUMMY_CREATED_DATE = datetime.now( )
NBFORMAT_VERSION = 4

class SwiftContentsManager(ContentsManager):

    # Initialise the instance
    def __init__(self, *args, **kwargs):
        super(SwiftContentsManager, self).__init__(*args, **kwargs)
        self.swiftfs = SwiftFS(log=self.log)

    # These are the methods needed by a 'ContentsManager'
    ###########

    def get(self, path, content=True, type=None, format=None):
        """Retrieve an object from the store, named in 'path'

        named parameters
        ----------
            content : boolean. whether we want the actual content or not
            type: ['notebook', 'directory', 'file'] specifies what type of object this is
            format: /dunno/
        """
        self.log.debug("swiftmanager.get called: '%s' %s %s", path, type, format)

        if type is None:
            type = self.swiftfs.guess_type(path)

        # func becomes a function call, depending on type    
        try:
            func = {
                "directory": self._get_directory,
                "notebook": self._get_notebook,
                "file": self._get_file,
            }[type]
        except KeyError:
            self.log.debug("swiftmanager.get error: unknown type `%s`", type )
            raise ValueError("Unknown type passed: '{}'".format(type))

        # now call the appropriate function, with the parameters given    
        response = func(path=path, content=content, format=format)
        self.log.debug("swiftmanager.get returning")
        return response

    def save(self, model, path):
        """Save a file or directory model to path.
        """
        self.log.debug("swiftmanager.save  called\nModel: %s\npath: '%s'", model, path)
        if "type" not in model:
            self.do_error("No model type provided", 400)
        if "content" not in model and model["type"] != "directory":
            self.do_error("No file content provided", 400)

        if model["type"] not in ("file", "directory", "notebook"):
            self.do_error("Unhandled contents type: %s" % model["type"], 400)
        self.log.debug("swiftmanager.save: type= '%s'", model["type"])

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
        model = self.get(path, type=model["type"], content=False)
        if validation_message is not None:
            model["message"] = validation_message
        self.log.debug("swiftmanager.save returning")
        return model

    def delete_file(self, path):
        """Delete the file or directory at path.
        """
        self.log.debug("swiftmanager.delete_file called '%s'", path)
        if self.file_exists(path) or self.dir_exists(path):
            self.swiftfs.rm(path)
        else:
            self.no_such_entity(path)
        self.log.debug("swiftmanager.delete_file ends")

    def rename_file(self, old_path, new_path):
        """Rename a file or directory.

        NOTE: This method is unfortunately named on the base class.  It
        actually moves a file or a directory.
        """
        self.log.debug("swiftmanager.rename_file : '%s' to '%s'", old_path, new_path)
        if self.file_exists(new_path) or self.dir_exists(new_path):
            self.already_exists(new_path)
        elif self.file_exists(old_path) or self.dir_exists(old_path):
            self.log.debug("swiftmanager.rename_file: Actually renaming '%s' to '%s'", old_path,
                           new_path)
            self.swiftfs.mv(old_path, new_path)
        else:
            self.no_such_entity(old_path)
        self.log.debug("swiftmanager.rename_file ends")

    def file_exists(self, path):
        self.log.debug("swiftmanager.file_exists called '%s'", path)
        return self.swiftfs.isfile(path)

    def dir_exists(self, path):
        self.log.debug("swiftmanager.dir_exists called '%s'", path)
        return self.swiftfs.isdir(path)

    # Swift doesn't do "hidden" files, so this always returns False
    def is_hidden(self, path):
        """Is path a hidden directory or file?
        """
        self.log.debug("swiftmanager.is_hidden called '%s'", path)
        return False

    # .... and these are all support methods
    ############

    def list_checkpoints(self, path):
        self.log.debug("swiftmanager.list_checkpoints: not implimented (path was '%s')", path)

    def delete(self, path):
        self.log.debug("swiftmanager.delete called (path was '%s')", path)
        self.delete_file(path)

    # We can rename_file, or mv directories
    def rename(self, old_path, new_path):
        self.log.debug("swiftmanager.rename called (old_path '%s', new_path '%s')", old_path, new_path)
        self.rename_file( old_path, new_path )

    def do_error(self, msg, code=500):
        raise HTTPError(code, msg)

    def no_such_entity(self, path):
        self.do_error("swiftmanager.no_such_entity called: [{path}]".format(path=path), 404)

    def already_exists(self, path):
        self.log.debug("swiftmanager.already_exists called : '%s'", path)
        thing = "File" if self.file_exists(path) else "Directory"
        self.log.debug("SwiftContents[swiftmanager] {thing} already exists: [{path}]".format(thing=thing, path=path))
        self.do_error(u"SwiftContents[swiftmanager] {thing} already exists: [{path}]".format(thing=thing, path=path), 409)

    def _get_directory(self, path, content=True, format=None):
        self.log.debug("swiftmanager._get_directory called '%s' %s %s", path, type, format)
        return self._directory_model_from_path(path, content=content)

    def _get_notebook(self, path, content=True, format=None):
        self.log.debug("swiftmanager._get_notebook called '%s' %s %s", path, content, format)
        return self._notebook_model_from_path(path, content=content, format=format)

    def _get_file(self, path, content=True, format=None):
        self.log.debug("swiftmanager._get_file called '%s' %s %s", path, content, format)
        return self._file_model_from_path(path, content=content, format=format)

    def _directory_model_from_path(self, path, content=False):
        self.log.debug("swiftmanager._directory_model_from_path called '%s' %s", path, content)
        model = base_directory_model(path)
        if content:
            if not self.dir_exists(path):
                self.no_such_entity(path)
            model["format"] = "json"
            dir_list = self.swiftfs.listdir(path=path)
            self.log.debug("swiftmanager._directory_model_from_path has dir_content '%s'", dir_list)
            model["content"] = self._convert_file_records(dir_list)
        self.log.debug("swiftmanager._directory_model_from_path returning '%s'" % model)
        return model

    def _notebook_model_from_path(self, path, content=False, format=None):
        """
        Build a notebook model from database record.
        """
        self.log.debug("swiftmanager._notebook_model_from_path called '%s' %s", path, content)
        # path = to_api_path(record['parent_name'] + record['name'])
        model = base_model(path)
        model['type'] = 'notebook'
        # model['last_modified'] = model['created'] = record['created_at']
        model['last_modified'] = model['created'] = DUMMY_CREATED_DATE
        if content:
            self.log.debug("swiftmanager._notebook_model_from_path content flag true")
            if not self.swiftfs.isfile(path):
                self.no_such_entity(path)
            file_content = self.swiftfs.read(path)
            nb_content = reads(file_content, as_version=NBFORMAT_VERSION)
            self.mark_trusted_cells(nb_content, path)
            model["format"] = "json"
            model["content"] = nb_content
            self.log.debug("swiftmanager._notebook_model_from_path validating model")
            self.validate_notebook_model(model)
            self.log.debug("swiftmanager._notebook_model_from_path validated model")
        else:
            self.log.debug("swiftmanager._notebook_model_from_path content flag false")
        self.log.debug("swiftmanager._notebook_model_from_path returning '%s'" % model)
        return model

    def _file_model_from_path(self, path, content=False, format=None):
        """
        Build a file model from object.
        """
        self.log.debug("swiftmanager._file_model_from_path called '%s' %s", path, content)
        model = base_model(path)
        model['type'] = 'file'
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
                from base64 import b64decode
                model["content"] = b64decode(content)
        self.log.debug("swiftmanager._file_model_from_path returning '%s'" % model)
        return model

    def _convert_file_records(self, paths):
        """
        Applies _notebook_model_from_swift_path or _file_model_from_swift_path to each entry of `paths`,
        depending on the result of `guess_type`.
        """
        ret = []
        for path in paths:
            self.log.debug("swiftmanager._convert_file_records iterating: '%s'", path)
            type_ = ""

            # For some reason, "path" sometimes comes through as a dictionary like:
            # {'hash': 'd41d8cd98f00b204e9800998ecf8427e', 'bytes': 0, 'last_modified': '2017-05-31T09:20:22.224Z', 'name': 'foo/file.txt'}
            # in which case, we 
            if isinstance(path, str):
               type_ = self.swiftfs.guess_type(path)
            else:
               path = path['name']
               type_ = self.swiftfs.guess_type( path )
            self.log.debug("swiftmanager._convert_file_records type is: '%s'", type_)
            if type_ == "notebook":
                ret.append(self._notebook_model_from_path(path, False))
            elif type_ == "file":
                ret.append(self._file_model_from_path(path, False, None))
            elif type_ == "directory":
                ret.append(self._directory_model_from_path(path, False))
            else:
                self.do_error("Unknown file type %s for file '%s'" % (type_, path), 500)
        return ret

    def _save_notebook(self, model, path):
        self.log.debug("swiftmanager._save_notebook called '%s'", path)
        nb_contents = from_dict(model['content'])
        self.check_and_sign(nb_contents, path)
        file_contents = json.dumps(model["content"])
        self.log.debug("swiftmanager._save_notebook calling swiftfs.write")
        self.swiftfs.write(path, file_contents)
        self.log.debug("swiftmanager._save_notebook calling validate_notebook_model")
        self.validate_notebook_model(model)
        self.log.debug("swiftmanager._save_notebook returning message '%s'", model.get("message") )
        return model.get("message")

    def _save_file(self, model, path):
        self.log.debug("swiftmanager._save_file called '%s'", path)
        file_contents = model["content"]
        self.swiftfs.write(path, file_contents)

    def _save_directory(self, path):
        self.log.debug("swiftmanager._save_directory called '%s'", path)
        self.swiftfs.mkdir(path)

    def _get_os_path(self, path):
        """A method for converting an object path into File System Path.
        As we only need local file-system copies for 'get' calls to download into, all files are
        dumped into /tmp
        """
        return '/tmp/' + path

def base_model(path):
    return {
        "name": path.rsplit('/', 1)[-1],
        "path": path.lstrip( '/' ),
        "writable": True,
        "last_modified": None,
        "created": None,
        "content": None,
        "format": None,
        "mimetype": None,
    }


def base_directory_model(path):
    model = base_model(path)
    model.update(
        type="directory",
        last_modified=DUMMY_CREATED_DATE,
        created=DUMMY_CREATED_DATE,)
    return model
