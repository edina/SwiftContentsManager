import os
import json
import mimetypes
from datetime import datetime

from tornado.web import HTTPError
from traitlets import default, Unicode, List

from swiftcontents.swiftfs import SwiftFS, SwiftFSError, NoSuchFile
from swiftcontents.ipycompat import ContentsManager

class SwiftContentsManager(ContentsManager):

    # Initialise the instance
    def __init__(self, *args, **kwargs):
        super(SwiftContentsManager, self).__init__(*args, **kwargs)
        self.swiftfs = SwiftFS(log=self.log)

    # These are the methods needed by a 'ContentsManager'
    ###########

    def get(self, path, content=True, type=None, format=None):
        # Get a file or directory model.
        self.log.debug("SwiftContents[swiftmanager]: get '%s' %s %s", path, type, format)

        if type is None:
            type = self.guess_type(path)
        try:
            func = {
                "directory": self._get_directory,
                "notebook": self._get_notebook,
                "file": self._get_file,
            }[type]
        except KeyError:
            raise ValueError("Unknown type passed: '{}'".format(type))

        return func(path=path, content=content, format=format)

    def save(self, model, path):
        """Save a file or directory model to path.
        """
        self.log.debug("SwiftContents[swiftmanager]: save %s: '%s'", model, path)
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
            self.log.error("Error while saving file: %s %s", path, e, exc_info=True)
            self.do_error("Unexpected error while saving file: %s %s" % (path, e), 500)

        model = self.get(path, type=model["type"], content=False)
        if validation_message is not None:
            model["message"] = validation_message
        return model

    def delete_file(self, path):
        """Delete the file or directory at path.
        """
        self.log.debug("SwiftContents[swiftmanager]: delete_file '%s'", path)
        if self.file_exists(path) or self.dir_exists(path):
            self.swiftfs.rm(path)
        else:
            self.no_such_entity(path)

    def rename_file(self, old_path, new_path):
        """Rename a file or directory.

        NOTE: This method is unfortunately named on the base class.  It
        actually moves a file or a directory.
        """
        self.log.debug("SwiftContents[swiftmanager]: Init rename of '%s' to '%s'", old_path, new_path)
        if self.file_exists(new_path) or self.dir_exists(new_path):
            self.already_exists(new_path)
        elif self.file_exists(old_path) or self.dir_exists(old_path):
            self.log.debug("SwiftContents[swiftmanager]: Actually renaming '%s' to '%s'", old_path,
                           new_path)
            self.swiftfs.mv(old_path, new_path)
        else:
            self.no_such_entity(old_path)

    def file_exists(self, path):
        self.log.debug("SwiftContents[swiftmanager]: file_exists '%s'", path)
        return self.swiftfs.isfile(path)

    def dir_exists(self, path):
        self.log.debug("SwiftContents[swiftmanager]: dir_exists '%s'", path)
        return self.swiftfs.isdir(path)

    # Swift doesn't do "hidden" files, so this always returns False
    def is_hidden(self, path):
        """Is path a hidden directory or file?
        """
        self.log.debug("SwiftContents[swiftmanager]: is_hidden '%s'", path)
        return False

    # .... and these are all support methods
    ############

    def guess_type(self, path):
        return "directory" if path.endswith('/') else super(ContentsManager, self).guess_type(path)

    def do_error(self, msg, code=500):
        raise HTTPError(code, msg)

    def no_such_entity(self, path):
        self.do_error("No such entity: [{path}]".format(path=path), 404)

    def already_exists(self, path):
        thing = "File" if self.file_exists(path) else "Directory"
        self.do_error(u"{thing} already exists: [{path}]".format(thing=thing, path=path), 409)

    def _get_directory(self, path, content=True, format=None):
        self.log.debug("SwiftContents[swiftmanager]: get_directory '%s' %s %s", path, type, format)
        return self._directory_model_from_path(path, content=content)

    def _get_notebook(self, path, content=True, format=None):
        self.log.debug("SwiftContents[swiftmanager]: get_notebook '%s' %s %s", path, content, format)
        return self._notebook_model_from_path(path, content=content, format=format)

    def _get_file(self, path, content=True, format=None):
        self.log.debug("SwiftContents[swiftmanager]: get_file '%s' %s %s", path, content, format)
        return self._file_model_from_path(path, content=content, format=format)

    def _directory_model_from_path(self, path, content=False):
        self.log.debug("SwiftContents[swiftmanager]: _directory_model_from_path '%s' %s", path, content)
        model = base_directory_model(path)
        if content:
            if not self.dir_exists(path):
                self.no_such_entity(path)
            model["format"] = "json"
            dir_content = self.swiftfs.listdir(path=path, with_prefix=True)
            model["content"] = self._convert_file_records(dir_content)
        return model

    def _notebook_model_from_path(self, path, content=False, format=None):
        """
        Build a notebook model from database record.
        """
        # path = to_api_path(record['parent_name'] + record['name'])
        model = base_model(path)
        model['type'] = 'notebook'
        # model['last_modified'] = model['created'] = record['created_at']
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

    def _file_model_from_path(self, path, content=False, format=None):
        """
        Build a file model from database record.
        """
        model = base_model(path)
        model['type'] = 'file'
        model['last_modified'] = model['created'] = DUMMY_CREATED_DATE
        if content:
            try:
                content = self.swiftfs.read(path)
            except NoSuchFile as e:
                self.no_such_entity(e.path)
            except S3FSError as e:
                self.do_error(str(e), 500)
            model["format"] = format or "text"
            model["content"] = content
            model["mimetype"] = mimetypes.guess_type(path)[0] or "text/plain"
            if format == "base64":
                model["format"] = format or "base64"
                from base64 import b64decode
                model["content"] = b64decode(content)
        return model

    def _convert_file_records(self, paths):
        """
        Applies _notebook_model_from_swift_path or _file_model_from_swift_path to each entry of `paths`,
        depending on the result of `guess_type`.
        """
        ret = []
        for path in paths:
            path = self.swiftfs.remove_prefix(path, self.prefix)    # Remove bucket prefix from paths
            if os.path.basename(path) == self.swiftfs.dir_keep_file:
                continue
            type_ = self.guess_type(path, allow_directory=True)
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
        nb_contents = from_dict(model['content'])
        self.check_and_sign(nb_contents, path)
        file_contents = json.dumps(model["content"])
        self.swiftfs.write(path, file_contents)
        self.validate_notebook_model(model)
        return model.get("message")

    def _save_file(self, model, path):
        file_contents = model["content"]
        self.swiftfs.write(path, file_contents)

    def _save_directory(self, path):
        self.swiftfs.mkdir(path)


## Ian: is the path/name thing right? Directories have no name?
def base_model(path):
    return {
        "name": path.rsplit('/', 1)[-1],
        "path": path,
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
