import logging
from nose.tools import assert_equals, assert_not_equals, assert_raises, assert_true, assert_false
import os
import json
import shutil
from pprint import pprint

from swiftcontents.ipycompat import TestContentsManager

from swiftcontents import SwiftContentsManager
from tempfile import TemporaryDirectory
from tornado.web import HTTPError

log = logging.getLogger('TestSwiftFManager')

# list of dirs to make
# note, directory names must end with a /
testDirectories = ['temp/',
                   'temp/bar/',
                   'temp/bar/temp/',
                   'temp/bar/temp/bar/',
                   'temp/bar/temp/bar/foo/',
                   'temp/bar/temp/bar/foo/bar/',
                   'temp/baz/',
                   'temp/baz/temp/',
                   'temp/baz/temp/bar/',
                   'temp/baz/temp/bar/foo/',
                   'temp/baz/temp/bar/foo/bar/']
testFileName = 'hello.txt'
testFileContent = 'Hello world'
testNotebookName = 'hello.ipynb'
testNotebookContent = {"metadata": {},
                       "nbformat_minor": 2,
                       "cells": [],
                       "nbformat": 4}

class Test_SwiftContentsManager(object):

    def __init__(self):
        self.swiftmanager = SwiftContentsManager()

    # Creates a bunch of directories & files
    def setUp(self):
        log.info('setting up directory structure')
        for d in testDirectories:
            self.swiftmanager.swiftfs.mkdir(d)
        log.info('create a bunch of files')
        for d in testDirectories:
            p = d+testFileName
            self.swiftmanager.swiftfs.write(p, testFileContent)

    # removes the stuff setup, and tests it's gone!
    def teardown(self):
        log.info('tidy up directory structure')
        self.swiftmanager.swiftfs.rm(testDirectories[0],recursive=True)
        assert_false(self.swiftmanager.swiftfs.isdir(testDirectories[0]))

    # Test the central error-handler
    def test_do_error(self):
        sm = self.swiftmanager
        log.info("test_do_error starting")
        assert_raises(HTTPError, lambda: sm.do_error('error_text', 501))
        # really? Oh, OK then...
        assert_raises(HTTPError, lambda: sm.do_error('error_text', 999))

    # Test no_such_entity works
    def test_no_such_entity(self):
        sm = self.swiftmanager
        log.info("test_ no_such_entity starting")
        assert_raises(HTTPError, lambda: sm.no_such_entity(testDirectories[1]) )

    # Test already_exists happens, and distinguishes between files & directories
    def test_already_exists(self):
        sm = self.swiftmanager
        log.info("test_already_exists starting")
        path = testDirectories[1] + testFileName
        assert_raises(HTTPError, lambda: sm.already_exists(path) )
        path =  path+ testFileName
        assert_raises(HTTPError, lambda: sm.already_exists(path) )

    # the last directory in the test suite should test true as a directory, and false as a file
    def test_dir_exists(self):
        sm = self.swiftmanager
        log.info("test_dir_exists starting")
        exists_dir = testDirectories[-1]
        exists_file = exists_dir + testFileName
        assert_true(sm.dir_exists(exists_dir))
        assert_false(sm.dir_exists(exists_file))

    # the last file in the test suite should test true as a file, and false as a directory
    def test_file_exists(self):
        sm = self.swiftmanager
        log.info("test_file_exists starting")
        exists_dir = testDirectories[-1]
        exists_file = exists_dir + testFileName
        assert_true(sm.file_exists(exists_file))
        assert_false(sm.dir_exists(exists_file))

    # is_hidden is hard-coded to be false
    def test_is_hidden(self):
        sm = self.swiftmanager
        log.info("test_is_hidden starting")
        exists_dir = testDirectories[-1]
        exists_file = exists_dir + testFileName
        assert_false(sm.is_hidden(exists_file))
        assert_false(sm.is_hidden(exists_dir))

    # tests make_dir. success if make, fail if it already exists
    def test_make_dir(self):
        sm = self.swiftmanager
        log.info("test_make_dir starting")
        path = testDirectories[1].rstrip('/') + '_b/'
        sm.make_dir(path)
        assert_true(sm.dir_exists(path))
        # can't make a directory if it exists aleady
        assert_raises(HTTPError, lambda: sm.make_dir(path))

    # fails on an unknown type
    def test_get_unknown(self):
        sm = self.swiftmanager
        log.info("test_get_unknown starting")
        path = testDirectories[0]
        assert_raises(HTTPError, lambda: sm.get(path, type='random_text', content=None) )

    # tests getting a directory: with & without content; with & without the type value defined
    def test_get_directory(self):
        sm = self.swiftmanager
        log.info("test_get_directory starting")
        path = testDirectories[0]

        # Specify it's a directory
        data = sm.get(path, type='directory', content=False)
        assert_true( data['content'] == None)
        data = sm.get(path, type='directory', content=True)
        assert_true( len(data['content']) > 0)

        # code deduces it's a directory
        data = sm.get(path, content=False)
        assert_true( data['content'] == None)
        data = sm.get(path, content=True)
        assert_true( len(data['content']) > 0)

    # tests getting a file: with & without content; with & without the type value defined
    def test_get_file(self):
        sm = self.swiftmanager
        log.info("test_get_file starting")
        path = testDirectories[0]+testFileName

        # Specify it's a file 
        data = sm.get(path, type='file', content=False)
        assert_true( data['content'] == None)
        data = sm.get(path, type='file', content=True)
        assert_true( data['content'] == testFileContent)

        # deduce it's a file 
        data = sm.get(path, content=False)
        assert_true( data['content'] == None)
        data = sm.get(path, content=True)
        assert_true( data['content'] == testFileContent)

    # tests getting a notebook: with & without content; with & without the type value defined
    def test_get_notebook(self):
        sm = self.swiftmanager
        log.info("test_get_notebook starting")
        path = testDirectories[0]+testNotebookName
        self.swiftmanager.swiftfs.write(path, json.dumps(testNotebookContent) )

        # Specify it's a notebook 
        data = sm.get(path, type='notebook', content=False)
        assert_true( data['content'] == None)
        data = sm.get(path, type='notebook', content=True)
        assert_true( data['content'] == testNotebookContent )

        # deduce it's a notebook 
        data = sm.get(path, content=False)
        assert_true( data['content'] == None)
        data = sm.get(path, content=True)
        assert_true( data['content'] == testNotebookContent)

    # tests that save raises errors for no type given & invalid type given
    def test_save_errors(self):
        sm = self.swiftmanager
        log.info("test_save_errors starting")
        path = testDirectories[1]+testFileName+'_2'
        # fail: no type given
        model={'content': testFileContent}
        assert_raises(HTTPError, lambda: sm.save(model, path) )
        # fail: invalid type given
        model={'content': testFileContent, 'type': 'random_text'}
        assert_raises(HTTPError, lambda: sm.save(model, path) )

    # tests saving a directory (with & without a trailing slash)
    def test_save_directory(self):
        sm = self.swiftmanager
        log.info("test_save_directory starting")
        path = testDirectories[1].rstrip('/')+'_2/'
        model={'type': 'directory'}
        returned_model = sm.save(model, path)
        assert_true( returned_model['path'] == path )
        assert_true( sm.dir_exists(path) )
        # this adds the trailing slash to the object,
        path = testDirectories[1].rstrip('/')+'_3'
        returned_model = sm.save(model, path)
        assert_true( returned_model['path'] == path + '/')
        assert_true( sm.dir_exists(path) )

    # tests saving a file
    def test_save_file(self):
        sm = self.swiftmanager
        log.info("test_save_file starting")
        path = testDirectories[1]+testFileName+'_2'
        model={'content': testFileContent, 'type': 'file'}
        returned_model = sm.save(model, path)
        assert_true( sm.file_exists(path) )

    # tests saving a notebook
    def test_save_notebook(self):
        sm = self.swiftmanager
        log.info("test_save_notebook starting")
        path = testDirectories[1] + testNotebookName
        model={'content': testNotebookContent, 'type': 'notebook'}
        returned_model = sm.save(model, path)
        assert_true( sm.file_exists(path) )

        # Get the model with 'content'
        data = sm.get(path)
        assert_true( data['content'] == testNotebookContent )

    def test_rename_file(self):
        sm = self.swiftmanager
        log.info("test_rename_file starting")
        from_path = testDirectories[1]+testFileName
        to_path = testDirectories[1]+testFileName+'_2'

        # rename non-existant file should fail
        assert_raises(HTTPError, lambda: sm.rename_file(to_path, to_path) )
        # rename to an existing file should fail
        assert_raises(HTTPError, lambda: sm.rename_file(from_path, from_path) )
        assert_true( sm.file_exists(from_path) )
        assert_false( sm.file_exists(to_path) )
        sm.rename_file(from_path, to_path)
        assert_false( sm.file_exists(from_path) )
        assert_true( sm.file_exists(to_path) )

    def test_rename(self):
        sm = self.swiftmanager
        log.info("test_rename starting")
        from_path = testDirectories[1]+testFileName
        to_path = testDirectories[1]+testFileName+'_2'

        # rename non-existant file should fail
        assert_raises(HTTPError, lambda: sm.rename_file(to_path, to_path) )
        # rename to an existing file should fail
        assert_raises(HTTPError, lambda: sm.rename_file(from_path, from_path) )
        assert_true( sm.file_exists(from_path) )
        assert_false( sm.file_exists(to_path) )
        sm.rename_file(from_path, to_path)
        assert_false( sm.file_exists(from_path) )
        assert_true( sm.file_exists(to_path) )

    def test_delete_file(self):
        sm = self.swiftmanager
        log.info("test_delete_file starting")
        path = testDirectories[1]+testFileName
        assert_true( sm.file_exists(path) )
        sm.delete_file(path)
        assert_false( sm.file_exists(path) )

    def test_delete(self):
        sm = self.swiftmanager
        log.info("test_delete starting")
        path = testDirectories[1]+testFileName
        assert_true( sm.file_exists(path) )
        sm.delete_file(path)
        assert_false( sm.file_exists(path) )

