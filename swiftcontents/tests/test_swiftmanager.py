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


    def setUp(self):
        log.info('setting up directory structure')
        for d in testDirectories:
            self.swiftmanager.swiftfs.mkdir(d)
        log.info('create a bunch of files')
        for d in testDirectories:
            p = d+testFileName
            self.swiftmanager.swiftfs.write(p, testFileContent)

    def teardown(self):
        log.info('tidy up directory structure')
        self.swiftmanager.swiftfs.rm(testDirectories[0],recursive=True)
        assert_false(self.swiftmanager.swiftfs.isdir(testDirectories[0]))

        log.info("--------------------------------------")

    # Test the central error-handler
    def test_do_error(self):
        sm = self.swiftmanager
        log.info("test_do_error starting")
        assert_raises(HTTPError, lambda: sm.do_error('error_text', 501))
        # really? Oh, OK then...
        assert_raises(HTTPError, lambda: sm.do_error('error_text', 999))

    # make the swiftfs test tree, then test it
    def test_dir_exists(self):
        sm = self.swiftmanager
        log.info("test_dir_exists starting")
        exists_dir = testDirectories[-1]
        exists_file = exists_dir + testFileName
        assert_true(sm.dir_exists(exists_dir))
        assert_false(sm.dir_exists(exists_file))

    # make the swiftfs test tree, then test it
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

    # tests make_dir (both success & fail)
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

    def test_save_file(self):
        sm = self.swiftmanager
        log.info("test_save_file starting")
        path = testDirectories[1]
        file_model_path = sm.new_untitled(path=path, ext='.txt')['path']
        file_model = cm.get(file_model_path)
        pprint(file_model)
        #self.assertDictContainsSubset(
        #    {
        #        'content': u'',
        #        'format': u'text',
        #        'mimetype': u'text/plain',
        #        'name': u'untitled.txt',
        #        'path': u'foo/untitled.txt',
        #        'type': u'file',
        #        'writable': True,
        #    },
        #    file_model,
        #)
        #self.assertIn('created', file_model)
        #self.assertIn('last_modified', file_model)
        raise Exception

    def test_save_notebook(self):
        sm = self.swiftmanager
        log.info("test_save_notebook starting")
        path = testDirectories[1].rstrip('/') + '_b/'
        model = cm.new_untitled(type='notebook')
        #name = model['name']
        #path = model['path']

        # Get the model with 'content'
        full_model = cm.get(path)
        pprint(full_model)
        raise Exception

    def test_swiftmanager_rename(self):
        sm = self.swiftmanager
        log.info("test_swiftmanager_rename starting")
        pass
    def test_swiftmanager_delete(self):
        sm = self.swiftmanager
        log.info("test_swiftmanager_delete starting")
        pass
    def test_swiftmanager_error_handling(self):
        sm = self.swiftmanager
        log.info("test_swiftmanager_error_handling starting")
        pass
