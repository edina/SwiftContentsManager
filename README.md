
[![Build Status](https://travis-ci.org/danielfrg/SwiftContents.svg?branch=master)](https://travis-ci.org/danielfrg/SwiftContents)
[![Coverage Status](https://coveralls.io/repos/github/danielfrg/SwiftContents/badge.svg?branch=master)](https://coveralls.io/github/danielfrg/SwiftContents?branch=master)

# SwiftContents

A backed ContentsManager implementation for Jupyter which writes data to the Swift impliemtnation on OpenStack.

It aims to a be a transparent, drop-in replacement for Jupyter standard filesystem-backed storage system.
With this implementation of a Jupyter Contents Manager you can save all your notebooks, regular files, directories
structure directly to OpenStack Volumes

While there are some implementations for S3 functionality already available online [2] I was unable to find something for Swift.

## How it works

Swift is an object store. The documentation says _do not treat swift as a filestore_ .... but, hey, we can ignore that, can't we?

So each _user_ has their own **container** within the store (and the SwiftClient package restricts access to specific containers).

Each file is just an object in the store, and objects are refered to with a resource path. Quoting https://docs.openstack.org/developer/swift/api/object_api_v1_overview.html

>For example, for the flowers/rose.jpg object in the images container in the 12345678912345 account, the resource path is:
>
> `/v1/12345678912345/images/flowers/rose.jpg`
>
>Notice that the object name contains the / character. This slash does not indicate that Object Storage has a sub-hierarchy called flowers because containers do not store objects in actual sub-folders. However, the inclusion of / or a similar convention inside object names enables you to create pseudo-hierarchical folders and directories.

... therefore we can simply create (and re-create) objects to represent files (and a directory is a path with just one object: `.`)

### Conventions

* There's just one account that authenticates to Swift
* Each notebook user (well, `uuid`, probably obfuscated) has their own container
* Each user is is responsible for organising their own data
* A directory is just an object whos path ends in a slash: `/images/flowers/`
* A file is just an object whos path does **not** end in a slash: `/images/flowers/rose.jpg`

### Further info

My main reference for Swift has been https://docs.openstack.org/developer/python-swiftclient/service-api.html

My main reference for `ContentsManager` has been http://jupyter-notebook.readthedocs.io/en/latest/extending/contents.html

* `ContentsManager.get` => `swift.download( container=container, objects=objects)`
* `ContentsManager.save` => `swift.upload(container, objs + dir_markers)`
* `ContentsManager.delete_file` => `swift.delete(container=container, objects=objects)`
* `ContentsManager.rename_file` => `swift.copy(container, ["a"], ["b"])`
* `ContentsManager.file_exists` => `swift.stat(container=container, objects=objects)` - ensure there's no longer path (?somehow?)
* `ContentsManager.dir_exists` => (as `file_exists`)
* `ContentsManager.is_hidden` => (we can't hide files.. _always returns false_

## Prerequisites

Write access (valid credentials) to an OpenStack system, with existing Volumes.

## Installation

```
$ pip install swiftcontents
```

## Jupyter config

Edit `~/.jupyter/jupyter_notebook_config.py` by filling the missing values:

```python
from swiftcontents import SwiftContentsManager

c = get_config()

# Tell Jupyter to use SwiftContentsManager for all storage.
c.NotebookApp.contents_manager_class = SwiftContentsManager
c.SwiftContentsManager.access_key_id = <AWS Access Key ID / IAM Access Key ID>
c.SwiftContentsManager.secret_access_key = <AWS Secret Access Key / IAM Secret Access Key>
c.SwiftContentsManager.bucket_name = "<>"
```

## See also

1. [PGContents](https://github.com/quantopian/pgcontents) - The reference
2. [SwiftContents](https://github.com/danielfrg/SwiftContents) (my inspiration for this code), [s3nb](https://github.com/monetate/s3nb) or [s3drive](https://github.com/stitchfix/s3drive)
