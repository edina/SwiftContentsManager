
# SwiftContents

A back-end ContentsManager implementation for Jupyter which writes data to the Swift impliemtnation on OpenStack.

It aims to a be a transparent, drop-in replacement for Jupyter standard filesystem-backed storage system.
With this implementation of a Jupyter Contents Manager you can save all your notebooks, regular files, directories
structure directly to an OpenStack Volume

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
* `ContentsManager.is_hidden` => (we can't hide files.. _always returns false_ )

## Prerequisites

Write access (valid credentials) to an OpenStack system, with existing Volumes.

## Installation

```
$ git clone https://github.com/edina/SwiftContentsManager.git
$ cd SwiftContentsmanager
$ pip install -r requirements.txt
$ pip install .
```

## Testing
Testing has been written to run in a Docker Container - thus be an isolated environment

```
docker build -t naas/swifttest .
docker run --rm -it -e OS_AUTH_URL='https://keystone.ecdf.ed.ac.uk/v3' \
-e OS_PROJECT_ID='0edbdcab2cda436a90ab73a26b77c83e' \
-e OS_PROJECT_NAME='edina' \
-e OS_REGION_NAME='edina' \
-e OS_USER_DOMAIN_NAME='ed' \
-e OS_USERNAME='jupyter' \
-e OS_PASSWORD='edina123' \
-e OS_IDENTITY_API_VERSION='v3' \
-e OS_INTERFACE='public' naas/swifttest
```
### Testing whilst developing

Change the `docker run` command to include a _volume_:

```
docker run --rm -it -e OS_AUTH_URL='https://keystone.ecdf.ed.ac.uk/v3' \
-e OS_PROJECT_ID='0edbdcab2cda436a90ab73a26b77c83e' \
-e OS_PROJECT_NAME='edina' \
-e OS_REGION_NAME='edina' \
-e OS_USER_DOMAIN_NAME='ed' \
-e OS_USERNAME='jupyter' \
-e OS_PASSWORD='edina123' \
-e OS_IDENTITY_API_VERSION='v3' \
-e OS_INTERFACE='public' \
-v $(realpath .):/home/jovyan/work/SwiftContentsManager naas/swifttest /bin/bash
```

and this means you can run tests with extra parameters:

```
py.test -v --debug swiftcontents/tests/test_swiftmanager.py > debug.out 2>&1
```
(and you can review `debug.out` on your local workstation disk)

or even edit the code locally and re-install it:

```
pip uninstall swiftcontentsmanager
pip install .
```

to update the installed code on the Docker & re-run the tests

## See also

1. [PGContents](https://github.com/quantopian/pgcontents) - The reference
2. [SwiftContents](https://github.com/danielfrg/SwiftContents) (my inspiration for this code), [s3nb](https://github.com/monetate/s3nb) or [s3drive](https://github.com/stitchfix/s3drive)
