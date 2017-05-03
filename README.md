
[![Build Status](https://travis-ci.org/danielfrg/SwiftContents.svg?branch=master)](https://travis-ci.org/danielfrg/SwiftContents)
[![Coverage Status](https://coveralls.io/repos/github/danielfrg/SwiftContents/badge.svg?branch=master)](https://coveralls.io/github/danielfrg/SwiftContents?branch=master)

# SwiftContents

A backed ContentsManager implementation for Jupyter which writes data to the Swift impliemtnation on OpenStack.

It aims to a be a transparent, drop-in replacement for Jupyter standard filesystem-backed storage system.
With this implementation of a Jupyter Contents Manager you can save all your notebooks, regular files, directories
structure directly to OpenStack Volumes

While there are some implementations for S3 functionality already available online [2] I was unable to find something for Swift.

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
