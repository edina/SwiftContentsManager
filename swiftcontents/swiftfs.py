"""
Utilities to make Swift look like a regular file system
"""
import six
import os
from swiftclient.service import SwiftService, SwiftError
from keystoneauth1 import session
from keystoneauth1.identity import v3
from traitlets import default, Unicode, Any, Instance

class SwiftFS():

    # Keystone has standardized on the term **project**
    # as the entity that owns the resources    
    os_auth_url = Unicode('',
        help="OpenStack Authentication URL",
        config = True
    )
    os_project_id = Unicode('',
        help="ID for the 'project' within Swift",
        config = True
        )
    os_project_name = Unicode('',
        help="name for the 'project' within the Swift store",
        config = True
        )
    os_region_name = Unicode('',
        help="name for the 'region' within the Swift store",
        config = True
        )
    os_user_domain_name = Unicode('',
        help="The 'domain' for the user within Swift",
        config = True
        )
    os_username = Unicode('',
        help="The username for connecting to the Swift system",
        config = True
        )
    os_password = Unicode('',
        help="The password for the user connecting to the Swift system",
        config = True
        )
    # hard-coded values
    os_identity_api_version = Unicode('3')
    os_interface = Unicode('public')

    notebook_user = Unicode('test_account',
        help="The user who's starting the notebook",
        config = True
        )
    storage_url = Unicode('',
        help="The base URL for containers",
        config = True
        )

    #swift_connection = Any()
    #swift_connection = Instance(
    #    klass = 'swiftclient.client.Connection'
    #    )

    delimiter = Unicode("/", help="Path delimiter", config=True)

    root_dir = Unicode("./", config=True)

    def __init__(self, log, **kwargs):
        super(SwiftFS, self).__init__(**kwargs)
        self.log = log

        # With the python swift client, the connection is automagically
        # created using environment variables (I know... horrible or what?)
        # What this block does is just ensure that all the environment variables
        # are set to the values we need for this user.
        if self.os_auth_url:
            os.environ['OS_AUTH_URL'] = str(self.os_auth_url)
            self.log.debug("SwiftContents[SwiftFS] exporting " + self.os_auth_url )
        if self.os_project_id:
            os.environ['OS_PROJECT_ID'] = str(self.os_project_id)
            self.log.debug("SwiftContents[SwiftFS] exporting " + self.os_project_id )
        if self.os_project_name:
            os.environ['OS_PROJECT_NAME'] = str(self.os_project_name)
            self.log.debug("SwiftContents[SwiftFS] exporting " + self.os_project_name )
        if self.os_region_name:
            os.environ['OS_REGION_NAME'] = str(self.os_region_name)
            self.log.debug("SwiftContents[SwiftFS] exporting " + self.os_region_name )
        if self.os_user_domain_name:
            os.environ['OS_USER_DOMAIN_NAME'] = str(self.os_user_domain_name)
            self.log.debug("SwiftContents[SwiftFS] exporting " + self.os_user_domain_name)
        if self.os_username:
            os.environ['OS_USERNAME'] = str(self.os_username)
            self.log.debug("SwiftContents[SwiftFS] exporting " + self.os_usernamel )
        if self.os_password:
            os.environ['OS_PASSWORD'] = str(self.os_password)
            self.log.debug("SwiftContents[SwiftFS] exporting " + self.os_password )
        if self.os_identity_api_version:
            os.environ['OS_IDENTITY_API_VERSION'] = str(self.os_identity_api_version)
            self.log.debug("SwiftContents[SwiftFS] exporting " + self.os_identity_api_version )
        if self.os_interface:
            os.environ['OS_INTERFACE'] = str(self.os_interface)
            self.log.debug("SwiftContents[SwiftFS] exporting " + self.os_interface )

        # Ensure there's a container for this user
        with SwiftService() as swift:
            try:
                list_parts_gen = swift.list(container=self.notebook_user)
                for page in list_parts_gen:
                    if not page["success"]:
                        raise page["error"]

            except SwiftError as e:
                logger.error(e.value)
                auth = v3.Password(auth_url=os.environ['OS_AUTH_URL'],
                                   username=os.environ['OS_USERNAME'],
                                   password=os.environ['OS_PASSWORD'],
                                   user_domain_name=os.environ['OS_USER_DOMAIN_NAME'],
                                   project_name=os.environ['OS_PROJECT_NAME'],
                                   project_domain_name=os.environ['OS_USER_DOMAIN_NAME'])
                keystone_session = session.Session(auth=auth)
                #self.swift_connection = swiftclient.client.Connection(session=keystone_session)
                #self.swift_connection.put_container(self.notebook_user)

        self.delimiter = "/"

        if self.prefix:
            self.mkdir("")

    def get_keys(self, prefix=""):
        ret = []
        for obj in self.bucket.objects.filter(Prefix=prefix):
            ret.append(obj.key)
        return ret

    def listdir(self, path="", with_prefix=False):
        self.log.debug("SwiftContents[SwiftFS] Listing directory: `%s`", path)
        #return map(self.as_path, files)

    def isfile(self, path):
        self.log.debug("SwiftContents[SwiftFS] Checking if `%s` is a file", path)
        #return is_file

    def isdir(self, path):
        self.log.debug("SwiftContents[SwiftFS] Checking if `%s` is a directory", path)
        #return is_dir

    def mv(self, old_path, new_path):
        self.cp(old_path, new_path)
        self.rm(old_path)

    def cp(self, old_path, new_path):
        self.log.debug("SwiftContents[SwiftFS] Copy `%s` to `%s`", old_path, new_path)

    def rm(self, path):
        self.log.debug("SwiftContents[SwiftFS] Deleting: `%s`", path)

    def mkdir(self, path):
        self.log.debug("SwiftContents[SwiftFS] Making dir: `%s`", path)

    def read(self, path):
        return path #text

    def write(self, path, content):
        key = self.as_key(path)



class SwiftFSError(Exception):
    pass


class NoSuchFile(SwiftFSError):

    def __init__(self, path, *args, **kwargs):
        super(NoSuchFile, self).__init__(*args, **kwargs)
        self.path = path
