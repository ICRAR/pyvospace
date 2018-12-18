#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
"""
Main module where application-specific tasks are carried out, like copying its
sources, installing it and making sure it works after starting it.

NOTE: This requires modifications for the specific application where this
fabfile is used. Please make sure not to use it without those modifications.
"""
import os, sys
import time
from fabric.state import env
from fabric.colors import red
from fabric.operations import local
from fabric.decorators import task, parallel
from fabric.context_managers import settings, cd
from fabric.contrib.files import exists, sed
from fabric.utils import abort
from fabric.contrib.console import confirm

# >>> All the settings below are kept in the special fabric environment
# >>> dictionary called env. Don't change the names, only adjust the
# >>> values if necessary. The most important one is env.APP.

# The following variable will define the Application name as well as directory
# structure and a number of other application specific names.
env.APP_NAME = 'PYVOSPACE'

# The username to use by default on remote hosts where APP is being installed
# This user might be different from the initial username used to connect to the
# remote host, in which case it will be created first
env.APP_USER = env.APP_NAME.lower()

# Name of the directory where APP sources will be expanded on the target host
# This is relative to the APP_USER home directory
env.APP_SRC_DIR_NAME = env.APP_NAME.lower() + '_src'

# Name of the directory where APP root directory will be created
# This is relative to the APP_USER home directory
env.APP_ROOT_DIR_NAME = env.APP_NAME.upper()

# Name of the directory where a virtualenv will be created to host the APP
# software installation, plus the installation of all its related software
# This is relative to the APP_USER home directory
env.APP_INSTALL_DIR_NAME = env.APP_NAME.lower() + '_rt'

# Version of Python required for the Application
env.APP_PYTHON_VERSION = '3.6'

# URL to download the correct Python version
env.APP_PYTHON_URL = 'https://www.python.org/ftp/python/3.7.0/Python-3.7.0.tgz'

env.APP_DATAFILES = []
# >>> The following settings are only used within this APPspecific file, but may be
# >>> passed in through the fab command line as well, which will overwrite the 
# >>> defaults below.

defaults = {}

### >>> The following settings need to reflect your AWS environment settings.
### >>> Please refer to the AWS API documentation to see how this is working
### >>> 
## AWS user specific settings
env.AWS_PROFILE = 'NGAS'
env.AWS_KEY_NAME = 'icrar_{0}'.format(env.APP_USER)

# These AWS settings are generic and should work for any user, but please make
# sure that the instance_type is appropriate.
env.AWS_INSTANCE_TYPE = 't1.micro'
env.AWS_REGION = 'us-east-1'
env.AWS_AMI_NAME = 'Amazon'
env.AWS_INSTANCES = 1
env.AWS_SEC_GROUP = env.APP_NAME.upper() # Security group allows SSH and other ports
env.AWS_SUDO_USER = 'ec2-user' # required to install init scripts.

# Alpha-sorted packages per package manager
env.pkgs = {
            'YUM_PACKAGES': [
                    'wget',
                    'curl',
                    'tar',
                    'gcc',
                    'git',
                    'python36',
                    'docker',
                    'fuse',
                      ],
            'APT_PACKAGES': [
                    'tar',
                    'wget',
                    'gcc',
                    ],
            'SLES_PACKAGES': [
                    'wget',
                    'gcc',
                    ],
            'BREW_PACKAGES': [
                    'wget',
                    'gcc',
                    ],
            'PORT_PACKAGES': [
                    'wget',
                    'gcc',
                    ],
            'APP_EXTRA_PYTHON_PACKAGES': [
                    'fusepy'
                    ],
        }

# Don't re-export the tasks imported from other modules, only the ones defined
# here
__all__ = [
    'cleanup'
]

# Set the rpository root to be relative to the location of this file.
env.APP_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# >>> The following lines need to be after the definitions above!!!
from fabfileTemplate.utils import sudo, info, success, default_if_empty, home, run
from fabfileTemplate.utils import overwrite_defaults, failure
from fabfileTemplate.system import check_command, get_linux_flavor, MACPORT_DIR
from fabfileTemplate.APPcommon import virtualenv, APP_doc_dependencies, APP_source_dir
from fabfileTemplate.APPcommon import APP_root_dir, extra_python_packages, APP_user, build
from fabfileTemplate.pkgmgr import check_brew_port, check_brew_cellar

# get the settings from the fab environment if set on command line
fab_settings = overwrite_defaults(defaults)

def start_APP_and_check_status():
    """
    Starts the APP daemon process and checks that the server is up and running
    then it shuts down the server
    """
    with cd('{0}/pyvospace/server/deploy'.format(APP_source_dir())):
            # >>>> Darwin docker shows a permission issue with keychain access <<<<
            if get_linux_flavor() != 'Darwin':
                virtualenv('docker-compose build')
            else:
                info('>>>> Darwin reuqires to build docker container manually')
                info('>>>> docker-compose build')
            virtualenv(
            'docker run -d -p 5435:5432 pyvospace/pyvospace-db  -h 0.0.0.0')
            time.sleep(10)
    with cd('{0}'.format(APP_source_dir())):
        virtualenv('python -m unittest discover test')
#     run('mkdir -p /tmp/fuse')
#     virtualenv('posix_space --cfg test_vo.ini > /tmp/space.log 2>&1')
#     time.sleep(2)
#     virtualenv('posix_storage --cfg test_vo.ini > /tmp/storage.log 2>&1')
#     time.sleep(2)
#     virtualenv('python -m pyvospace.client.fuse --host localhost --port 8080 --username test --password test --mountpoint /tmp/fuse/`` > /tmp/fusemnt.log 2>&1')
#     time.sleep(2)
# run("cd /tmp/fuse && mkdir -p newdir && cd newdir && echo 'Hello World!' >> data && cat data")
    success('{0} is working...'.format(env.APP_NAME))

def sysinitstart_APP_and_check_status():
    """
    Starts the APP daemon process and checks that the server is up and running
    then it shuts down the server
    """
    ###>>> 
    # The following just runs the DB in a docker container and runs the tests
    ###<<<
    nuser = APP_user()
    with settings(user=nuser):
        with cd('{0}'.format(APP_source_dir())):
        #    virtualenv('python -m unittest discover test')
        #     run('mkdir -p /tmp/fuse')
        #     virtualenv('posix_space --cfg test_vo.ini > /tmp/space.log 2>&1')
        #     time.sleep(2)
        #     virtualenv('posix_storage --cfg test_vo.ini > /tmp/storage.log 2>&1')
        #     time.sleep(2)
        #     virtualenv('python -m pyvospace.client.fuse --host localhost --port 8080 --username test --password test --mountpoint /tmp/fuse/`` > /tmp/fusemnt.log 2>&1')
        #     time.sleep(2)
        # run("cd /tmp/fuse && mkdir -p newdir && cd newdir && echo 'Hello World!' >> data && cat data")
            pass
    success("{0} successfully tested!".format(env.APP_NAME))


def APP_build_cmd():

    build_cmd = [
            'pip install .',
            'cd pyvospace/server/deploy',
#            'docker-compose build',
#            'docker-compose up'
            ]
    return ' && '.join(build_cmd)


# def build_APP():
#     """
#     Builds and installs APP into the target virtualenv.
#     """
#     with cd(APP_source_dir()):
#         extra_pkgs = extra_python_packages()
#         if extra_pkgs:
#             virtualenv('pip install %s' % ' '.join(extra_pkgs))
#         develop = False
#         no_doc_dependencies = APP_doc_dependencies()
#         build_cmd = APP_build_cmd()
#         print build_cmd
#         if build_cmd != '':
#             virtualenv(build_cmd)
#     success("{0} built and installed".format(env.APP_NAME))


def prepare_APP_data_dir():
    """Creates a new APP root directory"""

    ###>>> 
    # Provide the actual implementation here if required.
    ###<<<

def install_sysv_init_script(nsd, nuser, cfgfile):
    """
    Install the APP init script for an operational deployment.
    The init script is an old System V init system.
    In the presence of a systemd-enabled system we use the update-rc.d tool
    to enable the script as part of systemd (instead of the System V chkconfig
    tool which we use instead). The script is prepared to deal with both tools.
    """
    ###>>> 
    # Provide the actual implementation here if required.
    ###<<<

    success("{0} init script installed".format(env.APP_NAME))

@task
@parallel
def cleanup():
    ###>>> 
    # Provide the actual implementation here if required.
    ###<<<
    pass

def install_docker_compose():
    print(">>>>> Installing docker-compose <<<<<<<<")
    sudo('curl -L "https://github.com/docker/compose/releases/download/1.23.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose')
    sudo('chmod +x /usr/local/bin/docker-compose')
    sudo('usermod -aG docker pyvospace')
    sudo('service docker start')
    time.sleep(5)
    sudo("sudo sed -ie 's/# user_allow_other/user_allow_other/g' /etc/fuse.conf")
    

env.build_cmd = APP_build_cmd
env.APP_init_install_function = install_sysv_init_script
env.APP_start_check_function = start_APP_and_check_status
env.sysinitAPP_start_check_function = sysinitstart_APP_and_check_status
env.prepare_APP_data_dir = prepare_APP_data_dir
env.APP_extra_sudo_function = install_docker_compose
