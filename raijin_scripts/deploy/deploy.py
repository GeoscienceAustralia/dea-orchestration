#!/usr/bin/env python3
# coding: utf-8

import os
import shutil
import sys
import argparse
from pathlib import Path
import subprocess
import jinja2
import stat
import boto3
import botocore

USER = os.environ.get('USER', 'avin8534')

SCRIPT_DIR = str(Path(__file__).parents[0].absolute())
TEMP_DIR = os.environ.get('TMPDIR', '/short/v10/{}/tmp'.format(USER))
MODULE_DIR = '/g/data/v10/public/modules'

SRC_NAME = 'module_template.j2'
BUCKET_NAME = 'datacube-core-deployment'
PIP_EXE = '/g/data/v10/public/modules/agdc-py3-env/20170627/bin/pip'


def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Pass the object of the corresponding bucket')
    parser.add_argument('object_name', help='object path/object key')
    result = parser.parse_args(args)
    return result.object_name


def generate_template_context(obj_key):
    '''
    Derive the module_path, install root and the checkout path from the object key
    :param obj_key: object ket of S3 bucket
    :return: lists of path needed for execution
    '''
    name = (str(obj_key)).split('/')
    module_name = name[1]
    module_version = name[2]
    python_version = name[3]
    file_name = name[4]
    install_root = os.path.join(MODULE_DIR, module_name, module_version)
    checkout_path = os.path.join(TEMP_DIR, module_name)
    whl_path = os.path.join(checkout_path, file_name)
    python_path = os.path.join(install_root, 'lib', 'python' + python_version, 'site-packages')
    module_dest = os.path.join(MODULE_DIR, 'modulefiles', module_name)
    module_dest_file = os.path.join(module_dest, module_version)

    template_context = {
        'modules_path': MODULE_DIR,
        'module_name': module_name,
        'module_version': module_version,
        'install_root': install_root,
        'checkout_path': checkout_path,
        'python_path': python_path,
        'whl_path': whl_path,
        'module_dest': module_dest,
        'module_dest_file': module_dest_file,
    }

    return template_context


def deploy_package(get_path, obj_key):
    # get all the paths as a local variable

    install_root = get_path.get('install_root')
    checkout_path = get_path.get('checkout_path')
    python_path = get_path.get('python_path')
    whl_path = get_path.get('whl_path')

    os.environ['PYTHONPATH'] = python_path
    try:
        shutil.rmtree(checkout_path)
    except FileNotFoundError:
        pass

    os.makedirs(checkout_path)

# change directory to temp directory
    os.chdir(checkout_path)

# Download tarzip file from s3
    s3_client = boto3.client('s3')

    try:
        s3_client.download_file(BUCKET_NAME, obj_key, whl_path)
        print("Requested file saved at : %s" % whl_path)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

# Ensure the destination directory exists
    if not os.path.isdir(install_root):
        os.makedirs(install_root)
# change the permission to user rwx
    os.chmod(install_root, 0o700)

    if not os.path.isdir(python_path):
        os.makedirs(python_path)

# install tarball package with pip
    package = (
        PIP_EXE + " install file://" + whl_path + " --prefix " +
        install_root + " --no-deps --global-option=build" +
        " --global-option='--executable=/usr/bin/env python'"
    )
    subprocess.run(package, shell=True)

# Remove write permissions
    os.chmod(install_root, 0o555)

# cleanup_tarball
    try:
        shutil.rmtree(checkout_path)
    except FileNotFoundError:
        pass

    return 'success'


def run(template_directory, template_context):

    module_dest = template_context.get('module_dest')
    module_dest_file = template_context.get('module_dest_file')

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(template_directory))
    if not os.path.isdir(module_dest):
        os.makedirs(module_dest)
    tmpl = env.get_template(SRC_NAME)
    with open(module_dest_file, 'w') as fd:
        fd.write(tmpl.render(**template_context))
    os.chmod(module_dest_file, 0o660)
    return True


if __name__ == '__main__':
    KEY = check_arg(sys.argv[1:])
    TEMPLATE_CONTEXT = generate_template_context(KEY)
    deploy_package(TEMPLATE_CONTEXT, KEY)
    run(SCRIPT_DIR, TEMPLATE_CONTEXT)
