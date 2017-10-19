#!/usr/bin/env python3
# coding: utf-8

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import boto3
import botocore
import jinja2

USER = os.environ['USER']

SCRIPT_DIR = Path(__file__).parent.absolute()
INSTALL_NUMPY_PATH = SCRIPT_DIR / 'venv'
TEMP_DIR = Path(os.environ.get('TMPDIR', f'/short/v10/{USER}/tmp'))
MODULE_DIR = Path('/g/data/v10/public/modules')

SRC_NAME = 'module_template.j2'
BUCKET_NAME = 'datacube-core-deployment'
PIP_EXE = 'python -m pip'


def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Pass the object of the corresponding bucket')
    parser.add_argument('object_name', help='object path/object key')
    result = parser.parse_args(args)
    return result.object_name


def generate_template_context(s3_object):
    """
    Derive the module_path, install root and the checkout path from the object key
    :param s3_object: object key of S3 bucket
    :return: lists of path needed for execution
    """

    # name is of the format: opendatacube/datacube-core/datacube-1.5.1/3.6/datacube-1.5.1.tar.gz
    # <ignored>/{module_name}/<ignored>-{module_version}/{python-version}/{file_name}
    name = (str(s3_object)).split('/')
    module_name = name[1]
    module_version = name[2].split('-')[-1]
    python_version = name[3]
    file_name = name[4]
    install_root = MODULE_DIR / module_name / module_version
    checkout_path = TEMP_DIR / module_name
    whl_path = checkout_path / file_name
    python_path = install_root / 'lib' / ('python' + python_version) / 'site-packages'
    module_dest = MODULE_DIR / 'modulefiles' / module_name
    module_dest_file = module_dest / module_version
    numpy_pythonpath = INSTALL_NUMPY_PATH / 'lib' / ('python' + python_version) / 'site-packages'

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
        'numpy_path': numpy_pythonpath,
    }

    print(json.dumps({k: str(v) for k, v in template_context.items()}))

    return template_context


def deploy_package(template_context, s3_object):
    install_root = template_context.get('install_root')
    checkout_path = template_context.get('checkout_path')
    python_path = template_context.get('python_path')
    whl_path = template_context.get('whl_path')
    numpy_path = template_context.get('numpy_path')

    # Temporarily change the Pythonpath to numpy installed path and revert back
    # Some packages won't install unless numpy is already installed
    os.environ['PYTHONPATH'] = str(numpy_path)

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
        s3_client.download_file(BUCKET_NAME, s3_object, str(whl_path))
        print(f"Requested file saved at : {whl_path}")
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

    # Install numpy in a temporary location for the fc setup
    subprocess.run(f"{PIP_EXE} install numpy --prefix {INSTALL_NUMPY_PATH}", shell=True)

    # install tarball package with pip
    package = (
        f"{PIP_EXE} install file://{whl_path} --prefix {install_root}"
        f" --no-deps --global-option=build" +
        f" --global-option='--executable=/usr/bin/env python'"
    )
    subprocess.run(package, shell=True)

    # Remove write permissions
    os.chmod(install_root, 0o555)

    # Change the PYTHONPATH to destination folder path
    os.environ['PYTHONPATH'] = python_path

    # cleanup_tarball
    try:
        shutil.rmtree(checkout_path)
    except FileNotFoundError:
        pass

    return 'success'


def run(template_directory, template_context):
    module_dest = template_context['module_dest']
    module_dest_file = template_context['module_dest_file']

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(template_directory)))
    if not os.path.isdir(module_dest):
        os.makedirs(module_dest)
    if os.path.exists(module_dest_file):
        os.chmod(module_dest_file, 0o640)
    template = env.get_template(SRC_NAME)
    with open(module_dest_file, 'w') as fd:
        fd.write(template.render(**template_context))
    os.chmod(module_dest_file, 0o440)
    return True


def main():
    s3_object = check_arg(sys.argv[1:])
    template_context = generate_template_context(s3_object)
    deploy_package(template_context, s3_object)
    run(SCRIPT_DIR, template_context)


if __name__ == '__main__':
    main()
