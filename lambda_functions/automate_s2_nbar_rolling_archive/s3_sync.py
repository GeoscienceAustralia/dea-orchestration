import glob
import logging
import re
import sys
import uuid
import yaml
import zipfile

from botocore.exceptions import ClientError
from dateutil.parser import parse as date_parser
from os.path import join as pjoin, basename
from pathlib import Path
from xml.etree import ElementTree
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper
from wagl.acquisition import acquisitions


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)
PATTERN2 = re.compile('(L1[GTPCS]{1,2})')
ARD = 'ARD'
DEFAULT_PKGDIR = '/g/data/if87/datacube/002/S2_MSI_ARD/packaged'
METADATA_FILE = 'ARD-METADATA.yaml'
S3_PREFIX_PATH = 'L2/sentinel-2-nbar/S2MSIARD_NBAR'


def upload_to_s3(s3_client, file_name, bucket, key, ymd):
    """
       Upload a file to an S3 bucket
    """
    try:
        path_list = Path(file_name).parts
    except AttributeError:
        LOG.error("Error processing ", file_name)
        return True

    logging.info('Sync ' + file_name + ' file to S3')
    if 'ARD-METADATA' in file_name:
        obj_name = "/".join([S3_PREFIX_PATH, ymd, key, METADATA_FILE])
    else:
        idx = path_list.index(key)
        obj_name = "/".join((S3_PREFIX_PATH, ymd,) + path_list[idx:])

    try:
        # The upload_file method is handled by the S3 Transfer Manager,
        # this means that it will automatically handle multipart uploads behind the scenes for you, if necessary.
        s3_client.upload_file(file_name, bucket, obj_name)
        return True
    except ClientError as e:
        logging.error(e)
        return False


def sync_granules(s3_client, bucket_name, dir_path, ymd):
    """
    Uploads the given file using a managed uploader, which will split up large
    files automatically and upload parts in parallel
    """
    allfiles = [Path(f) for f in glob.glob(dir_path + "/*/*", recursive=True)
                if "NBART" not in f] + \
               [f for f in Path(dir_path).iterdir() if f.is_file()]

    key = Path(dir_path).parts[-1]

    for file_name in allfiles:
        if "ARD-METADATA.yaml" in str(file_name):
            update_metadatafile(s3_client, file_name, bucket_name, key, ymd)
        else:
            # Upload a file
            response = upload_to_s3(s3_client, str(file_name), bucket_name, key, ymd)
            if response:
                LOG.info(f'{file_name} file uploaded')


def extract_granule_names(pathname):
    """ Code to extract granule names from L1C Metadata, and processing baseline
    Logic has been ported from wagl.acquisition.__init__
    to avoid the overhead of caching the per measurement metadata
    methods in wagl.acquisition should be refactored to export
    this functionality
    returns a list of granule names
    """
    archive = zipfile.ZipFile(str(pathname))
    xmlfiles = [s for s in archive.namelist() if "MTD_MSIL1C.xml" in s]
    if not xmlfiles:
        pattern = basename(str(pathname).replace('PRD_MSIL1C', 'MTD_SAFL1C'))
        pattern = pattern.replace('.zip', '.xml')
        xmlfiles = [s for s in archive.namelist() if pattern in s]

    mtd_xml = archive.read(xmlfiles[0])
    xml_root = ElementTree.XML(mtd_xml)

    search_term = './*/Product_Info/Product_Organisation/Granule_List/Granules'
    grn_elements = xml_root.findall(search_term)

    # handling multi vs single granules + variants of each type
    if not grn_elements:
        grn_elements = xml_root.findall(search_term[:-1])

    if grn_elements[0].findtext('IMAGE_ID'):
        search_term = 'IMAGE_ID'
    else:
        search_term = 'IMAGE_FILE'

    # required to identify granule metadata in a multigranule archive
    # in the earlier l1c products
    processing_baseline = xml_root.findall('./*/Product_Info/PROCESSING_BASELINE')[0].text

    results = {}
    for granule in grn_elements:
        gran_id = granule.get('granuleIdentifier')
        if not pathname.suffix == '.zip':
            gran_path = str(pathname.parent.joinpath('GRANULE', gran_id, gran_id[:-7].replace('MSI', 'MTD') + '.xml'))
            root = ElementTree.parse(gran_path).getroot()
        else:
            xmlzipfiles = [s for s in archive.namelist() if 'MTD_TL.xml' in s]
            if not xmlzipfiles:
                pattern = gran_id.replace('MSI', 'MTD')
                pattern = pattern.replace('_N' + processing_baseline, '.xml')
                xmlzipfiles = [s for s in archive.namelist() if pattern in s]
            mtd_xml = archive.read(xmlzipfiles[0])
            root = ElementTree.XML(mtd_xml)
        sensing_time = root.findall('./*/SENSING_TIME')[0].text
        results[gran_id] = date_parser(sensing_time)

    return results


def update_metadatafile(s3_client, yaml_file, bucket_name, key, ymd):
    with open(yaml_file) as config_file:
        temp_metadata = yaml.load(config_file, Loader=Loader)

    temp_metadata['image']['bands'].pop('nbart_blue', None)
    temp_metadata['image']['bands'].pop('nbart_coastal_aerosol', None)
    temp_metadata['image']['bands'].pop('nbart_contiguity', None)
    temp_metadata['image']['bands'].pop('nbart_green', None)
    temp_metadata['image']['bands'].pop('nbart_nir_1', None)
    temp_metadata['image']['bands'].pop('nbart_nir_2', None)
    temp_metadata['image']['bands'].pop('nbart_red', None)
    temp_metadata['image']['bands'].pop('nbart_red_edge_1', None)
    temp_metadata['image']['bands'].pop('nbart_red_edge_2', None)
    temp_metadata['image']['bands'].pop('nbart_red_edge_3', None)
    temp_metadata['image']['bands'].pop('nbart_swir_2', None)
    temp_metadata['image']['bands'].pop('nbart_swir_3', None)
    temp_metadata.pop('lineage', None)
    temp_metadata['creation_dt'] = temp_metadata['extent']['center_dt']
    temp_metadata['product_type'] = 'S2MSIARD_NBAR'
    temp_metadata['id'] = str(uuid.uuid4())

    with open(METADATA_FILE, mode='w') as tf:
        yaml.dump(temp_metadata, tf, default_flow_style=False, Dumper=Dumper)

    # Upload a file
    response = upload_to_s3(s3_client, METADATA_FILE, bucket_name, key, ymd)
    if response:
        LOG.info(f'{METADATA_FILE} file uploaded')


def process_main(s3_client, s3_bucket, level1_done_filepath):
    for done_file in level1_done_filepath:
        level1_files = set(open(done_file))
        for level1 in level1_files:
            LOG.info(f"Input: {level1.strip()}")
            container = acquisitions(level1.strip())
            level1_granule = container.granules[0]
            s2_ard_granule = re.sub(PATTERN2, ARD, level1_granule)
            archive_md = extract_granule_names(Path(level1.strip()))
            sensing_time = list(archive_md.values())[0]
            ymd = sensing_time.strftime('%Y-%m-%d')
            metadata_path = pjoin(DEFAULT_PKGDIR, ymd, s2_ard_granule)
            LOG.info(f"Processed: {metadata_path}")
            LOG.info('\n')
            sync_granules(s3_client, s3_bucket, metadata_path, ymd)

        # Rename the file after processing, to avoid duplicate dataset generation during the work flow
        dest_path = str(Path(done_file).parent / Path(done_file).stem) + "_processed.txt"
        Path(done_file).rename(dest_path)


if __name__ == '__main__':
    process_main(sys.argv[0], sys.argv[1], sys.argv[2])
