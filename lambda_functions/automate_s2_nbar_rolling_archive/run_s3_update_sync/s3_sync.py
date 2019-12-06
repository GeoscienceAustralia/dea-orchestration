import logging
import re
import sys
import zipfile

from dateutil.parser import parse as date_parser
from os.path import join as pjoin, basename
from pathlib import Path
from xml.etree import ElementTree
from wagl.acquisition import acquisitions


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)
PATTERN2 = re.compile('(L1[GTPCS]{1,2})')
ARD = 'ARD'
DEFAULT_PKGDIR = '/g/data/if87/datacube/002/S2_MSI_ARD/packaged'
METADATA_FILE = 'ARD-METADATA.yaml'
S3_PREFIX_PATH = 'L2/sentinel-2-nbar/S2MSIARD_NBAR'


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


def process_granules(level1_done_filepath):
    ret_list, l1_list = list(), list()
    l1_list.append(level1_done_filepath.strip(','))

    for done_file in l1_list:
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
            ret_list.append(metadata_path)

        # Rename the file after processing, to avoid duplicate dataset generation during the work flow
        dest_path = str(Path(done_file).parent / Path(done_file).stem) + "_processed.txt"
        Path(done_file).rename(dest_path)

    print(",".join(ret_list))


if __name__ == '__main__':
    process_granules(sys.argv[1])
