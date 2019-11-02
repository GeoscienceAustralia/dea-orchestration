import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.request import urlopen

import dateutil.parser

del os.environ['http_proxy']

URLS = {
    'gsky' : "http://gsky.nci.org.au/ows?service=WMS&version=1.3.0&request=GetCapabilities",
    'ows' : "https://ows.services.dea.ga.gov.au/?service=WMS&version=1.3.0&request=GetCapabilities"
}

NS = {'wms': 'http://www.opengis.net/wms'}


def layer_name_and_age(node, now):
    layer_name = node.find('wms:Name', NS).text
    dates = node.find('wms:Dimension', NS).text
    latest = dates.split(',')[-1]
    latest_date = dateutil.parser.parse(latest)

    delta = now - latest_date

    print(layer_name, latest_date, delta)


def get_ages_gsky(url):
    # with open(FILE) as f:
    with urlopen(url) as f:
        tree = ET.parse(f)

    now = datetime.now(timezone.utc)

    for node in tree.findall('.//wms:Layer/wms:Layer', NS):
        layer_name_and_age(node, now)


def get_ages_dea_nrt(url):
    with urlopen(url) as f:
        tree = ET.parse(f)

    now = datetime.now()

    nrt_node = tree.find('.//wms:Layer[wms:Title="Near Real-Time"]', NS)
    for node in nrt_node.findall('wms:Layer', NS):
        layer_name_and_age(node, now)

get_ages_dea_nrt(URLS['ows'])

#for url in URLS:
#    print(url)
#    get_ages(url)
#    print('\n\n')
