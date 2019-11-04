import uuid
import logging
import yaml
import sys
from pathlib import Path
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper


_LOG = logging.getLogger(__name__)


def update_dataset_definition():
    yaml_file = sys.argv[1]
    out_path = Path(sys.argv[2])
    out_path.mkdir(parents=True, exist_ok=True)
    new_yamlfile = out_path / "ARD-METADATA.yaml"

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

    with open(new_yamlfile, 'w') as fp:
        yaml.dump(temp_metadata, fp, default_flow_style=False, Dumper=Dumper)


if __name__ == '__main__':
    update_dataset_definition()
