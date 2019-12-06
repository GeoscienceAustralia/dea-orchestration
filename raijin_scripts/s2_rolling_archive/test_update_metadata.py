import tempfile
import yaml
from yaml import CSafeLoader as Loader
from pathlib import Path
from unittest.mock import patch


from .update_metadata import update_dataset_definition


def test_can_parse_nci_email():
    metadatafile = Path(__file__).parent / 'sample_metadatafile.yaml'
    with open(str(metadatafile)) as dataset_file:
        df = yaml.load(dataset_file, Loader=Loader)

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Run the Function under test
        with patch('sys.argv', ['update_metadata.py', metadatafile, str(Path(tmpdirname))]):
            update_dataset_definition()

        new_file = Path(tmpdirname) / "ARD-METADATA.yaml"

        with open(new_file) as config_file:
            temp_data = yaml.load(config_file, Loader=Loader)

        assert temp_data['product_type'] == 'S2MSIARD_NBAR'
        assert 'nbart_blue' not in temp_data['image']['bands']
        assert 'nbart_coastal_aerosol' not in temp_data['image']['bands']
        assert 'nbart_contiguity' not in temp_data['image']['bands']
        assert 'nbart_green' not in temp_data['image']['bands']
        assert 'nbart_nir_1' not in temp_data['image']['bands']
        assert 'nbart_nir_2' not in temp_data['image']['bands']
        assert 'nbart_red' not in temp_data['image']['bands']
        assert 'nbart_red_edge_1' not in temp_data['image']['bands']
        assert 'nbart_red_edge_2' not in temp_data['image']['bands']
        assert 'nbart_red_edge_3' not in temp_data['image']['bands']
        assert 'nbart_blue' not in temp_data['image']['bands']
        assert 'nbart_swir_2' not in temp_data['image']['bands']
        assert 'nbart_swir_3' not in temp_data['image']['bands']
        assert 'lineage' not in temp_data
        assert df['id'] != temp_data['id']
