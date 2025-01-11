#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import os
import tempfile
from pathlib import Path
import requests

import pandas as pd
import numpy as np
import json

# Prevent Kaggle from auto-authenticating on import
os.environ['KAGGLE_USERNAME'] = 'DUMMY'
os.environ['KAGGLE_KEY'] = 'DUMMY'

# Now we can safely import KaggleApi
from kaggle.api.kaggle_api_extended import KaggleApi
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream


"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


class DatasetManager:
    """Singleton class to manage dataset download and extraction"""
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatasetManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not DatasetManager._initialized:
            self.api = None
            self._temp_dir = None
            self._extracted = False
            self.dataset_name = None
            self._csv_files = None
            DatasetManager._initialized = True

    def initialize(self, config: Mapping[str, Any]):
        """Initialize with config if not already initialized"""
        if not self.dataset_name:
            self.dataset_name = config["dataset_name"]
            self._ensure_api_initialized(config)

    def _ensure_api_initialized(self, config):
        """Initialize the Kaggle API if not already done"""
        if self.api is None:
            # Create kaggle.json file
            kaggle_dir = os.environ.get('KAGGLE_CONFIG_DIR', os.path.expanduser('~/.kaggle'))
            os.makedirs(kaggle_dir, exist_ok=True)
            
            kaggle_config = {
                "username": config["username"],
                "key": config["key"]
            }
            
            kaggle_path = os.path.join(kaggle_dir, "kaggle.json")
            with open(kaggle_path, "w") as f:
                json.dump(kaggle_config, f)
            
            # Set file permissions to 600 as required by Kaggle
            os.chmod(kaggle_path, 0o600)
            
            # Update environment variables with real credentials
            os.environ['KAGGLE_USERNAME'] = config["username"]
            os.environ['KAGGLE_KEY'] = config["key"]
            
            self.api = KaggleApi()
            self.api.authenticate()

    def get_temp_dir(self) -> str:
        """Get or create temporary directory"""
        if not self._temp_dir:
            self._temp_dir = tempfile.mkdtemp()
        return self._temp_dir

    def ensure_dataset_downloaded(self):
        """Download and extract dataset if not already done"""
        if not self._extracted:
            try:
                self.api.dataset_download_files(
                    dataset=self.dataset_name,
                    path=self.get_temp_dir(),
                    force=True,
                    quiet=True
                )
                
                # List all zip files in the directory
                zip_files = list(Path(self.get_temp_dir()).glob("*.zip"))
                
                if zip_files:
                    import zipfile
                    with zipfile.ZipFile(zip_files[0], 'r') as zip_ref:
                        zip_ref.extractall(self.get_temp_dir())
                
                self._extracted = True
                
            except Exception as e:
                raise Exception(f"Error downloading/extracting dataset: {str(e)}")

    def get_csv_files(self) -> List[str]:
        """Return list of CSV files in the dataset"""
        if self._csv_files is None:
            self.ensure_dataset_downloaded()
            self._csv_files = [f.name for f in Path(self.get_temp_dir()).glob("*.csv")]
        return self._csv_files

    def cleanup(self):
        """Cleanup temporary directory"""
        if self._temp_dir and os.path.exists(self._temp_dir):
            import shutil
            shutil.rmtree(self._temp_dir)
            self._temp_dir = None
            self._extracted = False
            self._csv_files = None

class KaggleStream(HttpStream):
    """Stream dynamique pour chaque fichier CSV"""
    url_base = "https://www.kaggle.com/api/v1/"
    primary_key = None

    def __init__(self, config: Mapping[str, Any], csv_file: str):
        super().__init__(authenticator=None)
        self.csv_file = csv_file
        self._schema = None
        self.dataset_manager = DatasetManager()
        self.dataset_manager.initialize(config)

    @property
    def name(self) -> str:
        # Remove .csv extension and use as stream name
        return self.csv_file.replace('.csv', '').lower()

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Returns the schema for the specific CSV file
        """
        if self._schema is not None:
            return self._schema
            
        schema = {
            "type": "object",
            "properties": {}
        }
        
        self.dataset_manager.ensure_dataset_downloaded()
        csv_path = Path(self.dataset_manager.get_temp_dir()) / self.csv_file
        
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path, nrows=1)
                for column in df.columns:
                    dtype = df[column].dtype
                    if pd.api.types.is_numeric_dtype(dtype):
                        if pd.api.types.is_integer_dtype(dtype):
                            field_type = "integer"
                        else:
                            field_type = "number"
                    else:
                        field_type = "string"
                    
                    schema["properties"][column] = {
                        "type": ["null", field_type]
                    }
            except Exception as e:
                self.logger.error(f"Error processing {self.csv_file}: {str(e)}")
        
        self._schema = schema
        return schema

    def next_page_token(self, response: Any) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def path(self, **kwargs) -> str:
        return f"datasets/download/{self.dataset_manager.dataset_name}"

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        self.dataset_manager.ensure_dataset_downloaded()
        csv_path = Path(self.dataset_manager.get_temp_dir()) / self.csv_file
        
        if not csv_path.exists():
            self.logger.warning(f"File {self.csv_file} not found in dataset")
            return []

        # Utiliser des chunks pour la lecture des fichiers
        chunk_size = 10000  # Ajuster selon les besoins
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
            for record in chunk.replace({np.nan: None}).to_dict('records'):
                yield record

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Cette méthode est requise par HttpStream mais nous utilisons read_records à la place
        car nous travaillons avec des fichiers locaux après le téléchargement
        """
        yield from self.read_records(**kwargs)
        
    def __del__(self):
        """Cleanup temporary directory when the object is destroyed"""
        self.dataset_manager.cleanup()

class SourceKaggle(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            manager = DatasetManager()
            manager.initialize(config)
            csv_files = manager.get_csv_files()
            if not csv_files:
                return False, "No CSV files found in the dataset"
            return True, None
        except Exception as e:
            return False, f"Error connecting to Kaggle: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        manager = DatasetManager()
        manager.initialize(config)
        return [KaggleStream(config, csv_file) for csv_file in manager.get_csv_files()]
