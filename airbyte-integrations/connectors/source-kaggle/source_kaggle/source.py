#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import os
import tempfile
from pathlib import Path

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
import numpy as np

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class KaggleDatasetStream(HttpStream):
    # TODO: Fill in the url base. Required.
    url_base = "https://www.kaggle.com/api/v1/"
    primary_key = None
    name = "kaggle_dataset"
    
    def __init__(self, config: Mapping[str, Any]):
        super().__init__(authenticator=None)
        self.dataset_name = config["dataset_name"]
        self.api = KaggleApi()
        # Configure the Kaggle API with credentials
        os.environ['KAGGLE_USERNAME'] = config["username"]
        os.environ['KAGGLE_KEY'] = config["key"]
        self.api.authenticate()
        self._schema = None
        self._temp_dir = None
        self._extracted = False

    def _download_and_extract(self):
        """Helper method to download and extract dataset"""
        if not self._temp_dir:
            self._temp_dir = tempfile.mkdtemp()
            
        if not self._extracted:
            try:
                self.api.dataset_download_files(
                    dataset=self.dataset_name,
                    path=self._temp_dir,
                    force=True,
                    quiet=True
                )
                
                # List all zip files in the directory
                zip_files = list(Path(self._temp_dir).glob("*.zip"))
                
                if zip_files:
                    import zipfile
                    with zipfile.ZipFile(zip_files[0], 'r') as zip_ref:
                        zip_ref.extractall(self._temp_dir)
                
                self._extracted = True
                
            except Exception as e:
                self.logger.error(f"Error downloading/extracting dataset: {str(e)}")
                raise e

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Override get_json_schema to return the schema directly instead of loading from a file
        Creates a unified schema that includes all columns from all CSV files
        """
        if self._schema is not None:
            return self._schema
            
        schema = {
            "type": "object",
            "properties": {
                "_ab_source_file": {
                    "type": "string"
                }
            }
        }
        
        self._download_and_extract()
        
        # Process all CSV files
        for csv_file in Path(self._temp_dir).glob("*.csv"):
            self.logger.info(f"Reading schema from: {csv_file.name}")
            try:
                df = pd.read_csv(csv_file, nrows=1)
                for column in df.columns:
                    # Infer the type from the data
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
                self.logger.error(f"Error processing {csv_file.name}: {str(e)}")
        
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
        return f"datasets/download/{self.dataset_name}"

    def parse_response(self, response: Any, **kwargs) -> Iterable[Mapping]:
        self._download_and_extract()
        
        # Process all CSV files in the directory
        for file_path in Path(self._temp_dir).glob("*.csv"):
            self.logger.info(f"Processing file: {file_path.name}")
            
            try:
                # Read the CSV file
                df = pd.read_csv(file_path)
                
                # Convert each row to a dictionary and yield it
                for record in df.to_dict('records'):
                    # Add metadata about which file this record came from
                    record['_ab_source_file'] = file_path.name
                    
                    # Convert numpy/pandas types to Python native types
                    for key, value in record.items():
                        if pd.isna(value):
                            record[key] = None
                        elif isinstance(value, (np.integer, np.floating)):
                            record[key] = value.item()
                    
                    yield record
            except Exception as e:
                self.logger.error(f"Error processing {file_path.name}: {str(e)}")
                continue

    def __del__(self):
        """Cleanup temporary directory when the object is destroyed"""
        if self._temp_dir and os.path.exists(self._temp_dir):
            import shutil
            shutil.rmtree(self._temp_dir)

# Source
class SourceKaggle(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            # Configure the Kaggle API with credentials
            api = KaggleApi()
            os.environ['KAGGLE_USERNAME'] = config["username"]
            os.environ['KAGGLE_KEY'] = config["key"]
            api.authenticate()
            
            # Test with a simple API call first
            api.dataset_list(search=config["dataset_name"])
            
            return True, None
        except Exception as e:
            return False, f"Error connecting to Kaggle: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [KaggleDatasetStream(config)]
