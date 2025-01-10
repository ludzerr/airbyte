#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_kaggle.source import SourceKaggle

def run():
    source = SourceKaggle()
    launch(source, sys.argv[1:])
