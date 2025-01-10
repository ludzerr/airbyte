# Kaggle Source Connector

This is the repository for the Kaggle source connector, written in Python.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.com/integrations/sources/kaggle).

## Features

### Key Capabilities

- Downloads and processes Kaggle datasets
- Handles multiple CSV files within a dataset
- Supports large files through chunked processing
- Automatic schema inference from CSV files
- Type conversion and null value handling
- Efficient caching mechanism for improved performance

### Data Handling

- **File Types**: Currently supports CSV files
- **Size Limits**:
  - Free Account: Up to 20GB per dataset, 10GB per file
  - Pro Account: Up to 100GB per dataset, 50GB per file
- **Memory Efficient**: Uses chunked processing (10,000 rows per chunk) for large files
- **Type Detection**: Automatically detects and converts:
  - Integer fields
  - Decimal numbers
  - String fields
  - Null values

### Performance Optimizations

- Single download per sync
- Cached schema inference
- Temporary file cleanup
- Chunked processing for large files
- Error handling with continued processing

## Configuration

### Required Parameters

- `username`: Your Kaggle username
- `key`: Your Kaggle API key
- `dataset_name`: Name of the dataset in format "owner/dataset-name"

### Authentication

1. Create a Kaggle account if you don't have one
2. Go to your account settings (https://www.kaggle.com/account)
3. Scroll to "API" section and click "Create New API Token"
4. This will download a `kaggle.json` file containing your credentials

## Usage Notes

### Dataset Access

- Ensure you have accepted the dataset's terms on Kaggle
- Public datasets are accessible to all
- Private datasets require appropriate permissions

### Memory Usage

- For large files (>1GB), the connector uses chunked processing
- Each chunk processes 10,000 rows at a time
- Memory usage is limited to:
  - One chunk of data
  - File extraction space
  - Schema cache

### Error Handling

- Continues processing if one file fails
- Logs detailed error messages
- Cleans up temporary files automatically

## Development and Testing

### Prerequisites

- Python (`^3.9`)
- Poetry (`^1.7`)

### Installation

```bash
poetry install --with dev
```

### Running Tests

```bash
poetry run pytest tests
```

### Adding New Features

When adding new features, consider:

1. Memory efficiency for large datasets
2. Error handling and logging
3. Type conversion accuracy
4. Cleanup of temporary resources

## Limitations and Known Issues

### Current Limitations

- Only supports CSV files
- No support for dataset metadata
- No incremental sync support
- Requires downloading entire dataset

### Future Improvements

1. Support for other file formats (JSON, Excel)
2. Incremental sync capabilities
3. Dataset metadata integration
4. Configurable chunk size
5. Column selection support

## Local development

### Prerequisites

- Python (`^3.9`)
- Poetry (`^1.7`) - installation instructions [here](https://python-poetry.org/docs/#installation)

### Installing the connector

From this connector directory, run:

```bash
poetry install --with dev
```

### Create credentials

**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.com/integrations/sources/kaggle)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `src/source_kaggle/spec.yaml` file.
Note that any directory named `secrets` is gitignored across the entire Airbyte repo, so there is no danger of accidentally checking in sensitive information.
See `sample_files/sample_config.json` for a sample config file.

### Locally running the connector

```
poetry run source-kaggle spec
poetry run source-kaggle check --config secrets/config.json
poetry run source-kaggle discover --config secrets/config.json
poetry run source-kaggle read --config secrets/config.json --catalog sample_files/configured_catalog.json
```

### Running tests

To run tests locally, from the connector directory run:

```
poetry run pytest tests
```

### Building the docker image

1. Install [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md)
2. Run the following command to build the docker image:

```bash
airbyte-ci connectors --name=source-kaggle build
```

An image will be available on your host with the tag `airbyte/source-kaggle:dev`.

### Running as a docker container

Then run any of the connector commands as follows:

```
docker run --rm airbyte/source-kaggle:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-kaggle:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-kaggle:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/integration_tests:/integration_tests airbyte/source-kaggle:dev read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json
```

### Running our CI test suite

You can run our full test suite locally using [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md):

```bash
airbyte-ci connectors --name=source-kaggle test
```

### Customizing acceptance Tests

Customize `acceptance-test-config.yml` file to configure acceptance tests. See [Connector Acceptance Tests](https://docs.airbyte.com/connector-development/testing-connectors/connector-acceptance-tests-reference) for more information.
If your connector requires to create or destroy resources for use during acceptance tests create fixtures for it and place them inside integration_tests/acceptance.py.

### Dependency Management

All of your dependencies should be managed via Poetry.
To add a new dependency, run:

```bash
poetry add <package-name>
```

Please commit the changes to `pyproject.toml` and `poetry.lock` files.

## Publishing a new version of the connector

You've checked out the repo, implemented a million dollar feature, and you're ready to share your changes with the world. Now what?

1. Make sure your changes are passing our test suite: `airbyte-ci connectors --name=source-kaggle test`
2. Bump the connector version (please follow [semantic versioning for connectors](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#semantic-versioning-for-connectors)):
   - bump the `dockerImageTag` value in in `metadata.yaml`
   - bump the `version` value in `pyproject.toml`
3. Make sure the `metadata.yaml` content is up to date.
4. Make sure the connector documentation and its changelog is up to date (`docs/integrations/sources/kaggle.md`).
5. Create a Pull Request: use [our PR naming conventions](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#pull-request-title-convention).
6. Pat yourself on the back for being an awesome contributor.
7. Someone from Airbyte will take a look at your PR and iterate with you to merge it into master.
8. Once your PR is merged, the new version of the connector will be automatically published to Docker Hub and our connector registry.
