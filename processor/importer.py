import logging
import os
import requests
import uuid

from clients import AuthenticationClient
from clients import ImportClient, ImportFile
from clients import IntegrationClient, Integration

from constants import TIME_SERIES_BINARY_FILE_EXTENSION, TIME_SERIES_METADATA_FILE_EXTENSION

from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger()

"""
Uses the Pennsieve API to initialize and upload time series files
for import into Pennsieve data ecosystem.

# note: this will be moved to a separated post-processor once the analysis pipeline is more
# easily able to handle > 3 processors
"""

def import_timeseries(authentication_host, api_host, api_key, api_secret, integration_id, file_directory):
    # gather all the time series files from the output directory
    timeseries_files = []
    for root, _, files in os.walk(file_directory):
        for file in files:
            if file.endswith((TIME_SERIES_BINARY_FILE_EXTENSION, TIME_SERIES_METADATA_FILE_EXTENSION)):
                timeseries_files.append(
                    ImportFile(upload_key=uuid.uuid4(), file_path=os.path.join(root, file))
                )

    # authentication against the Pennsieve API
    authorization_client = AuthenticationClient(authentication_host)
    session_token = authorization_client.authenticate(api_key, api_secret)

    # fetch integration for parameters (dataset_id, package_id, etc.)
    integration_client = IntegrationClient(api_host)
    integration  = integration_client.get_integration(session_token, integration_id)

    # constraint until we implement (upstream) performing imports over directories
    # and specifying how to group time series files together into an imported package
    assert len(integration.package_ids) == 1, "NWB post processor only supports a single package for import"

    # initialize import
    import_client = ImportClient(api_host)
    import_id = import_client.create(session_token, integration.id, integration.dataset_id, integration.package_ids[0], timeseries_files)

    # upload time series files to Pennsieve S3 import bucket
    def upload_timeseries_file(timeseries_file):
        upload_url = import_client.get_presign_url(session_token, import_id, integration.dataset_id, timeseries_file.upload_key)
        with open(timeseries_file.file_path, 'rb') as f:
            response = requests.put(upload_url, data=f)
    with ThreadPoolExecutor() as executor:
        executor.map(upload_timeseries_file, timeseries_files)
