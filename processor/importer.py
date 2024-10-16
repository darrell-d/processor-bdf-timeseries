import backoff
import logging
import os
import requests
import uuid

from clients import AuthenticationClient
from clients import ImportClient, ImportFile
from clients import IntegrationClient, Integration

from constants import TIME_SERIES_BINARY_FILE_EXTENSION, TIME_SERIES_METADATA_FILE_EXTENSION

from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Value, Lock

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    if len(timeseries_files) == 0:
        return None

    # authentication against the Pennsieve API
    authorization_client = AuthenticationClient(authentication_host)
    session_token = authorization_client.authenticate(api_key, api_secret)

    # fetch integration for parameters (dataset_id, package_id, etc.)
    integration_client = IntegrationClient(api_host)
    integration  = integration_client.get_integration(session_token, integration_id)

    # constraint until we implement (upstream) performing imports over directories
    # and specifying how to group time series files together into an imported package
    assert len(integration.package_ids) == 1, "NWB post processor only supports a single package for import"

    log.info(f"dataset_id={integration.dataset_id} package_id={integration.package_ids[0]} starting import of time series files")

    # initialize import
    import_client = ImportClient(api_host)
    import_id = import_client.create(session_token, integration.id, integration.dataset_id, integration.package_ids[0], timeseries_files)

    log.info(f"import_id={import_id} initialized import with {len(timeseries_files)} time series files for upload")

    # track time series file upload count
    upload_counter = Value('i', 0)
    upload_counter_lock = Lock()

    # upload time series files to Pennsieve S3 import bucket
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5
    )
    def upload_timeseries_file(timeseries_file):
        try:
            with upload_counter_lock:
                log.info(f"import_id={import_id} upload_key={timeseries_file.upload_key} uploading {upload_counter.value}/{len(timeseries_files)} {timeseries_file.file_path}")
            upload_url = import_client.get_presign_url(session_token, import_id, integration.dataset_id, timeseries_file.upload_key)
            with open(timeseries_file.file_path, 'rb') as f:
                response = requests.put(upload_url, data=f)
                response.raise_for_status()  # raise an error if the request failed
            with upload_counter_lock:
                upload_counter.value += 1
            return True
        except Exception as e:
            log.error(f"import_id={import_id} upload_key={timeseries_file.upload_key} failed to upload {timeseries_file.file_path}: %s", e)
            raise e

    successful_uploads = list()
    with ThreadPoolExecutor(max_workers=4) as executor:
        # wrapping in a list forces the executor to wait for all threads to finish uploading time series files
        successful_uploads = list(executor.map(upload_timeseries_file, timeseries_files))

    log.info(f"import_id={import_id} uploaded {upload_counter.value} time series files")

    assert sum(successful_uploads) == len(timeseries_files), "Failed to upload all time series files"
