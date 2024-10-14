import requests
import json
import logging

log = logging.getLogger()

class Integration:
    def __init__(self, id, application_id, dataset_id, package_ids, params):
        self.id = id
        self.application_id = application_id
        self.dataset_id = dataset_id
        self.package_ids = package_ids
        self.params = params

class IntegrationClient:
    def __init__(self, api_host):
        self.api_host = api_host

    # NOTE: integration API currently returns a 200 response
    #       with an empty body even when an integration does not exist
    def get_integration(self, session_token, integration_id):
        url = f"{self.api_host}/integrations/{integration_id}"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {session_token}"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            integration = Integration(
                id=data["uuid"],
                application_id=data["applicationId"],
                dataset_id=data["datasetId"],
                package_ids=data["packageIds"],
                params=data["params"]
            )

            return integration
        except requests.HTTPError as e:
            log.error(f"failed to fetch integration with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode integration response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to get integration with error: {e}")
            raise e
