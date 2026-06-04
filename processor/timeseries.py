import os
import json
import requests

from clients import KeySecretAuthProvider, TokenAuthProvider

API_HOST = os.getenv("PENNSIEVE_API_HOST", "https://api.pennsieve.net")
API_HOST2 = os.getenv("PENNSIEVE_API_HOST2", "https://api2.pennsieve.net")

def authenticate():
    session_token = os.getenv("SESSION_TOKEN")
    refresh_token = os.getenv("REFRESH_TOKEN")
    if session_token:
        return TokenAuthProvider(API_HOST, session_token, refresh_token).get_session_token()

    api_key = os.getenv("PENNSIEVE_API_KEY")
    api_secret = os.getenv("PENNSIEVE_API_SECRET")
    if api_key and api_secret:
        return KeySecretAuthProvider(API_HOST, api_key, api_secret).get_session_token()

    raise RuntimeError("no authentication credentials provided: set SESSION_TOKEN or PENNSIEVE_API_KEY/PENNSIEVE_API_SECRET")

def getWorkflowData(session_key):
    integration_id = os.getenv("INTEGRATION_ID")
    r = requests.get(f"{API_HOST2}/compute/workflows/runs/{integration_id}", headers={"Authorization": f"Bearer {session_key}"})

    return json.loads(r.text)

import requests

def getBDFPackageId(session_key, dataset_id):
    ''' 
    Gets the package ID of the file with the .bdf extension in the specified dataset. 
    Only expects one package with this extension. 
    If multiple packages are found, an error is raised.
    
    Parameters:
        session_key (str): Bearer token for authenticated access
        dataset_id (str): The Pennsieve dataset ID (e.g. "e7d1bfbe-2899-4f58-afe9-214cad4ad46a")

    Returns:
        str: The package ID of the .bdf file

    Raises:
        Exception: If no .bdf file is found or if multiple are found
    '''

    # Construct URL
    url = f"{API_HOST}/datasets/{dataset_id}/packages"

    headers = {
        "Authorization": f"Bearer {session_key}",
        "Content-Type": "application/json"
    }

    # Make request
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    # Extract children list (packages)
    packages = data.get("packages", [])
    print(f"Found {len(packages)} packages in dataset {dataset_id}")

    # Find packages ending in .bdf (case-insensitive)
    bdf_packages = [
        {"package_id": pkg["content"]["nodeId"], "name": pkg["content"]["name"]}
        for pkg in packages
        if pkg.get("content", {}).get("name", "").lower().endswith(".bdf")
    ]
    print(f"Found {len(bdf_packages)} .bdf packages in dataset {dataset_id}")

    if len(bdf_packages) == 0:
        raise Exception(f"No .bdf package found in dataset {dataset_id}")
    elif len(bdf_packages) > 1:
        raise Exception(f"Multiple .bdf packages found in dataset {dataset_id}: {bdf_packages}")

    return bdf_packages[0]

def createAnnotationLayer(session_key, bdf_package_id):
    '''
    Creates an annotation layer for the specified BDF package.

    Parameters:
        session_key (str): Bearer token for authenticated access
        bdf_package_id (str): The package ID of the BDF file

    Returns:
        bool: True if the annotation layer was created successfully, False otherwise
    '''

    from datetime import datetime
    now = datetime.now()

    url = f"{API_HOST}/timeseries/{bdf_package_id}/layers"
    params = {
        "name": f"Imported Annotations {now}",
        "description": "Annotations autoimported by `https://github.com/Pennsieve/bdf-annotations-extractor`",
    }
    headers = {
        "Authorization": f"Bearer {session_key}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, json=params, headers=headers)
    
    return json.loads(response.text)

def getChannels(session_key, timeSeriesPackageID):
    '''
    Retrieves the channels for the specified BDF package.

    Parameters:
        session_key (str): Bearer token for authenticated access
        timeSeriesPackageID (str): The package ID of the BDF file

    Returns:
        list: A list of channels in the BDF package
    '''


    url = f"{API_HOST}/timeseries/{timeSeriesPackageID}/channels"
    headers = {
        "Authorization": f"Bearer {session_key}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    channels = json.loads(response.text)
    channel_ids = [entry["content"]["id"] for entry in channels]
    
    return channel_ids

def createAnnotation(session_key,channels,timeseriesId,timeseriesIdPackageName,annotations):
    '''
    Creates an annotation in the specified timeseries layer.

    Parameters:
        session_key (str): Bearer token for authenticated access
        timeseries_id (str): The ID of the timeseries layer
        name (str): The name of the annotation
        label (str): The label of the annotation
        start (int): Start time in milliseconds
        end (int): End time in milliseconds
        relative_start (float): Relative start time as a fraction of the total duration
        relative_end (float): Relative end time as a fraction of the total duration

    Returns:
        dict: The created annotation object
    '''

    url = f"{API_HOST}/timeseries/{timeseriesIdPackageName}/layers/{timeseriesId}/annotations?startAtEpoch=true"
    headers = {
        "Authorization": f"Bearer {session_key}",
        "Content-Type": "application/json"
    }

    for annotation in annotations:
        params = {
            "name": annotation["name"],
            "label": annotation["label"],
            "start": int(annotation["relative_start"]),
            "end": (int(annotation["relative_end"])) + 10000,
            "layer_id": timeseriesId,
            "channelIds": channels,
        } 
        print(f"Creating annotation: {params}")
        response = requests.post(url, json=params, headers=headers)
        response.raise_for_status()

        if response.status_code != 201:
            print(f"Failed to create annotation: {response.text}")
            continue
    
    return json.loads(response.text)
