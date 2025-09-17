import json
import time
import sys
import os
import six.moves.urllib.parse
import requests
from requests.auth import HTTPBasicAuth
import google.auth
import google.auth.transport.requests
from google.oauth2 import id_token
from google.auth.transport.requests import Request

def get_env_var(var_name, default_value):
    """Get environment variable checking both uppercase and lowercase variants."""
    return os.environ.get(var_name.upper(), os.environ.get(var_name.lower(), default_value))

# Provide variables from OS Layer
airflow_url = get_env_var('airflow_url', 'Airflow/Composer API URL')
airflow_user = get_env_var('airflow_user', 'Airflow/Composer UI User')
airflow_pass = get_env_var('airflow_password', 'Airflow/Composer UI Password')
dag_id = get_env_var('dag_id', 'Airflow/Composer DAG ID')
platform = get_env_var('platform', 'Airflow/Composer')
project_id = get_env_var('project_id', 'Google Cloud Project ID')
location = get_env_var('location', 'Google Cloud Project Composer Location')
composer_environment = get_env_var('composer_environment_name', 'Google Cloud Project Composer Environment Name')
service_account_email = get_env_var('service_account_email', 'Google Cloud Project Service Account')

# If you are using the stable API, set this value to False
use_experimental_api = False

def basicauthapi_delete_dag(dag_id, airflow_user, airflow_pass):
    """Delete a DAG using basic authentication"""
    # Set the API endpoint, auth, and headers
    url = f'{airflow_url}/api/v1/dags/{dag_id}'
    headers = {'Accept': '*/*'}
    auth = HTTPBasicAuth(airflow_user, airflow_pass)

    # Credentials Used
    print(f'Credentials to use: {airflow_user}')
    print(f'Make a Delete request for API {url}')
    
    # Make the Delete request
    response = requests.delete(url, headers=headers, auth=auth)
    
    # Check the response status code
    if response.status_code == 204:
        print(f"DAG {dag_id} was successfully deleted.")
    else:
        print(f"Error: DAG {dag_id} cannot be deleted. Response code: {response.status_code}")
        print(response.text)
        sys.exit(1)

def get_client_id(project_id, location, composer_environment):
    """Get the client ID for Google Cloud Composer environment"""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    authed_session = google.auth.transport.requests.AuthorizedSession(credentials)

    environment_url = (
        "https://composer.googleapis.com/v1beta1/projects/{}/locations/{}"
        "/environments/{}"
    ).format(project_id, location, composer_environment)
    composer_response = authed_session.request("GET", environment_url)
    print("Access to Google Application Default Credentials. Response code: ", composer_response)
    environment_data = composer_response.json()
    
    # Verify Composer version
    composer_version = environment_data["config"]["softwareConfig"]["imageVersion"]
    if "composer-1" not in composer_version:
        version_error = ("This script is intended to be used with Composer 1 environments. "
                        "In Composer 2, the Airflow Webserver is not in the tenant project.")
        raise RuntimeError(version_error)
    
    airflow_uri = environment_data["config"]["airflowUri"]
    redirect_response = requests.get(airflow_uri, allow_redirects=False)
    redirect_location = redirect_response.headers["location"]
    parsed = six.moves.urllib.parse.urlparse(redirect_location)
    query_string = six.moves.urllib.parse.parse_qs(parsed.query)
    return query_string["client_id"][0]

def make_iap_request(url, client_id, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy"""
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    resp = requests.request(
        method, url,
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {open_id_connect_token}'
        },
        **kwargs
    )
    
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to access the IAP-protected application.')
    return resp

def googleauth_delete_dag(dag_id, client_id):
    """Delete a DAG using Google Cloud authentication"""
    endpoint = f'/api/v1/dags/{dag_id}'
    if use_experimental_api:
        endpoint = f'api/experimental/dags/{dag_id}'

    webserver_url = airflow_url + endpoint
    
    # Make a Delete request to IAP
    response = make_iap_request(webserver_url, client_id, method='DELETE')
    
    # Check the response status code
    if response.status_code == 204:
        print(f"DAG {dag_id} was successfully deleted.")
    else:
        print(f"Error: DAG {dag_id} cannot be deleted. Response code: {response.status_code}")
        print(response.text)
        sys.exit(1)

def main():
    if not dag_id:
        print("Error: DAG ID is required")
        sys.exit(1)

    if platform.lower() == "airflow":
        basicauthapi_delete_dag(dag_id, airflow_user, airflow_pass)
    elif platform.lower() == "composer":
        client_id = get_client_id(project_id, location, composer_environment)
        googleauth_delete_dag(dag_id, client_id)
    else:
        print(f"Error: Unsupported platform '{platform}'. Use 'airflow' or 'composer'")
        sys.exit(1)

if __name__ == "__main__":
    main()