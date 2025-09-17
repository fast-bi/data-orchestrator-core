import json
import time
import sys
import os
import six.moves.urllib.parse
import requests
from datetime import datetime, timezone
from requests.auth import HTTPBasicAuth
import google.auth
import google.auth.transport.requests
from google.oauth2 import id_token
from google.auth.transport.requests import Request

def get_env_var(var_name, default_value):
    """Get environment variable checking both uppercase and lowercase variants."""
    return os.environ.get(var_name.upper(), os.environ.get(var_name.lower(), default_value))

#Provide_variables from OS Layer
airflow_url = get_env_var('airflow_url', 'Airflow/Composer API URL')
airflow_user = get_env_var('airflow_user', 'Airflow/Composer UI User')
airflow_pass = get_env_var('airflow_password', 'Airflow/Composer UI Password')
airflow_dag_id = get_env_var('dag_id', 'Airflow/Composer DAG ID')
platform = get_env_var('platform', 'Airflow/Composer')
project_id = get_env_var('project_id', 'Google Cloud Project ID')
location = get_env_var('location', 'Google Cloud Project Composer Location')
composer_environment = get_env_var('composer_environment_name', 'Google Cloud Project Composer Environment Name')
service_account_email = get_env_var('service_account_email', 'Google Cloud Project Service Account')
dag_run_id = get_env_var('dag_run_id', 'E2E Test Pipeline RUN id')
current_dateTime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
# If you are using the stable API, set this value to False
# For more info about Airflow APIs see https://cloud.google.com/composer/docs/access-airflow-api

use_experimental_api = False

#BASIC AUTH STARTS
def basicauthapi_get_dag(airflow_dag_id, airflow_user, airflow_pass):
    # Set the API endpoint, auth, and headers
    url = airflow_url+'/api/v1/dags/'+airflow_dag_id
    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(airflow_user, airflow_pass)

    # # Credentials Used
    print('Credentials to use:'+airflow_user)
    print('Make a GET request for API '+url)
    
    # # start_waiting_time.
    start_time = time.time()

    while True:
        # # Make the GET request
        response = requests.get(url, headers=headers, auth=auth)
        if response.status_code == 200:
            break
        if time.time() - start_time > 3600:
            raise Exception("Failed to receive response with status code 200 within 1 hour")
        time.sleep(30)
    
    # # Check the response status code
    if response.status_code == 200:
        print("DAG "+airflow_dag_id+" found.")
    else:
        print("Error DAG "+airflow_dag_id+" cannot be found. Response code: ", response.status_code)
        print(response.text)
        sys.exit(1)

def basicauthapi_enable_dag(airflow_dag_id, airflow_user, airflow_pass):
    # Set the API endpoint, auth, and headers
    url = airflow_url+'/api/v1/dags/'+airflow_dag_id
    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(airflow_user, airflow_pass)
    
    payload = {
            "is_paused": False
            }

    # # Credentials Used
    print('Credentials to use:'+airflow_user)
    print('Make a Patch request for API '+url)
    # # Make the PATCH request
    response = requests.patch(url, headers=headers, data=json.dumps(payload), auth=auth)

    # # Check the response status code
    if response.status_code == 200:
        print("DAG "+airflow_dag_id+" enabled successfully.")
    else:
        print("Error DAG "+airflow_dag_id+" cannot be enabled. Response code: ", response.status_code)
        print(response.text)
        sys.exit(1)

def basicauthapi_dag_run(airflow_dag_id, airflow_user, airflow_pass):
    # Set the API endpoint, auth, and headers
    url = airflow_url+'/api/v1/dags/'+airflow_dag_id+'/dagRuns'
    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(airflow_user, airflow_pass)
    
    payload = {
        "dag_run_id": dag_run_id,
        "data_interval_end": current_dateTime,
        "data_interval_start": current_dateTime
    }

    # Credentials Used
    print('Credentials to use:'+airflow_user)
    print('Make a POST request for API '+url)

    max_retries = 6
    retry_delay = 60  # seconds
    for attempt in range(max_retries):
        print(f"Attempt {attempt+1} to trigger DAG")
        # Make the POST request
        response = requests.post(url, headers=headers, data=json.dumps(payload), auth=auth)

        # Check the response status code
        if response.status_code == 200:
            print("DAG "+airflow_dag_id+" triggered successfully.")
            break
        elif response.status_code == 409:
            print("DAG "+airflow_dag_id+" was triggered already, DAG run id exists: "+ dag_run_id)
            break
        elif response.status_code == 404:
            if attempt < max_retries - 1:
                print(f"DAG {airflow_dag_id} not found. Will retry after {retry_delay} seconds.")
                time.sleep(retry_delay)
            else:
                print("DAG "+airflow_dag_id+" not found after maximum retries. Response code: ", response.status_code)
                print(response.text)
                print("Max retries reached. Exiting.")
                sys.exit(1)
        else:
            print("Error: DAG "+airflow_dag_id+" not triggered. Response code: ", response.status_code)
            print(response.text)
            sys.exit(1)

#BASIC AUTH ENDS

def get_client_id(project_id, location, composer_environment):
    # Authenticate with Google Cloud.
    # See: https://cloud.google.com/docs/authentication/getting-started
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
    composer_version = environment_data["config"]["softwareConfig"]["imageVersion"]
    if "composer-1" not in composer_version:
        version_error = ("This script is intended to be used with Composer 1 environments. "
                         "In Composer 2, the Airflow Webserver is not in the tenant project, "
                         "so there is no tenant client ID. "
                         "See https://cloud.google.com/composer/docs/composer-2/environment-architecture for more details.")
        raise (RuntimeError(version_error))
    airflow_uri = environment_data["config"]["airflowUri"]

    # The Composer environment response does not include the IAP client ID.
    # Make a second, unauthenticated HTTP request to the web server to get the
    # redirect URI.
    redirect_response = requests.get(airflow_uri, allow_redirects=False)
    redirect_location = redirect_response.headers["location"]
    # Extract the client_id query parameter from the redirect.
    parsed = six.moves.urllib.parse.urlparse(redirect_location)
    query_string = six.moves.urllib.parse.parse_qs(parsed.query)
    client_id = (query_string["client_id"][0])
    return client_id

def make_iap_request(url, client_id, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.

    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.

    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    #print('Credentials to use:'+service_account_email)
    # Set the default timeout, if missing
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    # Obtain an OpenID Connect (OIDC) token from metadata server or using service
    # account.
    open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    resp = requests.request(
        method, url,
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer {}'.format(
            open_id_connect_token)}, **kwargs)
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    else:
        return resp

def googleauth_get_dag(airflow_dag_id, client_id):
    """Makes a POST request to the Composer DAG Trigger API

    When called via Google Cloud Functions (GCF),
    data and context are Background function parameters.

    For more info, refer to
    https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-python

    To call this function from a Python script, omit the ``context`` argument
    and pass in a non-null value for the ``data`` argument.

    This function is currently only compatible with Composer v1 environments.
    """

    # Fill in with your Composer info here
    # Navigate to your webserver's login page and get this from the URL
    # Or use the script found at
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/composer/rest/get_client_id.py
    # client_id = client_id
    # This should be part of your webserver's URL:
    # {tenant-project-id}.appspot.com
    webserver_id = airflow_url

    if use_experimental_api:
        endpoint = f'api/experimental/dags/'+airflow_dag_id
    else:
        endpoint = f'/api/v1/dags/'+airflow_dag_id

    webserver_url = webserver_id + endpoint
    # Make a GET request to IAP.
    response = make_iap_request(webserver_url, client_id, method='GET')
    
    # # start_waiting_time.
    start_time = time.time()

    while True:
        # # Make the GET request
        response = make_iap_request(webserver_url, client_id, method='GET')
        if response.status_code == 200:
            break
        if time.time() - start_time > 3600:
            raise Exception("Failed to receive response with status code 200 within 1 hour")
        time.sleep(30)
    
    # # Check the response status code
    if response.status_code == 200:
        print("DAG "+airflow_dag_id+" found.")
    else:
        print("Error DAG "+airflow_dag_id+" cannot be found. Response code: ", response.status_code)
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                response.status_code, response.text))

def googleauth_enable_dag(airflow_dag_id, client_id):
    """Makes a POST request to the Composer DAG Trigger API

    When called via Google Cloud Functions (GCF),
    data and context are Background function parameters.

    For more info, refer to
    https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-python

    To call this function from a Python script, omit the ``context`` argument
    and pass in a non-null value for the ``data`` argument.

    This function is currently only compatible with Composer v1 environments.
    """

    # Fill in with your Composer info here
    # Navigate to your webserver's login page and get this from the URL
    # Or use the script found at
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/composer/rest/get_client_id.py
    # client_id = client_id
    # This should be part of your webserver's URL:
    # {tenant-project-id}.appspot.com
    webserver_id = airflow_url

    if use_experimental_api:
        endpoint = f'api/experimental/dags/'+airflow_dag_id
        payload = {
        "is_paused": False
        }
    else:
        endpoint = f'/api/v1/dags/'+airflow_dag_id
        payload = {
        "is_paused": False
        }

    webserver_url = webserver_id + endpoint
    # Make a PATCH request to IAP.
    response = make_iap_request(webserver_url, client_id, method='PATCH', data=json.dumps(payload))
    
    # # Check the response status code
    if response.status_code == 200:
        print("DAG "+airflow_dag_id+" enabled successfully.")
    else:
        print("Error DAG "+airflow_dag_id+" cannot be enabled. Response code: ", response.status_code)
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                response.status_code, response.text))

def googleauth_dag_run(airflow_dag_id, dag_run_id, client_id):
    """Makes a POST request to the Composer DAG Trigger API

    When called via Google Cloud Functions (GCF),
    data and context are Background function parameters.

    For more info, refer to
    https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-python

    To call this function from a Python script, omit the ``context`` argument
    and pass in a non-null value for the ``data`` argument.

    This function is currently only compatible with Composer v1 environments.
    """

    # Fill in with your Composer info here
    # Navigate to your webserver's login page and get this from the URL
    # Or use the script found at
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/composer/rest/get_client_id.py
    # client_id = client_id
    # This should be part of your webserver's URL:
    # {tenant-project-id}.appspot.com
    webserver_id = airflow_url

    if use_experimental_api:
        endpoint = f'api/experimental/dags/'+airflow_dag_id+'/dagRuns'
    else:
        endpoint = f'/api/v1/dags/'+airflow_dag_id+'/dagRuns'

    payload = {
        "dag_run_id": dag_run_id,
        "data_interval_end": current_dateTime,
        "data_interval_start": current_dateTime
    }

    webserver_url = webserver_id + endpoint

    max_retries = 3
    retry_delay = 300  # seconds
    for attempt in range(max_retries):
        print(f"Attempt {attempt+1} to trigger DAG")
        # Make a POST request to IAP.
        response = make_iap_request(webserver_url, client_id, method='POST', data=json.dumps(payload))
        
        # Check the response status code
        if response.status_code == 200:
            print("DAG "+airflow_dag_id+" triggered successfully.")
            break
        elif response.status_code == 409:
            print("DAG "+airflow_dag_id+" was triggered already, DAG run id exists: "+ dag_run_id)
            break
        elif response.status_code == 404:
            if attempt < max_retries - 1:
                print(f"DAG {airflow_dag_id} not found. Will retry after {retry_delay} seconds.")
                time.sleep(retry_delay)
            else:
                print("DAG "+airflow_dag_id+" not found after maximum retries. Response code: ", response.status_code)
                print(response.text)
                print("Max retries reached. Exiting.")
                raise Exception(
                    'Bad response from application: {!r} / {!r}'.format(
                        response.status_code, response.text))
        else:
            print("Error: DAG "+airflow_dag_id+" not triggered. Response code: ", response.status_code)
            print(response.text)
            raise Exception(
                'Bad response from application: {!r} / {!r}'.format(
                    response.status_code, response.text))

if platform.lower() == "airflow":
    basicauthapi_get_dag(airflow_dag_id, airflow_user, airflow_pass)
    basicauthapi_enable_dag(airflow_dag_id, airflow_user, airflow_pass)
    basicauthapi_dag_run(airflow_dag_id, airflow_user, airflow_pass)
elif platform.lower() == "composer":
    client_id = get_client_id(project_id, location, composer_environment)
    googleauth_get_dag(airflow_dag_id, client_id)
    googleauth_enable_dag(airflow_dag_id, client_id)
    googleauth_dag_run(airflow_dag_id, dag_run_id, client_id)
else:
    print("Platform "+platform+" not in env variables.")
    sys.exit(1)
sys.exit(0)