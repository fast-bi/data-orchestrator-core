import os
import re
from datetime import datetime
import yaml
import uuid
import argparse

def get_env_var(var_name, default_value):
    """Get environment variable checking both uppercase and lowercase variants."""
    return os.environ.get(var_name.upper(), os.environ.get(var_name.lower(), default_value))

def parse_arguments():
    parser = argparse.ArgumentParser(description='Create DAG from template with optional parameters')
    parser.add_argument('--variables-file', help='Path to the variables YAML file')
    parser.add_argument('--dag-id', help='DAG ID to use')
    return parser.parse_args()

# Parse command line arguments
args = parse_arguments()

source_gke_file = get_env_var('source_gke_file', 'gke_DAG_template')
source_k8s_file = get_env_var('source_k8s_file', 'k8s_DAG_template')
source_api_file = get_env_var('source_api_file', 'api_DAG_template')
source_bash_file = get_env_var('source_bash_file', 'bash_DAG_template')

# Use command line argument if provided, otherwise fall back to environment variable
source_yml_file = args.variables_file if args.variables_file else get_env_var('airflow_secret_file_name', 'dbt_airflow_variables.yml')
source_commit_id = get_env_var('source_commit_id', None)
cd_cd_job_id = get_env_var('cd_cd_job_id', None)

current_datetime = datetime.now()
date_format = f"{current_datetime.year}_{current_datetime.month}_{current_datetime.day}_{current_datetime.hour}_{current_datetime.minute}_{current_datetime.second}_{current_datetime.microsecond}"

if source_commit_id is not None:
    dag_version = "v."+source_commit_id
else:
    uuid_value = uuid.uuid4()
    dag_version = "v."+str(uuid_value.hex)[:8]

with open(source_yml_file, 'r') as file:
    config_vars = yaml.safe_load(file)

PROJECT_VARS_LIST = list(config_vars.keys())[0]
OPERATOR = config_vars[PROJECT_VARS_LIST]["OPERATOR"]
# Use command line argument if provided, otherwise use from config
DBT_PROJECT_DAG_ID = args.dag_id if args.dag_id else config_vars[PROJECT_VARS_LIST]["DAG_ID"]

target_file = f"{DBT_PROJECT_DAG_ID}_dag.py"

def change_template_params(project_vars_list, dbt_project_name, source_file, target_file):
    with open(source_file, mode="r", encoding="utf-8") as file:
        filedata = file.read()
        # Replace the target string
        filedata = filedata.replace("AIRFLOW_VARS_SECRETS", project_vars_list)

    with open(target_file, mode="w", encoding="utf-8") as file:
        file.write(filedata)

def dag_version_tag(dag_version, source_file, target_file, dag_job_id = None):
    # Open the Python file for reading
    with open(source_file, mode="r", encoding="utf-8") as file:
        file_contents = file.read()
        # Find and replace the app_version value
        if dag_job_id:
            add_version_tag = re.sub(r'app_version\s*=\s*".*"', f'app_version = "{dag_version}"\ncd_cd_job_id = "{dag_job_id}" \n', file_contents)
        else:
            add_version_tag = re.sub(r'app_version\s*=\s*".*"', f'app_version = "{dag_version}"', file_contents)

    # Open the Python file for writing and write the new contents
    with open(target_file, mode="w", encoding="utf-8") as file:
        file.write(add_version_tag)

if OPERATOR.lower() == 'gke':
    change_template_params(PROJECT_VARS_LIST, DBT_PROJECT_DAG_ID, source_gke_file, target_file)
    dag_version_tag(dag_version, target_file, target_file, cd_cd_job_id)
    print(date_format)
    print("DAG for GKE Operator was created successfully.")
    print("DAG Version: "+dag_version)
elif OPERATOR.lower() == 'k8s':
    change_template_params(PROJECT_VARS_LIST, DBT_PROJECT_DAG_ID, source_k8s_file, target_file)
    dag_version_tag(dag_version, target_file, target_file, cd_cd_job_id)
    print(date_format)
    print("DAG for K8S Operator was created successfully.")
    print("DAG Version: "+dag_version)
elif OPERATOR.lower() == 'api':
    change_template_params(PROJECT_VARS_LIST, DBT_PROJECT_DAG_ID, source_api_file, target_file)
    dag_version_tag(dag_version, target_file, target_file, cd_cd_job_id)
    print(date_format)
    print("DAG for API Operator was created successfully.")
    print("DAG Version: "+dag_version)
elif OPERATOR.lower() == 'bash':
    change_template_params(PROJECT_VARS_LIST, DBT_PROJECT_DAG_ID, source_bash_file, target_file)
    dag_version_tag(dag_version, target_file, target_file, cd_cd_job_id)
    print(date_format)
    print("DAG for Bash Operator was created successfully.")
    print("DAG Version: "+dag_version)
