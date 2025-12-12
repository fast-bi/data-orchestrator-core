import sys
import json
import re
import os
from datetime import timedelta, date, datetime
import pendulum
from airflow import models
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from airflow.models import Variable, XCom
from airflow.utils.log.secrets_masker import mask_secret
from airflow.operators.python_operator import PythonOperator
from airflow.logging_config import log
from kubernetes.client import models as k8s
from fast_bi_dbt_runner.airbyte_task_group_builder import TaskBuilder
from fast_bi_dbt_runner.dbt_manifest_parser_api_operator import DbtManifestParser
from airflow.utils.db import provide_session
from airflow.models.param import Param
from airflow.exceptions import AirflowFailException
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup

sys.tracebacklimit = 0

app_version = "2.0"

# PROJECT_VARS_LIST - Secrets which are stored in Airflow variables as secret.
PROJECT_VARS_LIST = "AIRFLOW_VARS_SECRETS"


def convert_to_lower(str_obj):
    return str_obj.lower() if str_obj else None


def define_env_variables():
    all_vars = Variable.get(PROJECT_VARS_LIST, deserialize_json=True)
    for k, v in dict(all_vars).items():
        if re.search(r'\b(SECRET|TOKEN)\b', k, flags=re.I):
            mask_secret(v)
    env_vars = [
        k8s.V1EnvVar(
            name=k, value="{{{{ var.json.{}.{} }}}}".format(PROJECT_VARS_LIST, k)
        )
        for k, v in all_vars.items()
    ]
    return all_vars, env_vars


airflow_vars, env_var = define_env_variables()
DAG_FOLDER_NAME = airflow_vars.get("DBT_PROJECT_NAME")

if airflow_vars["PLATFORM"].lower() == "airflow":
    DBT_DIR_DAG_LEVEL = f"/opt/airflow/dags/repo/dags/{DAG_FOLDER_NAME}"
    DBT_DIR_MAIN_LEVEL = f"/opt/airflow/dags/repo/dags/main"

elif airflow_vars["PLATFORM"].lower() == "composer":
    DBT_DIR_DAG_LEVEL = f"/home/airflow/gcs/dags/{DAG_FOLDER_NAME}"
    DBT_DIR_MAIN_LEVEL = f"/home/airflow/gcs/dags/main"


# path to the directory containing the manifest file
if os.path.isfile(f"{DBT_DIR_DAG_LEVEL}/{airflow_vars['MANIFEST_NAME']}.json"):
    MANIFEST_PATH = f"{DBT_DIR_DAG_LEVEL}/{airflow_vars['MANIFEST_NAME']}.json"
else:
    MANIFEST_PATH = f"{DBT_DIR_MAIN_LEVEL}/{airflow_vars['MANIFEST_NAME']}.json"

# DAG_ID - Name of Dag in Airflow UI.
DAG_ID = airflow_vars.get("DAG_ID")
# DAG_TAG - Tag name of Dag in Airflow UI.
DAG_TAG = airflow_vars.get("DAG_TAG")
if not isinstance(DAG_TAG, list):
    DAG_TAG = [DAG_TAG]

# DBT_TAG - DBT Project models tag, if only tagged modules needs to run.
DBT_TAG = airflow_vars.get("DBT_TAGS") or airflow_vars.get("DBT_TAG") 
DBT_TAG_ANCESTORS = airflow_vars.get("DBT_TAG_ANCESTORS", True)
DBT_TAG_DESENDANTS = airflow_vars.get("DBT_TAG_DESENDANTS", False) 
DBT_WORKLOAD_API_RELEASE = airflow_vars.get("DBT_WORKLOAD_API_RELEASE")
DBT_SNAPSHOT = convert_to_lower(airflow_vars.get("DBT_SNAPSHOT"))
DBT_SNAPSHOT_SHARDING = convert_to_lower(airflow_vars.get("DBT_SNAPSHOT_SHARDING"))
DBT_SEED = convert_to_lower(airflow_vars.get("DBT_SEED"))
DBT_SEED_SHARDING = convert_to_lower(airflow_vars.get("DBT_SEED_SHARDING"))
DBT_SOURCE = convert_to_lower(airflow_vars.get("DBT_SOURCE", "True"))
DBT_SOURCE_SHARDING = convert_to_lower(airflow_vars.get("DBT_SOURCE_SHARDING", "True"))
DATA_QUALITY = convert_to_lower(airflow_vars.get("DATA_QUALITY"))
DBT_PROJECT_NAME = airflow_vars.get("DBT_PROJECT_NAME")
project_dir = f"/data/dbt_projects/{DBT_PROJECT_NAME}"
CONNECTION_ID = airflow_vars.get("DBT_WORKLOAD_API_CONNECTION_ID")
DAG_OWNER = airflow_vars.get("DAG_OWNER", "fast.bi")
DAG_START_DATE = airflow_vars.get("DAG_START_DATE", "days_ago(1)")

AIRBYTE_REPLICATION_FLAG = airflow_vars.get("AIRBYTE_REPLICATION_FLAG", "False")
if isinstance(AIRBYTE_REPLICATION_FLAG, str) and AIRBYTE_REPLICATION_FLAG.lower() == "true":
    AIRBYTE_REPLICATION_FLAG = True
else:
    AIRBYTE_REPLICATION_FLAG = False

AIRBYTE_CONNECTION_IDS = airflow_vars.get("AIRBYTE_CONNECTION_ID")
if not isinstance(AIRBYTE_CONNECTION_IDS, list):
    AIRBYTE_CONNECTION_IDS = [AIRBYTE_CONNECTION_IDS]


# Cluster variables
NAMESPACE = airflow_vars.get("NAMESPACE")

IMAGE = airflow_vars.get("IMAGE")

# Project level variables
DAG_SCHEDULE_INTERVAL = airflow_vars.get("DAG_SCHEDULE_INTERVAL")
# EXTERNAL_RUN_DATETIME_FORMAT = "%Y-%m-%d"
# AIRFLOW_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f+00:00"

# Get timezone from Airflow config or fallback to variable
try:
    from airflow.configuration import conf
    DAG_TIMEZONE = conf.get('core', 'default_timezone', fallback=None)
    if not DAG_TIMEZONE or DAG_TIMEZONE == 'system':
        DAG_TIMEZONE = airflow_vars.get("DAG_TIMEZONE", "UTC")
except Exception:
    DAG_TIMEZONE = airflow_vars.get("DAG_TIMEZONE", "UTC")

xcom_execution_date = '{{ti.xcom_pull(task_ids="show_input_data", key="execution_date")}}'
xcom_full_refresh_model_name = '{{ti.xcom_pull(task_ids="show_input_data", key="full_refresh_model_name")}}'

# create DbtManifestParser class object
dag_parser = DbtManifestParser(
    manifest_path=MANIFEST_PATH,  # json object that contains data from manifest.json
    dbt_tag=DBT_TAG,
    # class attributes need to create GKEStartPodOperator
    image=IMAGE,
    env_vars=env_var,
    airflow_vars=airflow_vars,
    namespace=NAMESPACE,
    connection_id=CONNECTION_ID,
    dbt_tag_ancestors=DBT_TAG_ANCESTORS,
    dbt_tag_descendants=DBT_TAG_DESENDANTS
)

# Validate START_DATE
try:
    START_DATE = dag_parser.get_valid_start_date(DAG_START_DATE)
except ValueError as e:
    raise ValueError(f"Error in START_DATE: {e}")

# Convert start_date to timezone-aware datetime
dag_tz = pendulum.timezone(DAG_TIMEZONE)
if START_DATE.tzinfo is None:
    # If start_date is naive, localize it to the specified timezone
    START_DATE = pendulum.instance(START_DATE).in_timezone(dag_tz)
else:
    # If start_date already has timezone, convert it to the specified timezone
    START_DATE = pendulum.instance(START_DATE).in_timezone(dag_tz)

airbyte_builder = TaskBuilder(
    connection_ids=AIRBYTE_CONNECTION_IDS
)

# Create a DAG Object
with models.DAG(
        DAG_ID,  # dag id
        catchup=False, # disable catchup
        schedule_interval=DAG_SCHEDULE_INTERVAL,  # override to match your needs
        start_date=START_DATE,  # start date, parameter, this is the
        description=f"Release: {DBT_WORKLOAD_API_RELEASE}",
        # point from where the scheduler will start filling in the dates
        max_active_tasks=100,
        concurrency=100,
        default_args={
            "retries": 1,  # the number of retries that should be performed before failing the task
            "retry_delay": timedelta(seconds=30),  # delay between retries
            "owner": DAG_OWNER
        },
        params={"full_refresh": Param(False, type="boolean"),
                "model_name": "",
                "project": "",
                "execution_date": date.today()},
        tags=DAG_TAG,  # a tag name per dag, to allows quick filtering in the DAG view.
) as dag:

    def check_all_tasks_status(**context):
        """Check the status of all tasks in the DAG and fail if any task has failed."""
        dag_run = context['dag_run']
        task_instances = dag_run.get_task_instances()

        for task_instance in task_instances:
            if task_instance.state == 'failed':
                raise AirflowFailException("DAG failed because one or more tasks have failed.")


    def f_cleanup_temp_airflow_vars(temp_airflow_vars):
        Variable.delete(temp_airflow_vars)

    @provide_session
    def f_show_input_data(session=None, **kwargs):
        dag = kwargs["dag"]
        dag_id = dag._dag_id
        dag_run = kwargs.get('dag_run')
        # It will delete all xcom of the dag_id
        session.query(XCom).filter(XCom.dag_id == dag_id, XCom.key == "pod_name").delete()
        session.query(XCom).filter(XCom.dag_id == dag_id, XCom.key == "pod_namespace").delete()
        log.info(f"all xcom variables have been removed")

        temp_dict = {}
        datetime_argument = kwargs["data_interval_end"].strftime('%Y-%m-%d')
        if dag_run and dag_run.conf:
            if "execution_date" in dag_run.conf and dag_run.conf["execution_date"]:
                log.info(f'dag_run.conf:  {dag_run.conf}')
                try:
                    datetime.strptime(dag_run.conf["execution_date"], '%Y-%m-%d')
                    datetime_argument = dag_run.conf["execution_date"]
                    log.info(f"date_argument from input dag_run.config:  {datetime_argument}")
                except ValueError:
                    raise ValueError("Incorrect data format, should be YYYY-MM-DD")
            else:
                log.info(f"date_argument data_interval_end (execution_date):  {datetime_argument}")

        else:
            log.info(f"DAG was run by schedule:  {datetime_argument}")

        if airflow_vars.get("DBT_DAG_RUN_DATE"):
            airflow_vars["DBT_DAG_RUN_DATE"] = datetime_argument
            Variable.set(key=f"{PROJECT_VARS_LIST}", value=json.dumps(airflow_vars))
            log.info(f"{PROJECT_VARS_LIST}.DBT_DAG_RUN_DATE attribute was updated: {datetime_argument}")

        if "full_refresh" in dag_run.conf:
            full_refresh_value = dag_run.conf["full_refresh"]
            log.info(f"full_refresh:  {full_refresh_value}")
            temp_dict['full_refresh'] = full_refresh_value

        if "model_name" in dag_run.conf:
            full_refresh_model_name = dag_run.conf["model_name"]
            kwargs["ti"].xcom_push(key="full_refresh_model_name", value=full_refresh_model_name)
            temp_dict['full_refresh_model_name'] = full_refresh_model_name
            log.info(f"full_refresh_model_name:  {full_refresh_model_name}")

        if temp_dict:
            Variable.set(key=f"temp_var_{DAG_ID}", value=json.dumps(temp_dict))
        kwargs["ti"].xcom_push(key="execution_date", value=datetime_argument)


    show_input_data = PythonOperator(
        task_id="show_input_data",
        python_callable=f_show_input_data,
        provide_context=True,
    )

    if AIRBYTE_CONNECTION_IDS and AIRBYTE_REPLICATION_FLAG:
        airbyte_group = airbyte_builder.build_tasks(
            connection_ids=AIRBYTE_CONNECTION_IDS
        )
        airbyte_group >> show_input_data

    task_list = []
    temp_var = Variable.get(f"temp_var_{DAG_ID}", deserialize_json=True, default_var=None)
    full_refresh_model_name_list = temp_var.get("full_refresh_model_name") if temp_var else []
    full_refresh = temp_var.get("full_refresh") if temp_var else False
    if full_refresh_model_name_list:
        DBT_SEED = "false" if full_refresh_model_name_list and DBT_SEED_SHARDING == "false" else DBT_SEED
        DBT_SNAPSHOT = "false" if full_refresh_model_name_list and DBT_SNAPSHOT_SHARDING == "false" else DBT_SNAPSHOT
        DBT_SOURCE = "false" if full_refresh_model_name_list and DBT_SOURCE_SHARDING == "false" else DBT_SOURCE

    if dag_parser.is_resource_type_in_manifest("seed"):
        if DBT_SEED == "true":
            if DBT_SEED_SHARDING == "true":
                dbt_seed_files = dag_parser.create_dbt_task_groups(
                    group_name="seeds",
                    resource_type="seed",
                    project_dir=project_dir,
                    dbt_command="seed",
                    running_rule=TriggerRule.ALL_SUCCESS,
                    task_params={"full_refresh": full_refresh,
                                 "full_refresh_model_name": full_refresh_model_name_list })
                task_list.append(dbt_seed_files)
            else:
                dbt_seed_all_files = dag_parser.create_api_task(
                    dbt_command="seed",
                    project_dir=project_dir,
                    running_rule=TriggerRule.ALL_SUCCESS,
                    task_params={"full_refresh": full_refresh,
                                 "full_refresh_model_name": full_refresh_model_name_list}
                )
                task_list.append(dbt_seed_all_files)
        else:
            log.info("dbt seed not enabled.")

    if dag_parser.is_resource_type_in_manifest("source"):
        if DBT_SOURCE == "true":
            if DBT_SOURCE_SHARDING == "true":
                dbt_sources_models = dag_parser.create_dbt_task_groups(
                    group_name="sources",
                    resource_type="source",
                    project_dir=project_dir,
                    dbt_command="freshness",
                    running_rule=TriggerRule.ALL_SUCCESS,
                    task_params={"full_refresh_model_name": full_refresh_model_name_list,
                                 "DBT_VAR": "'execution_date': '" + xcom_execution_date + "'"})
                task_list.append(dbt_sources_models)
            else:
                dbt_source_all_files = dag_parser.create_api_task(
                    dbt_command="freshness",
                    project_dir=project_dir,
                    running_rule=TriggerRule.ALL_SUCCESS,
                    task_params={"full_refresh_model_name": full_refresh_model_name_list,
                                 "DBT_VAR": "'execution_date': '" + xcom_execution_date + "'"})
                task_list.append(dbt_source_all_files)

    if dag_parser.is_resource_type_in_manifest("model"):
        dbt_run_models = dag_parser.create_dbt_task_groups(
            group_name="models",
            resource_type="model",
            project_dir=project_dir,
            dbt_command="run",
            running_rule=TriggerRule.ALL_SUCCESS,
            task_params={"full_refresh": full_refresh,
                         "full_refresh_model_name": full_refresh_model_name_list,
                         "DBT_VAR": "'execution_date': '" + xcom_execution_date + "'"})
        task_list.append(dbt_run_models)

    """ run method create_dbt_task that run command dbt snapshot
            snapshot: dbt command -> dbt snapshot
    """
    if dag_parser.is_resource_type_in_manifest("snapshot"):
        if DBT_SNAPSHOT == "true":
            if DBT_SNAPSHOT_SHARDING == "true":
                dbt_snapshot_models = dag_parser.create_dbt_task_groups(
                    group_name="snapshots",
                    resource_type="snapshot",
                    project_dir=project_dir,
                    dbt_command="snapshot",
                    running_rule=TriggerRule.ALL_SUCCESS
                    # , Variable.get(PROJECT_VARS_LIST, deserialize_json=True)
                )
                task_list.append(dbt_snapshot_models)
            else:
                dbt_snapshot_all_models = dag_parser.create_api_task(
                    dbt_command="snapshot",
                    running_rule=TriggerRule.ALL_SUCCESS,
                    project_dir=project_dir
                    # , Variable.get(PROJECT_VARS_LIST, deserialize_json=True)
                )
                task_list.append(dbt_snapshot_all_models)
        else:
            log.info("dbt snapshot not enabled.")

    if DATA_QUALITY == "true":
        re_data_models = dag_parser.create_api_task(dbt_command="re_data",
                                                    project_dir=project_dir,
                                                    running_rule=TriggerRule.ALL_DONE)
        task_list.append(re_data_models)

    task_list = list(filter(None, task_list))

   # Add cleanup task with ALL_DONE trigger rule to ensure it runs regardless of task status
    if Variable.get(f"temp_var_{DAG_ID}", None):
        cleanup_temp_airflow_vars = PythonOperator(
            task_id="cleanup_temp_airflow_vars",
            python_callable=f_cleanup_temp_airflow_vars,
            op_kwargs={"temp_airflow_vars": f"temp_var_{DAG_ID}"},
            trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of upstream task status
        )
        if task_list:
            task_list[-1] >> cleanup_temp_airflow_vars
        else:
            show_input_data >> cleanup_temp_airflow_vars

    # Create a final task to check all task statuses
    check_status = PythonOperator(
        task_id="check_all_tasks_status",
        python_callable=check_all_tasks_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,  # This ensures it runs even if some tasks fail
    )

    if task_list:
        for task_group in task_list:
            if isinstance(task_group, TaskGroup):
                # if task_group is TaskGroup
                for task_id in task_group:
                    if not task_id.upstream_task_ids:
                        if task_list.index(task_group) == 0:
                            show_input_data >> task_group
                        if task_list.index(task_group) > 0:
                            task_list[task_list.index(task_group) - 1] >> task_group
                        if task_list.index(task_group) + 1 < len(task_list):
                            task_group >> task_list[task_list.index(task_group) + 1]
            else:
                # if task_group is task (seed_all_files, snapshot_all_files)
                idx = task_list.index(task_group)
                if idx == 0:
                    show_input_data >> task_group
                if idx > 0:
                    task_list[idx - 1] >> task_group
                if idx + 1 < len(task_list):
                    task_group >> task_list[idx + 1]

        task_list[-1] >> check_status
    else:
        show_input_data >> check_status

