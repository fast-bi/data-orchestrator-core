# This is an old version of the dbt_manifest_parser library 
# that has now moved to the fast_bi_dbt_runner package.

import json
import datetime
import textwrap
from kubernetes.client import models as k8s
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from itertools import chain

# Display deprecation message
deprecation_message = """
🚨 Deprecated Library Notice 🚨

The current version of the dbt_manifest_parser library is now deprecated. We have migrated its functionality to the fast_bi_dbt_runner package.

To continue using the features provided by dbt_manifest_parser, please update your dependencies to include fast_bi_dbt_runner instead.

📦 **Migration Steps:**
1. Install the new package:
   ```bash
   pip install fast_bi_dbt_runner 

# Replace the import statement
# from dbt_manifest_parser import ...
# with
from fast_bi_dbt_runner import ...

We appreciate your understanding as we strive to provide a more streamlined and efficient solution. If you encounter any issues during the migration process, feel free to reach out for assistance.

Thank you for your continued support!

Best regards,
Fast.BI Administration
"""

class CustomKubernetesPodOperator(KubernetesPodOperator):
    def execute(self, context):
        """
        Executes a KubernetesPodOperator task with customized exception handling.

        This custom operator extends the KubernetesPodOperator from the Airflow library.
        It overrides the execute method to catch AirflowException raised by the parent class.
        The error message is then shortened and a new AirflowException is raised with the
        shortened message. This approach allows for more concise error reporting while
        preserving the detailed error information.

        :param context: The context dictionary provided by Airflow.
        """
        try:
            super().execute(context)
        except AirflowException as e:
            # Shorten the error message for clearer reporting
            msg = str(e)
            short_msg = textwrap.shorten(msg, width=160)
            raise AirflowException(short_msg) from None

class DbtManifestParser:
    """
    A class analyses dbt project and parses manifest.json and creates the respective task groups
    :param manifest_path: Path to the directory containing the manifest files
    :param pod_name: name of Kubercluster pod
    :param dbt_tag: define different parts of a project. Have to be set as
    a list of one or a few values
    :param env_var: airflow variables
    :param image: docker image where define dbt command
    :param namespace: default
    :param CLUSTER_ZONE: Getting from Airflow environment variables {{ var.value.CLUSTER_ZONE }}
    :param CLUSTER_NAME: Getting from Airflow environment variables {{ var.value.CLUSTER_NAME_DBT }}
    """

    def __init__(
            self,
            dbt_tag,
            env_vars,
            airflow_vars,
            manifest_path=None,
            pod_name=None,
            image=None,
            namespace=None
    ) -> None:
        self.env_vars = env_vars
        self.airflow_vars = airflow_vars
        self.pod_name = pod_name
        self.dbt_tag = dbt_tag
        self.image = image
        self.namespace = namespace
        self.manifest_path = manifest_path
        self.manifest_data = self.load_dbt_manifest()
        self.dbt_tasks = {}

    def change_to_test_in_models_depends_on(self, nodes_list):
        models_tests_dict = {}
        for k, v in nodes_list.items():
            if v["resource_type"] == 'test':
                for depends_on_model in nodes_list[k]['depends_on']:
                    models_tests_dict.setdefault(depends_on_model, []).append(k)

        for k, v in nodes_list.items():
            temp_models_list = []
            if v["resource_type"] != 'test':
                for i in v['depends_on']:
                    if i.split(".")[1] != "re_data":
                        if i in models_tests_dict:
                            if i not in temp_models_list:
                                temp_models_list.extend(models_tests_dict[i])
                    if temp_models_list:
                        v['depends_on'] = list(set(temp_models_list))
        return nodes_list

    def delete_source_test(self, nodes_list):
        tests_without_source = {}
        for k, v in list(nodes_list.items()):
            if v["resource_type"] == 'test':
                if v.get("group_type"):
                    for i in v.get("group_type"):
                        if i == "source":
                            del nodes_list[k]
        return nodes_list

    def get_file_tests(self, nodes_list):
        for k, v in nodes_list.items():
            if v["resource_type"] == 'test':
                for i in v.get('depends_on'):
                    if i.split(".")[-1] != nodes_list[k].get('file_key_name'):
                        v['depends_on'].remove(i)
                        v['group_type'].remove(i.split(".")[0])
        return nodes_list

    def load_dbt_manifest(self):
        """
        Helper function to load the dbt manifest file.
        Returns: A JSON object containing the dbt manifest content.
        """
        with open(self.manifest_path, encoding="utf-8") as file:
            file_content = json.load(file)
            node_dependency = {
                k: v
                for k, v in file_content["nodes"].items()
                if k.split(".")[0] in ("model", "seed", "snapshot", "test")}
            node_dependency_unique = {
                k: {
                    "name": v["name"],
                    "alias": v["alias"],
                    "package_name": v["package_name"],
                    "resource_type": v["resource_type"],
                    "group_type": [v["resource_type"]] if v["resource_type"] != "test" else list(
                        set([i.split('.')[0] for i in v["depends_on"].get("nodes", [])])),
                    "depends_on": list(set(v["depends_on"].get("nodes", []))),
                    "tags": v["tags"],
                    "file_key_name": v.get("file_key_name", "").split(".")[-1]
                }
                for k, v in node_dependency.items()
                if 'depends_on' in v}

            if self.dbt_tag:
                for node in node_dependency_unique:
                    if node_dependency_unique[node].get('depends_on') and \
                            node_dependency_unique[node].get('resource_type') == 'test':
                        for dependent_node in node_dependency_unique[node].get('depends_on'):
                            if dependent_node.split(".")[0] == 'model':
                                node_dependency_unique[node]['tags'].extend(
                                    node_dependency_unique[dependent_node].get('tags'))

                node_dependency_unique = {k: v for k, v in node_dependency_unique.items() if self.dbt_tag in v["tags"]}
            node_dependency_unique = self.delete_source_test(node_dependency_unique)
            node_dependency_unique = self.get_file_tests(node_dependency_unique)
            node_dependency_unique = self.change_to_test_in_models_depends_on(node_dependency_unique)
        return node_dependency_unique

    def get_package_list(self):
        package_list = {}
        for node in self.manifest_data.keys():
            k = self.manifest_data[node].get('package_name')
            v = self.manifest_data[node].get('resource_type') # {dvo_shared: [seed, model, test]}
            if v not in package_list.get(k, []) and k != "re_data":
                package_list[k] = package_list.get(k, []) + [v]
        return package_list

    def filter_models(self, models_list):
        filtered_dict = {}
        for i in self.manifest_data.keys():
            if self.manifest_data[i]['name'] in models_list:
                filtered_dict[i] = self.manifest_data[i]
            if self.manifest_data[i]['depends_on'] and self.manifest_data[i]['resource_type'] == 'test':
                for depends_on_model in self.manifest_data[i]['depends_on']:
                    if depends_on_model.split(".")[-1] in models_list:
                        filtered_dict[i] = self.manifest_data[i]
        return filtered_dict

    def add_additional_env_variables(self, task_params, dbt_command, node_name):
        # create env_vars_with_model list as a copy of env_vars
        # to avoid redefinition of env_vars list
        env_vars_with_model = self.env_vars.copy()
        airflow_var = self.airflow_vars.copy()

        if task_params.get('full_refresh', None):
            if dbt_command != "test":
                dbt_command = task_params['full_refresh']

        env_vars_with_model.append(k8s.V1EnvVar(name="DBT_COMMAND", value=f"{dbt_command}"))
        # add to env_vars_with_model new variable MODEL that equal
        # to current DBT project model node_name
        env_vars_with_model.append(k8s.V1EnvVar(name="MODEL", value=f"{node_name}"))
        if dbt_command == "seed":
            env_vars_with_model.append(k8s.V1EnvVar(name="SEED", value="true"))
        else:
            env_vars_with_model.append(k8s.V1EnvVar(name="SEED", value="false"))

        if task_params:
            env_vars_with_model_keys = [i.name for i in env_vars_with_model]
            kuber_dag_new_params = [k8s.V1EnvVar(name=k, value=str(v)) for k, v in task_params.items() if
                                    k not in env_vars_with_model_keys]
            env_vars_with_model.extend(kuber_dag_new_params)
            airflow_var = {**task_params, **airflow_var}

        if dbt_command == "snapshot":
            env_vars_with_model.append(k8s.V1EnvVar(name="SNAPSHOT", value="true"))
            if not airflow_var.get('DBT_SNAPSHOT_INTERVAL') or airflow_var['DBT_SNAPSHOT_INTERVAL'] == 'daily':
                env_vars_with_model.append(k8s.V1EnvVar(name="SNAPSHOT_RUN_PERIOD", value="true"))
            else:
                env_vars_with_model.append(k8s.V1EnvVar(name="SNAPSHOT_RUN_PERIOD", value="false"))

            if airflow_var.get('DBT_DAG_RUN_DATE'):
                date_input = datetime.datetime.strptime(airflow_var['DBT_DAG_RUN_DATE'], "%Y-%m-%d")
                if airflow_var.get('DBT_SNAPSHOT_VALID_FROM'):
                    date_input_from = datetime.datetime.strptime(airflow_var['DBT_SNAPSHOT_VALID_FROM'], "%Y-%m-%d")
                    if airflow_var.get('DBT_SNAPSHOT_VALID_TO'):
                        date_input_to = datetime.datetime.strptime(airflow_var['DBT_SNAPSHOT_VALID_TO'], "%Y-%m-%d")

                        if airflow_var['DBT_SNAPSHOT_INTERVAL'] == 'monthly' \
                                and date_input_from.day <= date_input.day <= date_input_to.day:
                            env_vars_with_model.append(k8s.V1EnvVar(name="SNAPSHOT_RUN_PERIOD", value="true"))
                        elif airflow_var['DBT_SNAPSHOT_INTERVAL'] == 'weekly' \
                                and date_input_from.weekday() <= date_input.weekday() <= date_input_to.weekday():
                            env_vars_with_model.append(k8s.V1EnvVar(name="SNAPSHOT_RUN_PERIOD", value="true"))
        else:
            env_vars_with_model.append(k8s.V1EnvVar(name="SNAPSHOT", value="false"))
        return env_vars_with_model

    def create_dbt_kuberoperator_task(self, dbt_command, running_rule, task_params={}, node_name=None, node_alias=None):
        """
        Takes the manifest JSON content and returns a KubernetesPodOperator task
        to run a dbt command.
        Args:
            node_name: The name of the node from manifest.json. By default is equal to None
            dbt_command: dbt command: run, test
            running_rule: argument which defines the rule by which the generated task get triggered
        Returns: A KubernetesPodOperator task that runs the respective dbt command
        :param dbt_command:
        :param running_rule:
        :param node_name:
        :param task_params:
        """

        if node_name is None:
            node_name = dbt_command + "_all_models"
            node_alias = dbt_command + "_all_models"

        env_vars_with_model = self.add_additional_env_variables(task_params, dbt_command, node_name)
        """
        Create KubernetesPodOperator operators
        Args:
        task_id: The ID specified for the task
            name: Name of task you want to run, used to generate Pod ID
            namespace: The namespace to run within Kubernetes
            image: Docker image specified
            trigger_rule: the conditions that Airflow applies to tasks to
            determine whether they are ready to execute.
            ALL_DONE - all upstream tasks are done with their execution
        """
        dbt_task = CustomKubernetesPodOperator(
            task_id=node_alias,
            name=self.pod_name + "_" + node_alias,
            namespace=self.namespace,
            image=self.image,
            # Debug - uncomment for debug the container.
            # cmds=["bash", "-cx"],
            # arguments=[f" tail -f /dev/null "],
            # trigger_rule=TriggerRule.ALL_DONE,
            # Optional, run with specific K8S SA Account
            # service_account_name="airflow",
            trigger_rule=running_rule,
            labels={"app": "dbt"},
            env_vars=env_vars_with_model,
            image_pull_policy="IfNotPresent",
            do_xcom_push=False,
            startup_timeout_seconds=300,
            is_delete_operator_pod=True,
            get_logs=True,
            container_resources=k8s.V1ResourceRequirements(
                requests={"memory": "128Mi", "cpu": "100m"},
            ),
        )
        return dbt_task

    def set_group_name(self, resource_type_item, dbt_command, task_params=None, package=None):
        if dbt_command == "test":
            group_name = task_params.get("resource_type") + "_" + dbt_command if task_params else dbt_command
        else:
            group_name = resource_type_item
        if package:
            group_name = group_name + "_" + package
        return group_name

    def create_dbt_task_groups(
            self,
            group_name,
            resource_type,
            dbt_command,
            running_rule,
            task_params={}):
        """
        Parse out a JSON file and populates the task groups with dbt tasks
        Args:
            group_name: name of Task Groups uses for DAGs graph view in the Airflow UI.
            resource_type: type of manifest nodes group: model, seed, snapshot
            package_name: project name, tag that define by which certain records
            from the manifest nodes will be selected
            dbt_command: dbt command run or test
            running_rule: trigger rule
            task_params: parameters that was added in the task
        Returns: task group
        """
        task_params = {k: v for k, v in task_params.items() if v}  #get only not empty value in task_params
        if "full_refresh_model_name" in task_params:
            self.manifest_data = self.filter_models(task_params["full_refresh_model_name"])

        package_list = self.get_package_list()
        resource_type_list = list(set(chain(*package_list.values())))
        for resource_type_item in resource_type_list:
            if resource_type == resource_type_item:
                with TaskGroup(group_id=self.set_group_name(resource_type_item, dbt_command,
                                                            task_params)) as resource_type_group:
                    for package in package_list.keys():
                        if resource_type_item in package_list[package]:
                            with TaskGroup(group_id=package) as task_group:
                                for node in self.manifest_data.keys():
                                    node_name = self.manifest_data[node]['name']
                                    node_alias = self.manifest_data[node]['alias']
                                    if resource_type_item in self.manifest_data[node]['group_type'] \
                                            and self.manifest_data[node]['package_name'] == package:
                                        if self.manifest_data[node]['resource_type'] == "test":
                                            dbt_command = "test"
                                        self.dbt_tasks[node] = self.create_dbt_kuberoperator_task(
                                            dbt_command, running_rule, task_params, node_name, node_alias)

                                for node in self.manifest_data.keys():
                                    if resource_type_item in self.manifest_data[node]['group_type'] \
                                            and node.split(".")[1] == package:
                                        for upstream_node in self.manifest_data[node].get("depends_on", []):
                                            if self.dbt_tasks.get(upstream_node, []):
                                                self.dbt_tasks[upstream_node] >> self.dbt_tasks[node]
                return resource_type_group
