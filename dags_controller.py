from datetime import datetime
import yaml
from pathlib import Path
import pendulum
import re

from airflow import DAG
from airflow.models import Variable
try:
    from airflow.providers.standard.operators.python import (
        PythonOperator,
        ShortCircuitOperator,
    )
except ModuleNotFoundError:
    from airflow.operators.python import (
        PythonOperator,
        ShortCircuitOperator,
    )
try:
    # Airflow 3
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ModuleNotFoundError:
    # Airflow 2
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Load YAML config
CONFIG_FILE = Variable.get(
    "DAG_CONFIG_FILE",
    "dynamic_dag_config.yml"
)

# get project name from folder
project_name = Path(__file__).parent.name

# build full path
config_path = (
    Path(__file__).parent.parent 
    / "dbt"
    / project_name
    / CONFIG_FILE
)

# safety check
if not config_path.exists():
    raise FileNotFoundError(f"Config file not found: {config_path}")

with open(config_path) as f:
    config = yaml.safe_load(f)

dags_config = config.get("dags", {})
controller_config = config.get("controller", {})


# Parse start_date (supports days_ago)
def parse_start_date(start_date_str, tz="UTC"):
    if re.fullmatch(r"days_ago\(\d+\)", start_date_str):
        days = int(re.findall(r"\d+", start_date_str)[0])
        return pendulum.now(tz).subtract(days=days)

    return pendulum.parse(start_date_str, tz=tz)


# Controller config
dag_id = controller_config.get("DAG_ID", project_name+"_"+"dags_controller")

#dag_id = controller_config.get("DAG_ID", "dags_controller")
schedule = controller_config.get("DAG_SCHEDULE_INTERVAL", "*/30 * * * *")
catchup = controller_config.get("CATCHUP", False)
max_active_runs = controller_config.get("MAX_ACTIVE_RUNS", 1)
tags = controller_config.get("TAGS", ["dynamic", project_name+"_dag_controller"])

tz = controller_config.get("TIMEZONE", "UTC")
start_date_str = controller_config.get("START_DATE", "days_ago(1)")

start_date = parse_start_date(start_date_str, tz)


# Cron → condition logic
def cron_to_condition(cron_expr, timezone="Europe/Vilnius"):
    cron_expr = cron_expr.strip()

    def _inner(**context):
        logical_date = context["data_interval_end"].in_timezone(timezone)

        # handle presets
        if cron_expr == "@hourly":
            return logical_date.minute == 0

        if cron_expr == "@daily":
            return logical_date.hour == 0 and logical_date.minute == 0

        if cron_expr == "@monthly":
            return (
                logical_date.day == 1
                and logical_date.hour == 0
                and logical_date.minute == 0
            )

        minute, hour, day, month, weekday = cron_expr.split()

        def match(value, current):
            if value == "*":
                return True
            if value.startswith("*/"):
                return current % int(value[2:]) == 0
            return int(value) == current

        return (
            match(minute, logical_date.minute)
            and match(hour, logical_date.hour)
            and match(day, logical_date.day)
        )

    return _inner


# Filter controlled DAGs
controlled_dags = {
    name: conf
    for name, conf in dags_config.items()
    if conf.get("USE_CONTROLLER", True)
}


# DAG definition
with DAG(
    dag_id=dag_id,
    start_date=start_date,
    schedule=schedule,
    catchup=catchup,
    max_active_runs=max_active_runs,
    tags=tags,
) as dag:

    tasks = {}

    # Create tasks dynamically
    for dag_name, dag_conf in controlled_dags.items():
        dag_id = dag_conf["DAG_ID"]
        schedule = dag_conf.get("DAG_SCHEDULE_INTERVAL")

        if not schedule:
            raise ValueError(
                f"{dag_name} must have DAG_SCHEDULE_INTERVAL when USE_CONTROLLER=True"
            )

        condition = ShortCircuitOperator(
            task_id=f"should_run_{dag_name}",
            python_callable=cron_to_condition(schedule),
        )

        trigger = TriggerDagRunOperator(
            task_id=f"trigger_{dag_name}",
            trigger_dag_id=dag_id,
            wait_for_completion=True,
            reset_dag_run=True,
        )

        condition >> trigger

        tasks[dag_name] = {
            "condition": condition,
            "trigger": trigger,
        }

    # Dependencies (from YAML)
    for dag_name, dag_conf in controlled_dags.items():
        parents = dag_conf.get("DEPENDS_ON", [])

        for parent in parents:
            if parent not in tasks:
                continue  # skip non-controller DAGs

            tasks[parent]["trigger"] >> tasks[dag_name]["condition"]
