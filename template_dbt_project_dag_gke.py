import sys
import json
import re
import os
from datetime import timedelta, date, datetime
from airflow import models
from airflow.utils.log.secrets_masker import mask_secret
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from airflow.logging_config import log
from airflow.models import Variable, XCom
from airflow.hooks.base import  BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKECreateCustomResourceOperator
)
from kubernetes.client import models as k8s
from fast_bi_dbt_runner.airbyte_task_group_builder import TaskBuilder
from fast_bi_dbt_runner.dbt_manifest_parser_gke_operator import DbtManifestParser
from airflow.utils.db import provide_session
from airflow.models.param import Param
from airflow.exceptions import AirflowFailException
from airflow.utils.state import State


sys.tracebacklimit = 0

app_version = "2.0"

# PROJECT_VARS_LIST - Secrets which are stored in Airflow variables as secret.
PROJECT_VARS_LIST = "AIRFLOW_VARS_SECRETS"


def convert_to_lower(str_obj):
    return str_obj.lower() if str_obj else None


def get_keyfile_from_connection():
    gcd_conn = BaseHook.get_connection('google_cloud_default')
    return json.loads(gcd_conn.extra_dejson.get('keyfile_dict', '{}'))


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

# POD_NAME - Kubernetes Pod name when dbt tasks is running.
POD_NAME = airflow_vars.get("POD_NAME")

# DBT_TAG - DBT Project models tag, if only tagged modules needs to run.
DBT_TAG = airflow_vars.get("DBT_TAGS") or airflow_vars.get("DBT_TAG") 
DBT_TAG_ANCESTORS = airflow_vars.get("DBT_TAG_ANCESTORS", True)
DBT_TAG_DESENDANTS = airflow_vars.get("DBT_TAG_DESENDANTS", False) 

NAMESPACE = airflow_vars.get("NAMESPACE")
# Cluster variables
NETWORK = airflow_vars.get("NETWORK")
SUBNETWORK = airflow_vars.get("SUBNETWORK")

IMAGE = airflow_vars.get("IMAGE")
PRIVATENODES_IP = airflow_vars.get("PRIVATENODES_IP")

PRIVATENODES = {"enablePrivateNodes": True, "masterIpv4CidrBlock": PRIVATENODES_IP}
MASTERAUTHORIZEDNETWORKSCONFIG = {
    "cidrBlocks": [{"cidrBlock": "0.0.0.0/0", "displayName": "allow-master"}],
    "enabled": True,
}

# Project level variables
PROJECT_ID = airflow_vars.get("PROJECT_ID")
CLUSTER_ZONE = airflow_vars.get("CLUSTER_ZONE")
CLUSTER_NAME = airflow_vars.get("CLUSTER_NAME")
CLUSTER_MACHINE_DISK_TYPE = airflow_vars.get("CLUSTER_MACHINE_DISK_TYPE")
CLUSTER_MACHINE_TYPE = airflow_vars.get("CLUSTER_MACHINE_TYPE")
CLUSTER_NODE_COUNT = airflow_vars.get("CLUSTER_NODE_COUNT")
CLUSTER_NODE_SA = airflow_vars.get("CLUSTER_NODE_SA") if airflow_vars.get("CLUSTER_NODE_SA") else get_keyfile_from_connection().get("client_email", None)
FAST_BI_COMMON_SA_SECRET = "eyJhdXRocyI6eyJldXJvcGUtY2VudHJhbDItZG9ja2VyLnBrZy5kZXYvZmFzdC1iaS1jb21tb24vYmktcGxhdGZvcm0iOnsidXNlcm5hbWUiOiJfanNvbl9rZXkiLCJwYXNzd29yZCI6IntcbiAgXCJ0eXBlXCI6IFwic2VydmljZV9hY2NvdW50XCIsXG4gIFwicHJvamVjdF9pZFwiOiBcImZhc3QtYmktY29tbW9uXCIsXG4gIFwicHJpdmF0ZV9rZXlfaWRcIjogXCI5N2M5ZGJkZjBlMmExMzFjMWFlYjVkYzMyMzcxZmJlMTZhYWM2OTlkXCIsXG4gIFwicHJpdmF0ZV9rZXlcIjogXCItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cXG5NSUlFdmdJQkFEQU5CZ2txaGtpRzl3MEJBUUVGQUFTQ0JLZ3dnZ1NrQWdFQUFvSUJBUURoTjBnbGowSDBuUEU4XFxua1hRVWxlMFIzSmZSNS9YU01Bck1zLzI3SFB6WEt2dCszTU83OE8zMmVKNmRiZUtXS3pNSVB4dXE5Nk1iTm51eFxcbmF0aFd5TUJHRjVkUjlnc05EOEpNc3Z6di9hU3FPRmhGellabXVwd3ViTFJhNzRwTUdUZ0FYNS9Bd1FNb1FzcFFcXG5PR1lVRERxZUhXMzUyOGQrM3pORFIxRTNZOFhweGZHWmVuUTlrSGpwUkpMbEdJRTdaOERiS01RMG5iYlpDYmphXFxuODQ1cVpRODY3cHkxLzFNUTBEQVFBOWRzbnp1MFVGdFVSdjlKOHBCbllsUzQrUTVLYkNmaktUVU5vbU1lN0VxU1xcbnhTT0VxMmJTS3NWbnEreXNNenZBc0h3Z1BuWXdXSTBxbHhyVzJNM0I2V0pZR2M4QWhacFAyeHMrL1VjMTJCZnVcXG5CRWEwUDk5M0FnTUJBQUVDZ2dFQUJXREprMFFKZ2lyaFE5NEJWS0NENmNKSHlkWnRNMWk4V1VrUERSSFRJOXc5XFxuNXppa1grK1A3eXZRTmRneTZmMGNHZDFQN3RROUMrQlFNZHRvVVNJNEhUUmhBNjhKY29WbTBXZ1F6RFpWWVU4NVxcbkpHbVZCUXo5djhvT3dJRXAzMGxXNGdObWtEYkx1WUZteldXYlltMHNWU3hFdlF1M2h0c1RvVTZwck5Yd0d2WDRcXG5wYVJ2WkFYaHV1aFFRMzJYVHp3ZnAwZXJSbUNNQ2ducVA5SDZVRHVIWkVKd3M1TEQvWDJJOEoyYnMydmZ1RDk0XFxueTlMUVh4K3lKY1U0L0FMRmNQWlJrNXd6QmhsYkR0SU9OVTJVS3NCcHhERVcxT2xUcDZab3VoOWpiTS9FS0wvalxcbmJ6R1c3QStyRVpIeXlxaWF5RnlQK0l3L2ZXZml6T2txNUtIbDEwOTRxUUtCZ1FENlg1dGVDY21BTUcrd09LeE9cXG5BWmxZT0pIQ0Zyb1BaaEhyMEZmZTFtNnVDZzVacGVEYStKeHd3QTlIaDVCVTBuWUs3OXMwQTFmL0JZWm9BWnUxXFxudUlESTJ4WmhVRVlZSG1XQzNJUWpMcWxzQTdGQ245SUJhWk9MakM1ZFZEcThnclZDSENDNGUySWJXR0QrZDF4aVxcbmVYU2hidURScmFPWXdFTnlqdGE3Rlp6UmV3S0JnUURtUnZHNnIrTjVmaG1QY2JqSmxTejVxYndNdmVYZ1IwelFcXG5oUmRVUkZOYWhORzF1cWVlZmxwSUNnU0tmZWp0dTR0TWF3Y3BWKzZhVFlQTHpJR1dIbmp4SGx0YUp2aFNLN0ZPXFxuUnpnWFdMR1QrdjNYZzV6NXFLK29SREZpK2ZMaHE2ckpweE82UVlXUHNkVGl0eUJSRFU3QUdXQmFvdlNPYm1Od1xcbjVWaW5FUmN6TlFLQmdHQmpHaDhUeVgwT0tKRkE0Q1NQdlFjWGtCV1dSajdUWHFiWDZGd2pWU3l2Tm1NUTF3VzFcXG5uQlJUL0EzZmhKTURDdXFlK0ZhSHl3S2tqOS9PYnJHQXZDT1hKSUNHTk5Yb0xkQTVNSzJTc3lxZ0tVZXEyMkFyXFxuMHQ5d01oRS84ZlRNcFJjMFdPeHNvNUkvYmVmSTc4b0JWQU1wK21iK1gvTFlZWjdpbW93ZmYrV3RBb0dCQUpMOVxcbjhrd0MvcWVNOE8vNTNjVC9ybWZvQ3h2dkZBL29NNFpmanBiQnpwdFhjUzRaNmVsb0ttVVZ6L1EvanpKQnB4ZXVcXG5zakdYNmIwaGdlSFR0MXlHTXhsbkVWVW96eFpVd2FlQUdyUDhiODVRUnowTXpHVXBZb2Q4a0RIbjd6eFNnb0NhXFxuWUNOaXM4a1g4UmFQWXRlYU1ReTZwaUQxS0RyTENEc0dpMktKckUrSkFvR0JBTlQ1enl5MFRZOEFaSmN6ZFRTY1xcbjB1cEJnby9xUlp1ek44S2FFQnQrNHJ3L3l4UjlWUm1WdGU3ekRiODVkTEk4bFlXM00xdW80ZVFlM0tNY1NsbE1cXG5OcWdLZllIbHMyVjNadFNqNTNyVmQ0dUVrdU9LV1phZTI4clNyRE1GY0xubG5QT1BWdFU5WUVDbjlFM2k5RVloXFxuY1RJY21CWEZNY2xFU2MxVWIrbytDY09yXFxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxcblwiLFxuICBcImNsaWVudF9lbWFpbFwiOiBcImFpcmZsb3ctc2EtcGFja2FnZXMtcmVhZEBmYXN0LWJpLWNvbW1vbi5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbVwiLFxuICBcImNsaWVudF9pZFwiOiBcIjExMjIzMjQyMjUxNjYxODY2MzQ5N1wiLFxuICBcImF1dGhfdXJpXCI6IFwiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGhcIixcbiAgXCJ0b2tlbl91cmlcIjogXCJodHRwczovL29hdXRoMi5nb29nbGVhcGlzLmNvbS90b2tlblwiLFxuICBcImF1dGhfcHJvdmlkZXJfeDUwOV9jZXJ0X3VybFwiOiBcImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0c1wiLFxuICBcImNsaWVudF94NTA5X2NlcnRfdXJsXCI6IFwiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9haXJmbG93LXNhLXBhY2thZ2VzLXJlYWQlNDBmYXN0LWJpLWNvbW1vbi5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbVwiLFxuICBcInVuaXZlcnNlX2RvbWFpblwiOiBcImdvb2dsZWFwaXMuY29tXCJcbn0iLCJlbWFpbCI6ImFpcmZsb3ctc2EtcGFja2FnZXMtcmVhZEBmYXN0LWJpLWNvbW1vbi5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsImF1dGgiOiJYMnB6YjI1ZmEyVjVPbnNLSUNBaWRIbHdaU0k2SUNKelpYSjJhV05sWDJGalkyOTFiblFpTEFvZ0lDSndjbTlxWldOMFgybGtJam9nSW1aaGMzUXRZbWt0WTI5dGJXOXVJaXdLSUNBaWNISnBkbUYwWlY5clpYbGZhV1FpT2lBaU9UZGpPV1JpWkdZd1pUSmhNVE14WXpGaFpXSTFaR016TWpNM01XWmlaVEUyWVdGak5qazVaQ0lzQ2lBZ0luQnlhWFpoZEdWZmEyVjVJam9nSWkwdExTMHRRa1ZIU1U0Z1VGSkpWa0ZVUlNCTFJWa3RMUzB0TFZ4dVRVbEpSWFpuU1VKQlJFRk9RbWRyY1docmFVYzVkekJDUVZGRlJrRkJVME5DUzJkM1oyZFRhMEZuUlVGQmIwbENRVkZFYUU0d1oyeHFNRWd3YmxCRk9GeHVhMWhSVld4bE1GSXpTbVpTTlM5WVUwMUJjazF6THpJM1NGQjZXRXQyZENzelRVODNPRTh6TW1WS05tUmlaVXRYUzNwTlNWQjRkWEU1TmsxaVRtNTFlRnh1WVhSb1YzbE5Ra2RHTldSU09XZHpUa1E0U2sxemRucDJMMkZUY1U5R2FFWjZXVnB0ZFhCM2RXSk1VbUUzTkhCTlIxUm5RVmcxTDBGM1VVMXZVWE53VVZ4dVQwZFpWVVJFY1dWSVZ6TTFNamhrS3pONlRrUlNNVVV6V1RoWWNIaG1SMXBsYmxFNWEwaHFjRkpLVEd4SFNVVTNXamhFWWt0TlVUQnVZbUphUTJKcVlWeHVPRFExY1ZwUk9EWTNjSGt4THpGTlVUQkVRVkZCT1dSemJucDFNRlZHZEZWU2RqbEtPSEJDYmxsc1V6UXJVVFZMWWtObWFrdFVWVTV2YlUxbE4wVnhVMXh1ZUZOUFJYRXlZbE5MYzFadWNTdDVjMDE2ZGtGelNIZG5VRzVaZDFkSk1IRnNlSEpYTWswelFqWlhTbGxIWXpoQmFGcHdVREo0Y3lzdlZXTXhNa0ptZFZ4dVFrVmhNRkE1T1ROQlowMUNRVUZGUTJkblJVRkNWMFJLYXpCUlNtZHBjbWhST1RSQ1ZrdERSRFpqU2toNVpGcDBUVEZwT0ZkVmExQkVVa2hVU1RsM09WeHVOWHBwYTFncksxQTNlWFpSVG1SbmVUWm1NR05IWkRGUU4zUlJPVU1yUWxGTlpIUnZWVk5KTkVoVVVtaEJOamhLWTI5V2JUQlhaMUY2UkZwV1dWVTROVnh1U2tkdFZrSlJlamwyT0c5UGQwbEZjRE13YkZjMFowNXRhMFJpVEhWWlJtMTZWMWRpV1cwd2MxWlRlRVYyVVhVemFIUnpWRzlWTm5CeVRsaDNSM1pZTkZ4dWNHRlNkbHBCV0doMWRXaFJVVE15V0ZSNmQyWndNR1Z5VW0xRFRVTm5ibkZRT1VnMlZVUjFTRnBGU25kek5VeEVMMWd5U1RoS01tSnpNblptZFVRNU5GeHVlVGxNVVZoNEszbEtZMVUwTDBGTVJtTlFXbEpyTlhkNlFtaHNZa1IwU1U5T1ZUSlZTM05DY0hoRVJWY3hUMnhVY0RaYWIzVm9PV3BpVFM5RlMwd3ZhbHh1WW5wSFZ6ZEJLM0pGV2toNWVYRnBZWGxHZVZBclNYY3ZabGRtYVhwUGEzRTFTMGhzTVRBNU5IRlJTMEpuVVVRMldEVjBaVU5qYlVGTlJ5dDNUMHQ0VDF4dVFWcHNXVTlLU0VOR2NtOVFXbWhJY2pCR1ptVXhiVFoxUTJjMVduQmxSR0VyU25oM2QwRTVTR2cxUWxVd2JsbExOemx6TUVFeFppOUNXVnB2UVZwMU1WeHVkVWxFU1RKNFdtaFZSVmxaU0cxWFF6TkpVV3BNY1d4elFUZEdRMjQ1U1VKaFdrOU1ha00xWkZaRWNUaG5jbFpEU0VORE5HVXlTV0pYUjBRclpERjRhVnh1WlZoVGFHSjFSRkp5WVU5WmQwVk9lV3AwWVRkR1ducFNaWGRMUW1kUlJHMVNka2MyY2l0T05XWm9iVkJqWW1wS2JGTjZOWEZpZDAxMlpWaG5VakI2VVZ4dWFGSmtWVkpHVG1Gb1RrY3hkWEZsWldac2NFbERaMU5MWm1WcWRIVTBkRTFoZDJOd1ZpczJZVlJaVUV4NlNVZFhTRzVxZUVoc2RHRktkbWhUU3pkR1QxeHVVbnBuV0ZkTVIxUXJkak5ZWnpWNk5YRkxLMjlTUkVacEsyWk1hSEUyY2twd2VFODJVVmxYVUhOa1ZHbDBlVUpTUkZVM1FVZFhRbUZ2ZGxOUFltMU9kMXh1TlZacGJrVlNZM3BPVVV0Q1owZENha2RvT0ZSNVdEQlBTMHBHUVRSRFUxQjJVV05ZYTBKWFYxSnFOMVJZY1dKWU5rWjNhbFpUZVhaT2JVMVJNWGRYTVZ4dWJrSlNWQzlCTTJab1NrMUVRM1Z4WlN0R1lVaDVkMHRyYWprdlQySnlSMEYyUTA5WVNrbERSMDVPV0c5TVpFRTFUVXN5VTNONWNXZExWV1Z4TWpKQmNseHVNSFE1ZDAxb1JTODRabFJOY0ZKak1GZFBlSE52TlVrdlltVm1TVGM0YjBKV1FVMXdLMjFpSzFndlRGbFpXamRwYlc5M1ptWXJWM1JCYjBkQ1FVcE1PVnh1T0d0M1F5OXhaVTA0VHk4MU0yTlVMM0p0Wm05RGVIWjJSa0V2YjAwMFdtWnFjR0pDZW5CMFdHTlRORm8yWld4dlMyMVZWbm92VVM5cWVrcENjSGhsZFZ4dWMycEhXRFppTUdoblpVaFVkREY1UjAxNGJHNUZWbFZ2ZW5oYVZYZGhaVUZIY2xBNFlqZzFVVko2TUUxNlIxVndXVzlrT0d0RVNHNDNlbmhUWjI5RFlWeHVXVU5PYVhNNGExZzRVbUZRV1hSbFlVMVJlVFp3YVVReFMwUnlURU5FYzBkcE1rdEtja1VyU2tGdlIwSkJUbFExZW5sNU1GUlpPRUZhU21ONlpGUlRZMXh1TUhWd1FtZHZMM0ZTV25WNlRqaExZVVZDZENzMGNuY3ZlWGhTT1ZaU2JWWjBaVGQ2UkdJNE5XUk1TVGhzV1ZjelRURjFielJsVVdVelMwMWpVMnhzVFZ4dVRuRm5TMlpaU0d4ek1sWXpXblJUYWpVemNsWmtOSFZGYTNWUFMxZGFZV1V5T0hKVGNrUk5SbU5NYm14dVVFOVFWblJWT1ZsRlEyNDVSVE5wT1VWWmFGeHVZMVJKWTIxQ1dFWk5ZMnhGVTJNeFZXSXJieXREWTA5eVhHNHRMUzB0TFVWT1JDQlFVa2xXUVZSRklFdEZXUzB0TFMwdFhHNGlMQW9nSUNKamJHbGxiblJmWlcxaGFXd2lPaUFpWVdseVpteHZkeTF6WVMxd1lXTnJZV2RsY3kxeVpXRmtRR1poYzNRdFlta3RZMjl0Ylc5dUxtbGhiUzVuYzJWeWRtbGpaV0ZqWTI5MWJuUXVZMjl0SWl3S0lDQWlZMnhwWlc1MFgybGtJam9nSWpFeE1qSXpNalF5TWpVeE5qWXhPRFkyTXpRNU55SXNDaUFnSW1GMWRHaGZkWEpwSWpvZ0ltaDBkSEJ6T2k4dllXTmpiM1Z1ZEhNdVoyOXZaMnhsTG1OdmJTOXZMMjloZFhSb01pOWhkWFJvSWl3S0lDQWlkRzlyWlc1ZmRYSnBJam9nSW1oMGRIQnpPaTh2YjJGMWRHZ3lMbWR2YjJkc1pXRndhWE11WTI5dEwzUnZhMlZ1SWl3S0lDQWlZWFYwYUY5d2NtOTJhV1JsY2w5NE5UQTVYMk5sY25SZmRYSnNJam9nSW1oMGRIQnpPaTh2ZDNkM0xtZHZiMmRzWldGd2FYTXVZMjl0TDI5aGRYUm9NaTkyTVM5alpYSjBjeUlzQ2lBZ0ltTnNhV1Z1ZEY5NE5UQTVYMk5sY25SZmRYSnNJam9nSW1oMGRIQnpPaTh2ZDNkM0xtZHZiMmRzWldGd2FYTXVZMjl0TDNKdlltOTBMM1l4TDIxbGRHRmtZWFJoTDNnMU1Ea3ZZV2x5Wm14dmR5MXpZUzF3WVdOcllXZGxjeTF5WldGa0pUUXdabUZ6ZEMxaWFTMWpiMjF0YjI0dWFXRnRMbWR6WlhKMmFXTmxZV05qYjNWdWRDNWpiMjBpTEFvZ0lDSjFibWwyWlhKelpWOWtiMjFoYVc0aU9pQWlaMjl2WjJ4bFlYQnBjeTVqYjIwaUNuMD0ifX19"
SEC_CONF = f"""
apiVersion: v1
kind: Secret
metadata:
  name: fast-bi-common-secret
  namespace: default
data:
  .dockerconfigjson: {FAST_BI_COMMON_SA_SECRET}
type: kubernetes.io/dockerconfigjson
"""
DBT_SNAPSHOT = convert_to_lower(airflow_vars.get("DBT_SNAPSHOT"))
DBT_SNAPSHOT_SHARDING = convert_to_lower(airflow_vars.get("DBT_SNAPSHOT_SHARDING"))
DBT_SEED = convert_to_lower(airflow_vars.get("DBT_SEED"))
DBT_SEED_SHARDING = convert_to_lower(airflow_vars.get("DBT_SEED_SHARDING"))
DBT_SOURCE = convert_to_lower(airflow_vars.get("DBT_SOURCE", "True"))
DBT_SOURCE_SHARDING = convert_to_lower(airflow_vars.get("DBT_SOURCE_SHARDING", "True"))
DATA_QUALITY = convert_to_lower(airflow_vars.get("DATA_QUALITY"))
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


DAG_SCHEDULE_INTERVAL = airflow_vars.get("DAG_SCHEDULE_INTERVAL")

xcom_execution_date = '{{ti.xcom_pull(task_ids="show_input_data", key="execution_date")}}'
xcom_full_refresh_model_name = '{{ti.xcom_pull(task_ids="show_input_data", key="full_refresh_model_name")}}'
xcom_full_refresh_model = '{{ti.xcom_pull(task_ids="show_input_data", key="full_refresh_model")}}'
xcom_full_refresh_source = '{{ti.xcom_pull(task_ids="show_input_data", key="full_refresh_source")}}'
xcom_full_refresh_seed = '{{ti.xcom_pull(task_ids="show_input_data", key="full_refresh_seed")}}'
xcom_full_refresh_snapshot = '{{ti.xcom_pull(task_ids="show_input_data", key="full_refresh_snapshot")}}'

# create DbtManifestParser class object
dag_parser = DbtManifestParser(
    manifest_path=MANIFEST_PATH,  # json object that contains data from manifest.json
    dbt_tag=DBT_TAG,
    # class attributes need to create GKEStartPodOperator
    image=IMAGE,
    pod_name=POD_NAME,
    env_vars=env_var,
    airflow_vars=airflow_vars,
    namespace=NAMESPACE,
    project_id=PROJECT_ID,
    cluster_zone=CLUSTER_ZONE,
    cluster_name=CLUSTER_NAME,
    dbt_tag_ancestors=DBT_TAG_ANCESTORS,
    dbt_tag_descendants=DBT_TAG_DESENDANTS
    )

# Validate START_DATE
try:
    START_DATE = dag_parser.get_valid_start_date(DAG_START_DATE)
except ValueError as e:
    raise ValueError(f"Error in START_DATE: {e}")

airbyte_builder = TaskBuilder(
    connection_ids=AIRBYTE_CONNECTION_IDS
)

# Create a DAG Object
with models.DAG(
    DAG_ID,  # dag id
    schedule_interval=DAG_SCHEDULE_INTERVAL,  # override to match your needs
    start_date=START_DATE,  # start date, parameter, this is the
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
            "execution_date": date.today()
            },
    tags=DAG_TAG,  # a tag name per dag, to allows quick filtering in the DAG view.
) as dag:
    # Creating a private gke cluster
    if airflow_vars.get("SHARED_VPC") == "True":
        CLUSTER_SECONDARY_RANGE_NAME = airflow_vars.get("CLUSTER_SECONDARY_RANGE_NAME")
        SERVICES_SECONDARY_RANGE_NAME = airflow_vars.get("SERVICES_SECONDARY_RANGE_NAME")
        CLUSTER = {
            "name": CLUSTER_NAME,  # the name of the Google Kubernetes Engine cluster
            "network": NETWORK,  # define a network
            "subnetwork": SUBNETWORK,
            "ipAllocationPolicy": {
                "use_ip_aliases": True,
                "cluster_secondary_range_name": CLUSTER_SECONDARY_RANGE_NAME,
                "services_secondary_range_name": SERVICES_SECONDARY_RANGE_NAME,
            },
            "node_pools": [
                {
                    "name": "dbt",
                    "initial_node_count": CLUSTER_NODE_COUNT,
                    "config": {
                        "serviceAccount": CLUSTER_NODE_SA,
                        "machine_type": CLUSTER_MACHINE_TYPE,
                        "disk_type": CLUSTER_MACHINE_DISK_TYPE,
                        "oauth_scopes": [
                            "https://www.googleapis.com/auth/cloud-platform"
                        ],
                    },
                }
            ],
            "privateClusterConfig": PRIVATENODES,
            "masterAuthorizedNetworksConfig": MASTERAUTHORIZEDNETWORKSCONFIG,
        }
    elif airflow_vars.get("SHARED_VPC") == "False":
        CLUSTER = {
            "name": CLUSTER_NAME,  # the name of the Google Kubernetes Engine cluster
            "network": NETWORK,  # define a network
            "subnetwork": SUBNETWORK,
            "node_pools": [
                {
                    "name": "dbt",
                    "initial_node_count": CLUSTER_NODE_COUNT,
                    "config": {
                        "serviceAccount": CLUSTER_NODE_SA,
                        "machine_type": CLUSTER_MACHINE_TYPE,
                        "disk_type": CLUSTER_MACHINE_DISK_TYPE,
                        "oauth_scopes": [
                            "https://www.googleapis.com/auth/cloud-platform"
                        ],
                    },
                }
            ],
            "privateClusterConfig": PRIVATENODES,
            "masterAuthorizedNetworksConfig": MASTERAUTHORIZEDNETWORKSCONFIG,
        }


    def check_all_tasks_status(context):
        dag_run = context['dag_run']
        task_instances = dag_run.get_task_instances()

        for task_instance in task_instances:
            if task_instance.state == 'failed':
                dag_run.set_state(State.FAILED)
                raise AirflowFailException("DAG completed successfully, but one or more tasks have failed.")


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
            full_refresh = "-f" if dag_run.conf["full_refresh"] else ""
            log.info(f"full_refresh:  {dag_run.conf['full_refresh']}")
            kwargs["ti"].xcom_push(key="full_refresh_model", value="run " + full_refresh if full_refresh else "run")
            kwargs["ti"].xcom_push(key="full_refresh_source", value="source freshness")
            kwargs["ti"].xcom_push(key="full_refresh_seed", value="seed " + full_refresh if full_refresh else "seed")
            kwargs["ti"].xcom_push(key="full_refresh_snapshot", value="snapshot")
        else:
            kwargs["ti"].xcom_push(key="full_refresh_model", value="run")
            kwargs["ti"].xcom_push(key="full_refresh_source", value="source freshness")
            kwargs["ti"].xcom_push(key="full_refresh_seed", value="seed")
            kwargs["ti"].xcom_push(key="full_refresh_snapshot", value="snapshot")

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
        provide_context=True)

    # create cluster operator
    create_cluster = GKECreateClusterOperator(
        task_id="create_cluster",  # task name
        project_id=PROJECT_ID,
        location=CLUSTER_ZONE,
        body=CLUSTER)

    # create image pull secret
    create_secret_task = GKECreateCustomResourceOperator(
        task_id="create_secret_task",
        use_internal_ip=True,
        project_id=PROJECT_ID,
        location=CLUSTER_ZONE,
        cluster_name=CLUSTER_NAME,
        yaml_conf=SEC_CONF,
    )

    if AIRBYTE_CONNECTION_IDS and AIRBYTE_REPLICATION_FLAG:
        airbyte_group = airbyte_builder.build_tasks(
            connection_ids=AIRBYTE_CONNECTION_IDS
        )
        airbyte_group >> show_input_data

    task_list = []
    temp_var = Variable.get(f"temp_var_{DAG_ID}", deserialize_json=True, default_var=None)
    full_refresh_model_name_list = temp_var.get("full_refresh_model_name") if temp_var else []
    if temp_var:
        DBT_SEED = "false" if full_refresh_model_name_list and DBT_SEED_SHARDING == "false" else DBT_SEED
        DBT_SNAPSHOT = "false" if full_refresh_model_name_list and DBT_SNAPSHOT_SHARDING == "false" else DBT_SNAPSHOT
        DBT_SOURCE = "false" if full_refresh_model_name_list and DBT_SOURCE_SHARDING == "false" else DBT_SOURCE

    """ run method create_dbt_task that run command dbt seed
            seed: dbt command -> dbt seed
    """
    if dag_parser.is_resource_type_in_manifest("seed"):
        if DBT_SEED == "true":
            if DBT_SEED_SHARDING == "true":
                dbt_seed_files = dag_parser.create_dbt_task_groups(
                    group_name="seeds",
                    resource_type="seed",
                    dbt_command="seed",
                    running_rule=TriggerRule.ALL_SUCCESS,
                    task_params={"full_refresh": xcom_full_refresh_seed,
                                 "full_refresh_model_name": full_refresh_model_name_list})
                task_list.append(dbt_seed_files)
            else:
                dbt_seed_all_files = dag_parser.create_dbt_task(
                    dbt_command="seed",
                    running_rule=TriggerRule.ALL_SUCCESS,
                    task_params={"full_refresh": xcom_full_refresh_seed,
                                 "full_refresh_model_name": full_refresh_model_name_list})
                task_list.append(dbt_seed_all_files)
        else:
            log.info("dbt seed not enabled.")

    if dag_parser.is_resource_type_in_manifest("source"):
        if DBT_SOURCE == "true":
            if DBT_SOURCE_SHARDING == "true":
                dbt_sources_models = dag_parser.create_dbt_task_groups(
                    group_name="sources",
                    resource_type="source",
                    dbt_command="source freshness",
                    running_rule=TriggerRule.ALL_SUCCESS,
                    task_params={"full_refresh": xcom_full_refresh_source,
                                 "full_refresh_model_name": full_refresh_model_name_list,
                                 "DBT_VAR": "'execution_date': '" + xcom_execution_date + "'"})
                task_list.append(dbt_sources_models)
            else:
                dbt_source_all_files = dag_parser.create_dbt_task(
                    dbt_command="source freshness",
                    running_rule=TriggerRule.ALL_SUCCESS,
                    task_params={"full_refresh": xcom_full_refresh_source,
                                 "full_refresh_model_name": full_refresh_model_name_list,
                                 "DBT_VAR": "'execution_date': '" + xcom_execution_date + "'"})
                task_list.append(dbt_source_all_files)

    """ run method create_dbt_task_groups that create Airflow task from DBT models
            models: name of Task Groups uses for DAG’s graph view in the Airflow UI
            from the manifest nodes will be selected
            run: dbt command -> dbt run
    """
    if dag_parser.is_resource_type_in_manifest("model"):
        dbt_run_models = dag_parser.create_dbt_task_groups(
                    group_name="models",
                    resource_type="model",
                    dbt_command="run",
                    running_rule=TriggerRule.ALL_SUCCESS,
                    task_params={"full_refresh": xcom_full_refresh_model,
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
                    dbt_command="snapshot",
                    running_rule=TriggerRule.ALL_SUCCESS,
                    task_params={"full_refresh": xcom_full_refresh_snapshot,
                                 "full_refresh_model_name": full_refresh_model_name_list}
                    # , Variable.get(PROJECT_VARS_LIST, deserialize_json=True)
                )
                task_list.append(dbt_snapshot_models)
            else:
                dbt_snapshot_all_models = dag_parser.create_dbt_task(
                    dbt_command="snapshot",
                    running_rule=TriggerRule.ALL_SUCCESS,
                    task_params={"full_refresh": xcom_full_refresh_snapshot,
                                 "full_refresh_model_name": full_refresh_model_name_list}
                    # , Variable.get(PROJECT_VARS_LIST, deserialize_json=True)
                )
                task_list.append(dbt_snapshot_all_models)
        else:
            log.info("dbt snapshot not enabled.")

    # Delete cluster operator
    delete_cluster = GKEDeleteClusterOperator(
        task_id="delete_cluster",  # task name
        name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        location=CLUSTER_ZONE,
        on_success_callback=check_all_tasks_status,
        trigger_rule=TriggerRule.NONE_SKIPPED  # the conditions that Airflow applies to task
        # to determine whether they are ready to execute
        # All upstream tasks have not failed or upstream_
        # failed - that is, all upstream tasks have succeeded or been skipped
    )

    if DATA_QUALITY == "true":
        re_data_models = dag_parser.create_dbt_task(dbt_command="re_data",
                                                                  node_name="re_data_quality_checks",
                                                                  node_alias="re_data_quality_checks",
                                                                  running_rule=TriggerRule.ALL_DONE)
        task_list.append(re_data_models)

    task_list = list(filter(None, task_list))

    if Variable.get(f"temp_var_{DAG_ID}", None):
        cleanup_temp_airflow_vars = PythonOperator(
            task_id="cleanup_temp_airflow_vars",
            python_callable=f_cleanup_temp_airflow_vars,
            provide_context=True,
            op_kwargs={"temp_airflow_vars": f"temp_var_{DAG_ID}"},
            trigger_rule=TriggerRule.ALL_DONE)
        if task_list:
            task_list[-1] >> cleanup_temp_airflow_vars
        else:
            show_input_data >> cleanup_temp_airflow_vars

# SET DEPENDENCIES
    create_cluster >> create_secret_task >> show_input_data

    if task_list:
        for task_group in task_list:
            try:
                task_group.group_id
            except:
                if task_list.index(task_group) == 0:
                    show_input_data >> task_group
                if task_list.index(task_group) > 0:
                    task_list[task_list.index(task_group) - 1] >> task_group
                if task_list.index(task_group) + 1 < len(task_list):
                    task_group >> task_list[task_list.index(task_group) + 1]
            else:
                for task_id in task_group:
                    if not task_id.upstream_task_ids:
                        try:
                            dbt_seed_all_files >> task_id
                        except:
                            show_input_data >> task_id

        if not delete_cluster.upstream_task_ids:
            task_list[-1] >> delete_cluster
    else:
        show_input_data >> delete_cluster

last_task = dag.tasks[-1]
last_task.on_success_callback = check_all_tasks_status
