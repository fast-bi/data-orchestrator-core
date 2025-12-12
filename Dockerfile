##
#  Generic dockerfile for dbt image building.
#  See README for operational details
##

# Top level build args
ARG build_for=linux/amd64

##
# base image (abstract)
FROM --platform=$build_for python:3.11.14-slim-bookworm as base
LABEL maintainer=support@fast.bi

# System setup and dependencies installation
RUN apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
        git \
        ssh-client \
        software-properties-common \
        make \
        build-essential \
        ca-certificates \
        libpq-dev \
        curl \
        apt-transport-https \
        gnupg \
        cl-base64 \
        jq \
        uuid-runtime \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update -y \
    && apt-get install -y google-cloud-cli \
    # Install kubectl
    && curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
    && chmod +x kubectl \
    && mv kubectl /usr/local/bin/ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Python environment setup
ENV PYTHONIOENCODING=utf-8 \
    LANG=C.UTF-8 \
    source_k8s_file="/usr/app/tsb-data-orchestrator-core/template_dbt_project_dag_k8s.py" \
    source_gke_file="/usr/app/tsb-data-orchestrator-core/template_dbt_project_dag_gke.py" \
    source_api_file="/usr/app/tsb-data-orchestrator-core/template_dbt_project_dag_api_server.py" \
    source_bash_file="/usr/app/tsb-data-orchestrator-core/template_dbt_project_dag_bash.py"

# Set docker basics
WORKDIR /usr/app/tsb-data-orchestrator-core/
LABEL maintainer=TeraSky(c)

# Copy requirements first to leverage cache for pip install
COPY ./requirements.txt /usr/app/tsb-data-orchestrator-core/

# Update python and install packages
RUN python -m pip install --upgrade pip setuptools wheel yq --no-cache-dir \
    && python -m pip install -r /usr/app/tsb-data-orchestrator-core/requirements.txt

# Copy scripts and templates
COPY ./api-entrypoint.sh /usr/app/dbt/
RUN chmod 755 /usr/app/dbt/api-entrypoint.sh

# Copy application code
COPY ./create_dag_from_template.py \
     ./airflow_reserialize_dag.py \
     ./create_variables_airflow_bauth.op.py \
     ./delete_variables_airflow_bauth.op.py \
     ./delete_dag_airflow_bauth.op.py \
     ./template_dbt_project_dag_gke.py \
     ./template_dbt_project_dag_k8s.py \
     ./template_dbt_project_dag_api_server.py \
     ./template_dbt_project_dag_bash.py \
     ./get_dag_status_airflow_bauth.op.py \
     ./run_dag_airflow_bauth.op.py \
     ./deploy_dbt_api_service.sh \
     ./delete_dbt_api_service.sh \
     /usr/app/tsb-data-orchestrator-core/

# Copy main directory
COPY ./main/dbt_manifest_parser_gke_operator.py \
     ./main/dbt_manifest_parser_k8s_operator.py \
     /usr/app/tsb-data-orchestrator-core/main/

# Set permissions for scripts
RUN chmod 755 /usr/app/tsb-data-orchestrator-core/deploy_dbt_api_service.sh \
    && chmod 755 /usr/app/tsb-data-orchestrator-core/delete_dbt_api_service.sh

ENTRYPOINT ["/bin/bash", "-c", "/usr/app/dbt/api-entrypoint.sh"]
