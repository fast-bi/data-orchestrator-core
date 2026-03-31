##
#  Generic dockerfile for dbt image building.
#  See README for operational details
##

# Top level build args
ARG build_for=linux/amd64
# v1.35.0 uses Go 1.25.x which contains the fix for CVE-2025-68121 (stdlib crypto/tls)
ARG KUBECTL_VERSION=v1.35.0

##
# Pull Google Cloud SDK from the official image.
# This avoids the brittle apt-key / packages.cloud.google.com setup entirely.
FROM --platform=$build_for gcr.io/google.com/cloudsdktool/google-cloud-cli:slim AS gcloud-sdk

##
# base image (abstract)
FROM --platform=$build_for python:3.11.14-slim-bookworm AS base
LABEL maintainer=support@fast.bi

# Re-declare build arg so it is accessible in this stage
ARG KUBECTL_VERSION

# Copy Google Cloud SDK from the official image.
# Strip all non-essential Go binaries to eliminate bundled CVEs:
#   - anthoscli          CVE-2026-33186 (grpc) + CVE-2025-68121 (stdlib)
#   - docker-credential-gcloud  CVE-2025-68121 (stdlib) — Docker registry helper, not used here
# gke-gcloud-auth-plugin is retained for kubectl ↔ GKE authentication.
COPY --from=gcloud-sdk /usr/lib/google-cloud-sdk /usr/lib/google-cloud-sdk
RUN ln -sf /usr/lib/google-cloud-sdk/bin/gcloud /usr/local/bin/gcloud \
    && ln -sf /usr/lib/google-cloud-sdk/bin/gsutil /usr/local/bin/gsutil \
    && rm -f /usr/lib/google-cloud-sdk/bin/anthoscli \
    && rm -f /usr/lib/google-cloud-sdk/bin/docker-credential-gcloud

# System setup and dependencies installation
RUN apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
        git \
        ssh-client \
        make \
        build-essential \
        ca-certificates \
        libpq-dev \
        curl \
        gnupg \
        cl-base64 \
        jq \
        uuid-runtime \
    # Install kubectl
    && curl -fsSLo /tmp/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
    && curl -fsSLo /tmp/kubectl.sha256 "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl.sha256" \
    && echo "$(cat /tmp/kubectl.sha256)  /tmp/kubectl" | sha256sum -c - \
    && chmod +x /tmp/kubectl \
    && mv /tmp/kubectl /usr/local/bin/kubectl \
    && rm -f /tmp/kubectl.sha256 \
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
LABEL maintainer=Fast.BI(c)

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
