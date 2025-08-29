#!/bin/bash
#

#Exit on error
set -o errexit

#Error handling
catch() {
    echo 'catching!'
    if [ "$1" != "0" ]; then
    # error handling goes here
    echo "Error $1 occurred on $2"
    exit 1
    fi
}

python --version
git --version
gcloud --version

#Authentificate to Google Platform

if [[ ! -z "${GCP_SA_SECRET}" ]]; then
    echo "Authentificate at GCP."
    echo "Decrypting and saving sa.json file."
    secret_location="/usr/src/secret"
    mkdir -p ${secret_location}
    $(echo "${GCP_SA_SECRET}" | base64 --decode > ${secret_location}/sa.json 2>/dev/null)
    if [[ ! -z "${SA_EMAIL}" ]]; then
        echo 'Authentificate to Google Cloud Platform with SA.'
        gcloud auth activate-service-account ${SA_EMAIL} --key-file ${secret_location}/sa.json
    else
        echo 'Authentificate to Google Cloud Platform with SA.'
        gcloud auth activate-service-account --key-file ${secret_location}/sa.json
    fi
    if [[ ! -z "${PROJECT_ID}" ]]; then
        echo "The GCP Project will be set: ${PROJECT_ID}"
        gcloud config set project ${PROJECT_ID}
        gcloud config set disable_prompts true
    else
        echo "Project Name is not in environment variables ${PROJECT_ID}."
    fi
fi

trap 'catch $? $LINENO' EXIT

exec "$@"