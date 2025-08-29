#!/bin/bash

# Set strict error handling
set -euo pipefail

# ANSI color codes for logging
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Icons for different log levels
INFO_ICON="🔵"
SUCCESS_ICON="✅"
WARNING_ICON="⚠️"
ERROR_ICON="❌"
DEPLOY_ICON="🚀"
CHECK_ICON="🔍"
CONFIG_ICON="⚙️"
FILE_ICON="📄"
API_ICON="🌐"

# Logging functions with modern icons
log_info() {
    echo -e "${BLUE}${INFO_ICON} [INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}${SUCCESS_ICON} [SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}${WARNING_ICON} [WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}${ERROR_ICON} [ERROR]${NC} $1"
}

log_deploy() {
    echo -e "${MAGENTA}${DEPLOY_ICON} [DEPLOY]${NC} $1"
}

log_check() {
    echo -e "${CYAN}${CHECK_ICON} [CHECK]${NC} $1"
}

log_config() {
    echo -e "${BLUE}${CONFIG_ICON} [CONFIG]${NC} $1"
}

log_file() {
    echo -e "${CYAN}${FILE_ICON} [FILE]${NC} $1"
}

log_api() {
    echo -e "${MAGENTA}${API_ICON} [API]${NC} $1"
}

# Function to check dependencies
check_dependencies() {
    log_check "Checking required dependencies..."
    local missing_deps=()
    
    # Check for jq
    if ! command -v jq >/dev/null 2>&1; then
        missing_deps+=("jq")
    fi
    
    # Check for curl
    if ! command -v curl >/dev/null 2>&1; then
        missing_deps+=("curl")
    fi
    
    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        log_error "Missing required dependencies:"
        printf '%s\n' "${missing_deps[@]}"
        return 1
    fi
    
    log_success "All required dependencies are available"
    return 0
}

# Function to check if required variables are set
check_required_vars() {
    local missing_vars=()
    
    log_check "Verifying required environment variables..."
    
    # Required variables for deployment
    local required_vars=(
        "DBT_PROJECT_NAME"
        "CUSTOMER"
        "DOMAIN"
        "PHASE_ENVIRONMENT"
        "DATA_WAREHOUSE_PLATFORM"
        "DBT_REPO_NAME"
        "GITLINK_SECRET"
        "SECRET_DBT_PACKAGE_REPO_TOKEN"
        "SECRET_PACKAGE_REPO_TOKEN_NAME"
        "DBT_API_SERVER_CONTROLLER_IMAGE_TAG"
        "HTTPS_ENABLED"
        "WORKER_NUM"
        "MAX_REQUESTS"
        "CPU_REQUEST"
        "MEMORY_REQUEST"
        "DBT_API_SERVER_CONTROLLER_SECRET"
        "APP_VERSION"
    )

    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            missing_vars+=("$var")
        fi
    done

    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        log_error "Missing required variables:"
        printf '%s\n' "${missing_vars[@]}"
        return 1
    fi
    log_success "All required variables are set"
    return 0
}

# Function to parse JSON response and create env file
parse_json_response() {
    local json_file="$1"
    local output_file="$2"
    local env_file="$3"
    
    if [[ ! -f "$json_file" ]]; then
        log_error "JSON response file not found: $json_file"
        return 1
    fi

    log_check "Parsing deployment response..."

    # Check if file contains valid JSON
    if ! jq empty "$json_file" 2>/dev/null; then
        log_error "Response is not valid JSON. Content of $json_file:"
        cat "$json_file"
        return 1
    fi

    # Create output directory if it doesn't exist
    local output_dir=$(dirname "$output_file")
    mkdir -p "$output_dir"

    local env_dir=$(dirname "$env_file")
    mkdir -p "$env_dir"

    # Extract required fields using jq
    local status
    local message
    local connection_id
    local release_name
    local validation_errors

    status=$(jq -r '.status // "error"' "$json_file")
    message=$(jq -r '.message // "Unknown error"' "$json_file")
    validation_errors=$(jq -r '.details.validation_errors // empty' "$json_file")

    # Check if this is a validation error
    if [[ "$status" == "error" ]]; then
        log_error "Deployment failed: $message"
        if [[ -n "$validation_errors" ]]; then
            log_error "Validation details:"
            echo "$validation_errors" | jq '.'
        else
            log_error "Full response content:"
            cat "$json_file"
        fi
        return 1
    fi

    connection_id=$(jq -r '.details.airflow_connection.id // "unknown"' "$json_file")
    release_name=$(jq -r '.details.release_name // "unknown"' "$json_file")

    # Write to output file
    {
        echo "Deployment Status: $status"
        echo "Message: $message"
        echo "Connection ID: $connection_id"
        echo "Release Name: $release_name"
    } > "$output_file"

    # Create environment file for sourcing
    {
        echo "export DBT_WORKLOAD_API_CONNECTION_ID='$connection_id'"
        echo "export DBT_WORKLOAD_API_RELEASE='$release_name'"
    } > "$env_file"

    # Check if deployment was successful
    if [[ "$status" != "success" ]]; then
        log_error "Deployment failed: $message"
        return 1
    fi

    log_success "Deployment successful"
    log_info "Connection ID: $connection_id"
    log_info "Release Name: $release_name"
    log_file "Environment variables written to: $env_file"
    return 0
}

# Function to cleanup temporary files
cleanup() {
    local json_file="$1"
    
    if [[ -f "$json_file" ]]; then
        log_info "Cleaning up temporary files..."
        rm -f "$json_file"
        log_success "Cleanup completed"
    fi
}

# Function to check if API service is reachable
check_api_service() {
    local api_url="$1"
    
    log_check "Checking if API service is reachable..."
    
    if curl -s --head --connect-timeout 5 "$api_url" >/dev/null; then
        log_success "API service is reachable"
        return 0
    else
        log_error "API service is not reachable: $api_url"
        return 1
    fi
}

# Main execution
main() {
    log_deploy "Starting DBT API Service deployment"
    echo -e "\n${CYAN}=== Deployment Configuration ===${NC}"

    # Get the directory where the script is being called from
    local current_dir="$PWD"
    log_info "Working directory: $current_dir"

    # Check dependencies
    if ! check_dependencies; then
        log_error "Dependency check failed"
        exit 1
    fi

    # Check required variables
    if ! check_required_vars; then
        log_error "Required variables check failed"
        exit 1
    fi

    # Create deployment payload
    log_config "Creating deployment payload"
    
    # Validate boolean values
    if [[ ! "${HTTPS_ENABLED}" =~ ^(true|false)$ ]]; then
        log_error "HTTPS_ENABLED must be 'true' or 'false'"
        exit 1
    fi

    # Validate numeric values
    if ! [[ "${WORKER_NUM}" =~ ^[0-9]+$ ]]; then
        log_error "WORKER_NUM must be a number"
        exit 1
    fi

    if ! [[ "${MAX_REQUESTS}" =~ ^[0-9]+$ ]]; then
        log_error "MAX_REQUESTS must be a number"
        exit 1
    fi

    # Create JSON payload with proper formatting in the current directory
    local json_file="$current_dir/dbt-api-service-controller.json"
    
    # Create JSON with proper escaping and formatting
    cat > "$json_file" << EOF
{
    "project_name": "${DBT_PROJECT_NAME}",
    "namespace": "dbt-server",
    "customer": "${CUSTOMER}",
    "domain": "${DOMAIN}",
    "environment": "${PHASE_ENVIRONMENT}",
    "datawarehouse_type": "${DATA_WAREHOUSE_PLATFORM}",
    "data_warehouse_platform": "${DATA_WAREHOUSE_PLATFORM}",
    "git_branch": "${GIT_BRANCH:-main}",
    "dbt_repo_name": "${DBT_REPO_NAME}",
    "gitlink_secret": "${GITLINK_SECRET}",
    "gitlink_deploy_key": "EMPTY",
    "secret_dbt_package_repo_token": "${SECRET_DBT_PACKAGE_REPO_TOKEN}",
    "secret_package_repo_token_name": "${SECRET_PACKAGE_REPO_TOKEN_NAME}",
    "tag": "${DBT_API_SERVER_CONTROLLER_IMAGE_TAG}",
    "https_enabled": ${HTTPS_ENABLED},
    "worker_num": ${WORKER_NUM},
    "max_requests": ${MAX_REQUESTS},
    "celery_log_level": "WARNING",
    "cpu_request": "${CPU_REQUEST}",
    "memory_request": "${MEMORY_REQUEST}",
    "cpu_limit": "${CPU_LIMIT:-$CPU_REQUEST}",
    "memory_limit": "${MEMORY_LIMIT:-$MEMORY_REQUEST}",
    "app_version": "${APP_VERSION}"
}
EOF

    # Validate JSON format
    if ! jq . "$json_file" >/dev/null 2>&1; then
        log_error "Failed to create valid JSON payload"
        exit 1
    fi

    # Check if JSON file was created
    if [[ ! -f "$json_file" ]]; then
        log_error "Failed to create deployment payload file"
        exit 1
    fi

    log_success "Created deployment payload at: $json_file"

    # Deploy the service
    log_api "Deploying DBT Project API Server"
    local response_file="$current_dir/${DBT_PROJECT_NAME}_${APP_VERSION:-deploy}_output.json"
    local env_file="$current_dir/${DBT_PROJECT_NAME}_${APP_VERSION:-deploy}_env.sh"
    
    # Define the API URL
    local api_url="http://dbt-api-service-controller.dbt-server.svc.cluster.local/api/v1/deployments"
    
    # Check if API service is reachable
    if ! check_api_service "http://dbt-api-service-controller.dbt-server.svc.cluster.local"; then
        log_warning "API service is not reachable, but will attempt deployment anyway"
    fi
    
    # Add timeout and retry logic
    local max_retries=3
    local retry_count=0
    local success=false

    while [[ $retry_count -lt $max_retries && $success == false ]]; do
        if curl -X POST "$api_url" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $DBT_API_SERVER_CONTROLLER_SECRET" \
            -d @"$json_file" \
            --connect-timeout 10 \
            --max-time 600 \
            -o "$response_file" 2>"$current_dir/curl_error.log"; then
            success=true
        else
            retry_count=$((retry_count + 1))
            if [[ $retry_count -lt $max_retries ]]; then
                log_warning "Deployment attempt $retry_count failed, retrying..."
                log_warning "Error: $(cat "$current_dir/curl_error.log")"
                sleep 2
            else
                log_error "Error details: $(cat "$current_dir/curl_error.log")"
                rm -f "$current_dir/curl_error.log"
            fi
        fi
    done

    if [[ $success == false ]]; then
        log_error "Failed to send deployment request after $max_retries attempts"
        cleanup "$json_file"
        exit 1
    fi

    # Parse the response and create env file
    if ! parse_json_response "$response_file" "$response_file" "$env_file"; then
        log_error "Failed to parse deployment response"
        cleanup "$json_file"
        exit 1
    fi

    # Ensure env file is created and readable
    if [[ ! -f "$env_file" ]]; then
        log_error "Environment file was not created: $env_file"
        cleanup "$json_file"
        exit 1
    fi

    # Clean up temporary files
    cleanup "$json_file"
    rm -f "$current_dir/curl_error.log"

    echo -e "\n${GREEN}=== Deployment Summary ===${NC}"
    log_success "DBT Project API Server deployment completed successfully"
    log_info "To use the deployment variables, source the environment file:"
    log_file "source $env_file"
}

# Set trap to handle script interruptions
trap 'log_error "Script interrupted"; exit 1' INT TERM

# Execute main function
main "$@"