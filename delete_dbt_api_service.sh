#!/bin/bash

#Template for the deployment payload
# {
#   "project_name": "$DBT_PROJECT_NAME",
#   "namespace": "dbt-server",
#   "customer": "$CUSTOMER",
#   "domain": "$DOMAIN",
#   "environment": "e2e",
#   "git_branch": "$GIT_BRANCH",
#   "cloud_provider": "",
#   "datawarehouse_type": "$DATA_WAREHOUSE_PLATFORM",
#   "https_enabled": false,
#   "dbt_repo_name": "$DBT_REPO_NAME",
#   "gitlink_secret": "$GITLINK_SECRET",
#   "gitlink_deploy_key": "EMPTY",
#   "secret_dbt_package_repo_token": "$SECRET_DBT_PACKAGE_REPO_TOKEN",
#   "secret_package_repo_token_name": "$SECRET_PACKAGE_REPO_TOKEN_NAME",
#   "basic_auth": "",
#   "basic_auth_user": "",
#   "basic_auth_password": "",
#   "repository": "",
#   "image": "",
#   "tag": "$DBT_API_SERVER_CONTROLLER_IMAGE_TAG",
#   "service_account": "$SERVICE_ACCOUNT",
#   "worker_num": 2,
#   "max_requests": 10,
#   "enable_ddtrace": true,
#   "debug": false,
#   "celery_log_level": "ERROR",
#   "cicd_env_key": "ENV",
#   "cicd_env_value": "prod",
#   "app_version": "1.0.0",
#   "cpu_request": "1500m",
#   "memory_request": "2Gi",
#   "cpu_limit": "3500m",
#   "memory_limit": "6Gi",
#   "storage_size": "1Gi",
#   "data_warehouse_platform": "$DATA_WAREHOUSE_PLATFORM"
# }

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
DELETE_ICON="🗑️"
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

log_delete() {
    echo -e "${MAGENTA}${DELETE_ICON} [DELETE]${NC} $1"
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
    
    # Required variables for deletion
    local required_vars=(
        "DBT_WORKLOAD_API_RELEASE"
        "DBT_API_SERVER_CONTROLLER_SECRET"
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

# Function to validate release name format
validate_release_name() {
    local release_name="$1"
    if [[ ! "$release_name" =~ ^[a-zA-Z0-9][a-zA-Z0-9-]*$ ]]; then
        log_error "Invalid release name format: $release_name"
        return 1
    fi
    return 0
}

# Function to cleanup temporary files
cleanup() {
    local files=("$@")
    for file in "${files[@]}"; do
        if [[ -f "$file" ]]; then
            rm -f "$file"
            log_info "Cleaned up temporary file: $file"
        fi
    done
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

# Function to parse JSON response
parse_json_response() {
    local json_file="$1"
    local output_file="$2"
    
    if [[ ! -f "$json_file" ]]; then
        log_error "JSON response file not found: $json_file"
        return 1
    fi

    log_check "Parsing deletion response..."

    # Validate JSON format
    if ! jq . "$json_file" >/dev/null 2>&1; then
        log_error "Invalid JSON format in response"
        return 1
    fi

    # Extract required fields using jq with proper error handling
    local status=$(jq -r '.status // "error"' "$json_file")
    local message=$(jq -r '.message // "Unknown error"' "$json_file")
    
    # Handle case where service is already deleted
    if [[ "$message" == *"not found"* ]]; then
        log_warning "Service appears to be already deleted"
        status="success"
        message="Service was already deleted"
    fi

    local connection_id=$(jq -r '.details.airflow_connection.id // "N/A"' "$json_file")
    local release_name=$(jq -r '.details.release_name // "N/A"' "$json_file")
    local pvc_deleted=$(jq -r '.details.pvc_deleted[] // "N/A"' "$json_file")

    # Write to output file
    echo "Deletion Status: $status" > "$output_file"
    echo "Message: $message" >> "$output_file"
    echo "Connection ID: $connection_id" >> "$output_file"
    echo "Release Name: $release_name" >> "$output_file"
    if [[ "$pvc_deleted" != "N/A" ]]; then
        echo "Deleted PVCs:" >> "$output_file"
        echo "$pvc_deleted" >> "$output_file"
    fi

    # Check if deletion was successful
    if [[ "$status" != "success" ]]; then
        log_error "Deletion failed: $message"
        return 1
    fi

    log_success "Deletion successful"
    log_info "Connection ID: $connection_id"
    log_info "Release Name: $release_name"
    if [[ "$pvc_deleted" != "N/A" ]]; then
        log_info "Deleted PVCs:"
        echo "$pvc_deleted"
    fi
    return 0
}

# Main execution
main() {
    log_delete "Starting DBT API Service deletion"
    echo -e "\n${CYAN}=== Deletion Configuration ===${NC}"

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

    # Validate release name
    if ! validate_release_name "$DBT_WORKLOAD_API_RELEASE"; then
        exit 1
    fi

    # Define API base URL
    local api_base_url="http://dbt-api-service-controller.dbt-server.svc.cluster.local"
    
    # Check if API service is reachable
    if ! check_api_service "$api_base_url"; then
        log_warning "API service is not reachable, but will attempt deletion anyway"
    fi

    # Delete the service
    log_api "Deleting DBT Project API Server"
    local response_file="$current_dir/${DBT_WORKLOAD_API_RELEASE}_delete_output.json"
    local output_file="$current_dir/${DBT_WORKLOAD_API_RELEASE}_delete_summary.txt"
    local error_log="$current_dir/curl_error.log"
    
    # Add timeout and retry logic
    local max_retries=3
    local retry_count=0
    local success=false

    while [[ $retry_count -lt $max_retries && $success == false ]]; do
        if curl -X DELETE "${api_base_url}/api/v1/deployments/${DBT_WORKLOAD_API_RELEASE}" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $DBT_API_SERVER_CONTROLLER_SECRET" \
            --connect-timeout 10 \
            --max-time 600 \
            -o "$response_file" 2>"$error_log"; then
            success=true
        else
            retry_count=$((retry_count + 1))
            if [[ $retry_count -lt $max_retries ]]; then
                log_warning "Deletion attempt $retry_count failed, retrying..."
                log_warning "Error: $(cat "$error_log")"
                sleep 2
            else
                log_error "Error details: $(cat "$error_log")"
            fi
        fi
    done

    if [[ $success == false ]]; then
        log_error "Failed to send deletion request after $max_retries attempts"
        cleanup "$response_file" "$output_file" "$error_log"
        exit 1
    fi

    # Parse the response
    if ! parse_json_response "$response_file" "$output_file"; then
        log_error "Failed to parse deletion response"
        cleanup "$response_file" "$output_file" "$error_log"
        exit 1
    fi

    # Ensure output file is created and readable
    if [[ ! -f "$output_file" ]]; then
        log_error "Output file was not created: $output_file"
        cleanup "$response_file" "$output_file" "$error_log"
        exit 1
    fi

    echo -e "\n${GREEN}=== Deletion Summary ===${NC}"
    log_success "DBT Project API Server deletion completed successfully"
    log_info "Deletion summary written to: $output_file"

    # Cleanup temporary files
    cleanup "$response_file" "$output_file" "$error_log"
}

# Set up cleanup on script interruption
trap 'log_error "Script interrupted"; exit 1' INT TERM

# Execute main function
main "$@"