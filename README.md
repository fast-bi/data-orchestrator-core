# Data Orchestrator Core

A Python library for running dbt-core workloads in Airflow orchestrator with support for multiple deployment environments including GKE, Kubernetes, API server, and bash operators.

## Overview

This Docker image provides a comprehensive Python library and orchestration framework for executing dbt-core workloads within Airflow environments. It includes DAG template generation, manifest parsing, and operator implementations for various deployment scenarios including Google Kubernetes Engine (GKE), standard Kubernetes, API server interactions, and bash operations. The image is designed to streamline dbt project deployment and execution across different infrastructure platforms.

## Architecture

### Core Components

**DAG Template Generator**: Creates Airflow DAGs from configurable templates supporting multiple operator types (GKE, K8s, API, Bash) with environment-specific configurations.

**Manifest Parser**: Analyzes dbt project manifest.json files to automatically generate task groups and dependencies for Airflow workflows.

**Operator Implementations**: Provides custom operators for different deployment environments including GKEStartPodOperator with enhanced error handling and custom task builders.

**Airflow Integration Manager**: Handles Airflow variable management, secret masking, and environment configuration for secure dbt execution.

**Google Cloud Integration**: Manages GCP authentication, service account activation, and project configuration for cloud-based dbt operations.

## Docker Image

### Base Image
- **Base**: Python 3.11.11-slim-bullseye

### Build

```bash
# Build the image
./build.sh

# Or manually
docker build -t data-orchestrator-core .
```

### Build Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `build_for` | Target platform for the build | `linux/amd64` |

### Environment Variables

The container expects the following environment variables:

- `GCP_SA_SECRET` - Base64 encoded Google Cloud service account secret
- `SA_EMAIL` - Service account email for GCP authentication
- `PROJECT_ID` - Google Cloud project ID
- `source_gke_file` - Path to GKE DAG template file
- `source_k8s_file` - Path to Kubernetes DAG template file
- `source_api_file` - Path to API server DAG template file
- `source_bash_file` - Path to bash DAG template file
- `airflow_secret_file_name` - Path to Airflow variables YAML file
- `source_commit_id` - Git commit ID for versioning
- `cd_cd_job_id` - CI/CD job identifier

### Configuration Files

The image supports configuration through the following files:

- `template_dbt_project_dag_gke.py`: GKE operator DAG template
- `template_dbt_project_dag_k8s.py`: Kubernetes operator DAG template
- `template_dbt_project_dag_api_server.py`: API server operator DAG template
- `template_dbt_project_dag_bash.py`: Bash operator DAG template
- `create_dag_from_template.py`: DAG generation script
- `airflow_reserialize_dag.py`: DAG serialization utility

## Main Functionality

### DAG Generation and Management

The image orchestrates a complete dbt workflow management system:

1. **Template Processing**: Generates DAGs from configurable templates based on operator type
2. **Variable Substitution**: Replaces template placeholders with environment-specific values
3. **Version Management**: Adds version tags and CI/CD job identifiers to generated DAGs
4. **Manifest Analysis**: Parses dbt manifest.json to create task dependencies
5. **Operator Selection**: Supports GKE, Kubernetes, API server, and bash operators

### Airflow Integration

Provides comprehensive Airflow integration capabilities:

- **Variable Management**: Handles Airflow variables with secret masking for sensitive data
- **Connection Management**: Manages Google Cloud connections and service account authentication
- **Task Group Creation**: Automatically generates task groups from dbt manifest analysis
- **Error Handling**: Custom exception handling with shortened error messages for clarity

### Google Cloud Platform Support

Includes full GCP integration features:

- **Service Account Authentication**: Automatic GCP service account activation
- **Project Configuration**: Sets up GCP project and cluster configurations
- **GKE Operations**: Manages Google Kubernetes Engine cluster operations
- **Secret Management**: Handles GCP secrets and service account key files

### Error Handling

- Comprehensive error handling with custom exception classes
- Shortened error messages for improved readability
- Graceful handling of authentication and connection failures
- Detailed logging for troubleshooting deployment issues

### Maintenance Tasks

- **DAG Versioning**: Manages DAG versions with commit IDs and timestamps
- **Template Management**: Maintains and updates DAG templates for different environments
- **Operator Health Monitoring**: Monitors operator performance and error rates
- **Secret Rotation**: Supports service account key rotation and secret updates

## Testing

### Health Checks

The image includes built-in health checks:

```bash
# Check container health
docker inspect --format='{{.State.Health.Status}}' data-orchestrator-core

# View health check logs
docker inspect --format='{{range .State.Health.Log}}{{.Output}}{{end}}' data-orchestrator-core
```

## Troubleshooting

### Common Issues

#### Issue: GCP Authentication Failure
**Problem**: Failed to authenticate with Google Cloud Platform

**Solution**: Verify GCP_SA_SECRET is properly base64 encoded and SA_EMAIL is correct

#### Issue: DAG Template Generation Failure
**Problem**: DAG template processing fails due to missing variables

**Solution**: Ensure all required environment variables and YAML configuration files are present

#### Issue: Manifest Parsing Error
**Problem**: Failed to parse dbt manifest.json file

**Solution**: Verify manifest file exists and is valid JSON format

#### Issue: Airflow Variable Access Failure
**Problem**: Cannot access Airflow variables or secrets

**Solution**: Check Airflow connection configuration and variable permissions

### Getting Help

- **Documentation**: [Fast.BI Documentation](https://wiki.fast.bi)
- **Issues**: [GitHub Issues](https://github.com/fast-bi/data-orchestrator-core/issues)
- **Email**: support@fast.bi

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

```
MIT License

Copyright (c) 2025 Fast.BI

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

This project is part of the FastBI platform infrastructure.

## Support and Maintain by Fast.BI

For support and questions, contact: support@fast.bi
