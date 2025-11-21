#!/usr/bin/env python3
"""
Airflow DAG reserialization script
Triggers immediate DAG file processing using airflow CLI
Handles both standalone DAG processor and scheduler deployments
"""
import subprocess
import logging
import sys
import os
import json
import argparse
from typing import Tuple, Optional, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AirflowPodDetector:
    def __init__(self, namespace: str = "data-orchestration", kubeconfig: str = None):
        self.namespace = namespace
        self.kubeconfig = kubeconfig
        
    def _get_kubectl_base_cmd(self) -> List[str]:
        """
        Get base kubectl command with optional kubeconfig
        """
        cmd = ["kubectl"]
        if self.kubeconfig:
            cmd.extend(["--kubeconfig", self.kubeconfig])
        return cmd
        
    def _list_pods(self) -> None:
        """
        List all pods in the namespace for debugging
        """
        try:
            cmd = self._get_kubectl_base_cmd() + [
                "get", "pods",
                "-n", self.namespace,
                "-o", "wide"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            logger.debug(f"Available pods:\n{result.stdout}")
            
        except Exception as e:
            logger.error(f"Error listing pods: {e}")
        
    def _check_pod_exists(self, component: str) -> Tuple[bool, Optional[str]]:
        """
        Check if a pod with specific component label exists and return its name
        
        Args:
            component: Component label to check for (dag-processor or scheduler)
            
        Returns:
            Tuple of (exists: bool, pod_name: Optional[str])
        """
        try:
            cmd = self._get_kubectl_base_cmd() + [
                "get", "pods",
                "-n", self.namespace,
                "-l", f"component={component}",
                "-o", "jsonpath='{.items[0].metadata.name}'"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            pod_name = result.stdout.strip("'")
            
            if result.returncode == 0 and pod_name and pod_name != "":
                return True, pod_name
            return False, None
            
        except Exception as e:
            logger.error(f"Error checking for {component} pod: {e}")
            return False, None
        
    def get_pod_info(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Detect the appropriate pod and container for DAG processing
        
        Returns:
            Tuple of (pod_name, container_name) or (None, None) if not found
            Special case: Returns ("dag-processor", None) when DAG processor exists
                         to signal that reserialization is not needed
        """
        # First list all pods for debugging
        self._list_pods()
        
        # First check for DAG processor deployment
        exists, pod_name = self._check_pod_exists("dag-processor")
        if exists:
            logger.info(f"Detected standalone DAG processor pod: {pod_name}")
            logger.info("DAG processor handles serialization automatically - skipping manual reserialization")
            # Return special signal that reserialization is not needed
            return "dag-processor", None
        
        # If no DAG processor, check for scheduler
        exists, pod_name = self._check_pod_exists("scheduler")
        if exists:
            logger.info(f"Using scheduler pod for reserialization: {pod_name}")
            return pod_name, "scheduler"
        
        logger.error("No suitable pod found for DAG processing")
        return None, None

def reserialize_dag(pod_name: str, container_name: str, dag_id: str = None, kubeconfig: str = None) -> bool:
    """
    Trigger reserialization of DAGs using airflow CLI in the specified pod
    
    Args:
        pod_name: Name of the pod to execute the command in
        container_name: Name of the container in the pod
        dag_id: Optional specific DAG ID to reserialize
        kubeconfig: Optional path to kubeconfig file for local testing
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Build the kubectl exec command
        cmd = ["kubectl"]
        if kubeconfig:
            cmd.extend(["--kubeconfig", kubeconfig])
            
        cmd.extend([
            "exec",
            "-n", "data-orchestration",
            pod_name,
            "-c", container_name,
            "--", "airflow", "dags", "reserialize"
        ])
        
        if dag_id:
            cmd.extend(["--dag-id", dag_id])
        
        logger.debug(f"Executing command: {' '.join(cmd)}")
        
        # Run the command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with exit code {e.returncode}")
        logger.error(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description='Reserialize Airflow DAGs')
    parser.add_argument('--kubeconfig', help='Path to kubeconfig file for local testing')
    parser.add_argument('--dag-id', help='Specific DAG ID to reserialize')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    return parser.parse_args()

def main():
    """
    Main function for CI/CD integration
    """
    # Parse command line arguments
    args = parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Get DAG ID from args or environment variable
    DAG_ID = args.dag_id or os.getenv("DAG_ID")
    
    logger.info("Starting DAG reserialization check")
    if DAG_ID:
        logger.info(f"Target DAG: {DAG_ID}")
    
    # Detect the appropriate pod and container
    detector = AirflowPodDetector(kubeconfig=args.kubeconfig)
    pod_name, container_name = detector.get_pod_info()
    
    if not pod_name:
        logger.error("❌ Failed to detect appropriate pod for DAG processing")
        sys.exit(1)
    
    # Check if DAG processor is being used (container_name will be None)
    if container_name is None:
        logger.info("✅ Standalone DAG processor detected - no manual reserialization needed!")
        logger.info("The DAG processor will automatically detect and serialize DAG changes")
        sys.exit(0)
    
    # Trigger reserialization (only when using scheduler)
    logger.info("Triggering manual DAG reserialization via scheduler")
    success = reserialize_dag(pod_name, container_name, DAG_ID, args.kubeconfig)
    
    if success:
        logger.info("✅ DAG reserialization completed successfully!")
    else:
        logger.error("❌ DAG reserialization failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 