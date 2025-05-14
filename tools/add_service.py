#!/usr/bin/env python3

import os
import json
import sys
import time
import argparse
import logging
from datetime import datetime
from scheduler_core.services import Service, ServiceList, service_dir, cluster_file 
from scheduler import acquire_lock, release_lock, TIME_FORMAT 


def acquire_lock_with_retry(retry_attempts=5, retry_delay=5):
    """Attempts to acquire a lock with retries if the file is already locked."""
    for attempt in range(retry_attempts):
        if acquire_lock():
            return True
        else:
            print(f"Lock is currently held by another process. Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{retry_attempts})")
            time.sleep(retry_delay)
    print("Failed to acquire the lock after multiple attempts.")
    return False

def add_service(
        service_name, id, sbatch_path, inferences_per_instance=0,
        minimum_instances=0, maximum_instances=0, job_expiry_time="00:30:00", owned_by=None, 
        input_data=None, output_data=None
    ):
    """Adds a new service to the cluster.services file."""
    
    # Retry lock acquisition
    lock_acquired = False
    try:
        if not acquire_lock_with_retry():
            return
        
        lock_acquired = True
        
        # Load the existing service list
        service_list_path = os.path.join(service_dir, cluster_file)
        
        if os.path.exists(service_list_path):
            with open(service_list_path, 'r') as f:
                json_data = json.loads(f.read())
                service_list = ServiceList()
                service_list.from_json(json_data)
        else:
            service_list = ServiceList()
        
        # Check if the service already exists
        for service in service_list.services:
            if service.id == id:
                print(f"Service '{id}' already exists.")
                return
        
        # Create a new Service object
        new_service = Service()
        new_service.name = service_name
        new_service.id = id
        new_service.sbatch_path = sbatch_path
        new_service.inferences_per_instance = inferences_per_instance
        new_service.minimum_number_instances = minimum_instances
        new_service.maximum_number_instances = maximum_instances
        new_service.job_expiry_time = job_expiry_time
        new_service.owned_by = owned_by
        new_service.input = input_data if input_data else []
        new_service.output = output_data if output_data else []
        new_service.target_number_instances = 0
        new_service.number_required_jobs = 0
        new_service.created_time = datetime.now().strftime(TIME_FORMAT)
        new_service.average_inferences = 0.0002
        new_service.service_jobs = []
        # Add the new service to the list
        service_list.services.append(new_service)
        
        # Save the updated service list
        service_list.save_to_file()
        print(f"Service '{id}' added successfully.")
    
    except Exception as e:
        logging.error(f"Error occurred while adding service: {str(e)}")
    
    finally:
        # Release the lock
        if lock_acquired:
            release_lock()

def validate_service_data(service_data):
    """Validate the service data to ensure it matches the expected structure."""
    required_keys = {
        "id": str,
        "name": str,
        "sbatch": str,
        "input": list,
        "output": list,
        "inferences_per_instance": int,
        "maximum_number_instances": int,
        "minimum_number_instances": int,
        "job_expiry_time": str,
    }
    
    for key, expected_type in required_keys.items():
        if key not in service_data:
            logging.error(f"Validation error: Missing key '{key}' in service data.")
            return False
        if not isinstance(service_data[key], expected_type):
            logging.error(f"Validation error: Key '{key}' has invalid type. Expected {expected_type}, got {type(service_data[key])}.")
            return False
    
    return True

def add_services_from_json(json_file_path):
    """Add multiple services from a JSON file."""
    
    # Retry lock acquisition
    lock_acquired = False
    try:
        if not acquire_lock_with_retry():
            return
        
        lock_acquired = True
        
        # Load the service data from the JSON file
        with open(json_file_path, 'r') as f:
            services_data = json.load(f)
        
        # Validate the existence of the "services" key
        if "services" not in services_data or not isinstance(services_data["services"], list):
            logging.error(f"Validation error: The JSON file '{json_file_path}' does not contain a valid 'services' list.")
            print(f"Error: The JSON file '{json_file_path}' does not contain a valid 'services' list.")
            return
        
        # Load the existing service list
        service_list_path = os.path.join(service_dir, cluster_file)
        
        if os.path.exists(service_list_path):
            with open(service_list_path, 'r') as f:
                json_data = json.loads(f.read())
                service_list = ServiceList()
                service_list.from_json(json_data)
        else:
            service_list = ServiceList()
        
        # Iterate through the services from the JSON and add each one
        for service_data in services_data.get("services", []):
            if not validate_service_data(service_data):
                print(f"Validation failed for service: {service_data.get('id', 'Unknown ID')}. Skipping.")
                continue
            
            service_id = service_data.get("id")
            
            # Check if the service already exists
            if any(service.id == service_id for service in service_list.services):
                print(f"Service '{service_id}' already exists, skipping.")
                continue
            
            # Create a new Service object
            new_service = Service()
            new_service.name = service_data.get("name")
            new_service.id = service_id
            new_service.sbatch_path = service_data.get("sbatch")
            new_service.inferences_per_instance = service_data.get("inferences_per_instance", 0)
            new_service.minimum_number_instances = service_data.get("minimum_number_instances", 0)
            new_service.maximum_number_instances = service_data.get("maximum_number_instances", 0)
            new_service.job_expiry_time = service_data.get("job_expiry_time", "00:30:00")
            new_service.owned_by = service_data.get("owned_by", "chat-ai")
            new_service.input = service_data.get("input", [])
            new_service.output = service_data.get("output", [])
            new_service.target_number_instances = service_data.get("target_number_instances", 0)
            new_service.number_required_jobs = service_data.get("number_required_jobs", 0)
            new_service.created_time = datetime.now().strftime(TIME_FORMAT)
            new_service.average_inferences = service_data.get("average_inferences", 0.0002)
            new_service.service_jobs = []

            # Add the new service to the list
            service_list.services.append(new_service)
        
        # Save the updated service list
        service_list.save_to_file()
        print(f"All valid services from '{json_file_path}' added successfully.")
    
    except Exception as e:
        logging.error(f"Error occurred while adding services: {str(e)}")
    
    finally:
        # Release the lock
        if lock_acquired:
            release_lock()


def main():
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="Add a new service or multiple services to the cluster.services file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Define options
    parser.add_argument('--json_file', type=str, help="Path to a JSON file to add multiple services")
    
    # Required service arguments
    parser.add_argument('--service_name', type=str, help="Name of the service (required)")
    parser.add_argument('--sbatch_path', type=str, help="Path to the sbatch file (required)")
    parser.add_argument('--id', type=str, help="ID of the service (required)")
    parser.add_argument('--inferences_per_instance', type=int, help="Number of inferences per instance (required)")
    parser.add_argument('--job_expiry_time', type=str, help="Job expiry time in HH:MM:SS format (required)")
    parser.add_argument('--input', type=str, nargs='+', help="Input data paths or parameters (required, space-separated)")
    parser.add_argument('--output', type=str, nargs='+', help="Output data paths or parameters (required, space-separated)")
    
    # Define optional arguments
    parser.add_argument('--minimum_instances', type=int, default=0, help="Minimum number of instances (default: 0)")
    parser.add_argument('--maximum_instances', type=int, default=0, help="Maximum number of instances (default: 0)")
    parser.add_argument('--owned_by', type=str, default='chat-ai', help="Owner of the service")

    # Parse arguments
    args = parser.parse_args()

    # If a JSON file is provided, add multiple services
    if args.json_file:
        add_services_from_json(args.json_file)
    else:
        # Existing single service addition logic (as you have already implemented)
        if not args.input or not args.output:
            print("Error: Input and output parameters must be specified as space-separated lists.")
            parser.print_help()
            sys.exit(1)
        
        add_service(
            service_name=args.service_name,
            id=args.id,
            sbatch_path=args.sbatch_path,
            inferences_per_instance=args.inferences_per_instance,
            minimum_instances=args.minimum_instances,
            maximum_instances=args.maximum_instances,
            job_expiry_time=args.job_expiry_time,
            owned_by=args.owned_by,
            input_data=args.input,
            output_data=args.output
        )

if __name__ == "__main__":
    main()
