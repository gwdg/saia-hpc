#!/usr/bin/env python3

import os
import json
import sys
import time
import logging
from scheduler_core.services import ServiceList, service_dir, cluster_file, release_lock, acquire_lock

# Retry configuration
MAX_RETRIES = 5
RETRY_DELAY = 5


def increase_jobs(service_id, num_jobs):
    lock_acquired = False
    try:
        # Retry mechanism for locked file using acquire_lock from scheduler.py
        for attempt in range(MAX_RETRIES):
            if acquire_lock():
                lock_acquired = True
                break
            else:
                logging.warning(f"Attempt {attempt + 1}: Could not acquire lock, retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
        else:
            logging.error("Max retries exceeded. Unable to acquire lock.")
            print("Max retries exceeded. Unable to acquire lock.")
            return

        # Load the existing service list from the cluster file
        with open(os.path.join(service_dir, cluster_file), 'r') as f:
            json_data = json.loads(f.read())
        
        # Initialize the service list
        service_list = ServiceList()
        service_list.from_json(json_data)

        # Find the service by its ID and increase the number of required jobs
        service_found = False
        for service in service_list.services:
            if service.id == service_id:
                service.number_required_jobs += num_jobs
                service_found = True
                logging.info(f"Increased number_required_jobs for service {service_id} to {num_jobs}")
                break

        if not service_found:
            logging.error(f"Service with ID {service_id} not found.")
            print(f"Service with ID {service_id} not found.")
            return
        
        # Save the updated service list back to the cluster file
        with open(os.path.join(service_dir, cluster_file), 'w') as f:
            f.write(service_list.to_json())
        
        print(f"Successfully updated number_required_jobs for service {service_id} to {num_jobs}")
    
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        print(f"Error occurred: {str(e)}")
    
    finally:
        # Ensure the lock is released after the operation
        if lock_acquired:
            release_lock()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python add_job.py <service_id> <num_jobs>")
        sys.exit(1)
    
    service_id = sys.argv[1]
    num_jobs = int(sys.argv[2])

    increase_jobs(service_id, num_jobs)
