#!/usr/bin/env python3

import os
import json
import sys
import time
import logging
from scheduler import ServiceList, service_dir, cluster_file, cancel_job, acquire_lock, release_lock

# Retry configuration
MAX_RETRIES = 5
RETRY_DELAY = 5

def remove_service(service_id):
    lock_acquired = False
    try:
        # Retry mechanism for acquiring lock
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

        # Find the service by its ID and remove it
        service_found = False
        for service in service_list.services:
            if service.id == service_id:
                # Cancel all running jobs associated with this service
                for service_job in service.service_jobs:
                    cancel_job(service_job.jobid)
                    logging.info(f"Cancelled job {service_job.jobid} for service {service_id}")
                
                # Remove the service from the list
                service_list.services.remove(service)
                service_found = True
                logging.info(f"Removed service {service_id}")
                break

        if not service_found:
            logging.error(f"Service with ID {service_id} not found.")
            print(f"Service with ID {service_id} not found.")
            return

        # Save the updated service list back to the cluster file
        with open(os.path.join(service_dir, cluster_file), 'w') as f:
            f.write(service_list.to_json())

        print(f"Successfully removed service {service_id} and canceled its running jobs.")

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        print(f"Error occurred: {str(e)}")
    
    finally:
        # Ensure the lock is released after the operation
        if lock_acquired:
            release_lock()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python remove_service.py <service_id>")
        sys.exit(1)

    service_id = sys.argv[1]

    remove_service(service_id)
