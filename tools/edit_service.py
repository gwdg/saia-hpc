#!/usr/bin/env python3

import os
import json
import sys
import time
import logging
from scheduler import ServiceList, service_dir, cluster_file, acquire_lock, release_lock

# Retry configuration
MAX_RETRIES = 5
RETRY_DELAY = 5

def edit_service(service_id, updated_info):
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

        # Find the service by its ID and update the fields
        service_found = False
        for service in service_list.services:
            if service.id == service_id:
                service_found = True
                # Update the service's attributes with the values from updated_info
                for key, value in updated_info.items():
                    if hasattr(service, key):
                        setattr(service, key, value)
                        logging.info(f"Updated {key} for service {service_id} to {value}")
                    else:
                        logging.warning(f"{key} is not a valid attribute for service {service_id}")
                break

        if not service_found:
            logging.error(f"Service with ID {service_id} not found.")
            print(f"Service with ID {service_id} not found.")
            return

        # Save the updated service list back to the cluster file
        with open(os.path.join(service_dir, cluster_file), 'w') as f:
            f.write(service_list.to_json())

        print(f"Successfully updated service {service_id}.")

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        print(f"Error occurred: {str(e)}")
    
    finally:
        # Ensure the lock is released after the operation
        if lock_acquired:
            release_lock()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python edit_service.py <service_id> <attribute1=value1> <attribute2=value2> ...")
        sys.exit(1)

    service_id = sys.argv[1]
    updated_info = {}

    # Parse the command-line arguments for attributes to update
    for arg in sys.argv[2:]:
        if '=' in arg:
            key, value = arg.split('=')
            try:
                # Attempt to convert value to a number if possible, otherwise leave as string
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass
            updated_info[key] = value

    edit_service(service_id, updated_info)
