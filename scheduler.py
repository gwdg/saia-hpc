#!/usr/bin/env python3

#########################################################################################
## To initialize your cluster setup based on the config file, run:                     ##
##     python scheduler.py init                                                        ##
#########################################################################################

import datetime
import os
import subprocess
import sys
import logging
import json
import time
from datetime import timedelta, datetime
import random
import http.client
from scheduler_core.services import Service, ServiceList, ServiceJob, service_dir, cluster_file, TIME_FORMAT, squeue_time_to_timedelta

##############################################################################
## Configuration                                                            ##
##############################################################################

# Get the current month and year
current_month = datetime.now().strftime("%Y-%m")

# Create the log filepath with the current month
log_filepath = os.path.expanduser(f"./log/scheduler-{current_month}.log")
config_file = "config.json"
squeue_path = '/usr/local/slurm/current/install/bin/squeue'
sbatch_path = '/usr/local/slurm/current/install/bin/sbatch'
scancel_path = '/usr/local/slurm/current/install/bin/scancel'
EXPIRE_LIMIT = "9:30"
FILE_LOCK = ".scheduler.lock"
MIN_PORT = 61001
MAX_PORT = 62000
ROUTINE_INTERVAL = 5     # Period of check_routine
ROUTINE_WINDOW = 60      # For moving average
LAST_INTERVAL_WEIGHT = 2 # Multiplier for number of active inferences in recent interval


def get_squeue_status():
    squeue_output = subprocess.run(
        [squeue_path, '--me', '-h', '--name=service-backend',
         '--format="{\"JOBID\": \"%.18i\", \"STATE\": \"%.2t\", \"TIME\": \"%.10M\", \"TIME_LIMIT\": \"%.9l\", \"NODELIST\": \"%N\"}"'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.decode('utf-8')
    lines = squeue_output.split("\n")
    lines = [" ".join(line.split()).strip("\"") for line in lines]
    return lines


def generate_random_port_number(excluded: set) -> int:
    # The port number should be unique across all current instances,
    # to avoid clashes if two jobs end up on the same node
    while True:
        random_number = random.randint(MIN_PORT, MAX_PORT)
        if random_number not in excluded:
            return random_number


def test_readiness(host, port):
    # Check if a backend is reachable
    # Consider implementing this as async to run readiness probes concurrently
    try:
        connection = http.client.HTTPConnection(host=host, port=port, timeout=3)
        connection.request("GET", "/")
        response = connection.getresponse()
        if 200 <= response.status < 500:
            return True
    except Exception as e:
        logging.debug(f'Tested connection for {host}:{port} and received {str(e)}')
    return False


def restart_job(jobid):
    # Restart a job by requeueing it
    jobid = str(jobid)
    result = subprocess.run(['scontrol', 'requeue', jobid], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Job {jobid} requeued successfully.")
        logging.info(f"Job {jobid} requeued successfully.")
    else:
        print(f"Failed to requeue job{jobid}.")
        logging.error(f"Failed to requeue job{jobid}.")
        print(f"Error: {result.stderr}")
        logging.error(f"Error: {result.stderr}")


def cancel_job(jobid):
    """Cancel a job using its job ID and remove it from the list."""
    jobid = str(jobid)
    # Use subprocess to cancel the job via slurm command
    result = subprocess.run([scancel_path, jobid], capture_output=True, text=True)
    if result.returncode == 0:
        logging.info(f"Job {jobid} canceled successfully.")
    else:
        logging.error(f"Failed to cancel job {jobid}. Error: {result.stderr}")


def check_service_job_status(host, port):
    command = ['curl', '-i', f'{host}:{port}/health', '--max-time', '20']
    result = subprocess.run(command, capture_output=True, text=True)
    if "HTTP/1.1 200 OK" in result.stdout or "HTTP/1.1 404 Not Found" in result.stdout:
        return True
    else:
        logging.info("Found unhealthy job: " + str(host) + ":" + str(port))
        return False


def start_service_job(service_id, max_retries, retry_delay):
    """
    service_id: str
    Start a new service job for a given service id.
    """
    logging.info(f"Starting on-demand service job for {service_id}")
    scale_to_zero = True
    max_retries = 5
    retry_delay = 2  # seconds
    for attempt in range(max_retries):
        success = check_routine(service_id, scale_to_zero)
        if success:
            logging.info(f"Successfully started service job for {service_id} on attempt {attempt + 1}")
            return
        else:
            if attempt < max_retries - 1:
                logging.warning(f"Attempt {attempt + 1} to start service job for {service_id} failed due to lock. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
    logging.error(f"Failed to start service job for {service_id} after {max_retries} attempts due to scheduler lock.")


def is_running(pid):
    # Check if a given process id is running
    if os.path.isdir(f"/proc/{pid}"):
        return True
    return False


def acquire_lock():
    # Set a lock file with the current process id, to serve as a mutex across multiple processes
    try:
        # Create the file (O_CREAT), fail if it exists (O_EXCL), open in read write mode (O_RDWR)
        fd = os.open(FILE_LOCK, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        os.write(fd, str.encode(str(os.getpid())))
        os.close(fd)
        logging.debug('Set lock for routine scheduling check.')
        return True
    except OSError:
        #with open(FILE_LOCK) as f:
        #    line = f.readline().strip()
        #    if len(line) == 0:
        #        return False
        #    pid = int(f.readline().strip())
        #if is_running(pid):
        logging.warning('Previous routine check is still running.')
        return False
        #logging.info('Previous routine check is not running, resetting lock.')
        #os.remove(FILE_LOCK)
        #return acquire_lock()


def release_lock():
    # Attempt to release the lock
    try:
        os.remove(FILE_LOCK)
        logging.debug('Released lock.')
        return True
    except OSError:
        logging.error('Could not release lock.')
        return False


def main():
    logging.basicConfig(filename=log_filepath, level=logging.INFO,
                        format='%(asctime)s.%(msecs)d %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    #logging.info(str(sys.argv[1:]))
    if len(sys.argv) < 2:
        ## ERROR: No arguments given
        return
    if sys.argv[1] == 'check_routine':
        return check_routine()
    if sys.argv[1] == 'health_check':
        return health_check()
    if sys.argv[1] == 'init':
        return init()
    if sys.argv[1] == 'start_service_job' and len(sys.argv) >= 3:
        service_id = sys.argv[2]
        return start_service_job(service_id, max_retries=3, retry_delay=4)
    if len(sys.argv) < 5:
        ## ERROR: Invalid arguments
        return
    try:
        command, inference_id, user, app = sys.argv[1:5]
        if command == 'begin_inference':
            return begin_inference(inference_id, user, app)
        elif command == 'end_inference':
            return end_inference(inference_id, user, app)
    except Exception as e:
        print(str(e))


def check_routine(service_id="", scale_to_zero=False):
    """Routine called every 60 seconds or so. Schedules the running backend jobs"""
    logging.debug('Checking routine.')
    lock_acquired = False
    try:
        # Create a mutex via a file signaling that check_routine is already running
        if not acquire_lock():
            return False
        lock_acquired = True
        # Read cluster.json and create Service and ServiceJob objects from it
        with open(os.path.join(service_dir, cluster_file), 'r') as f:
            json_data = json.loads(f.read())

        service_list = ServiceList()
        service_list.from_json(json_data)
        logging.debug(f'Loaded service list from json\n{json_data}')

        # Check status of running jobs
        squeue_output = get_squeue_status()
        logging.debug(f"Current running jobs:\n{squeue_output}")

        service_list.update_service_job_from_queue(squeue_output)

        # ServiceJobs that were not found can be removed
        for service in service_list.services:
            service.drop_expired_jobs()
        logging.debug('Finished dropping expired jobs.')

        # Compare number of running jobs to target number
        for service in service_list.services:
            # Calculate number of active service jobs
            active_jobs = service.calculate_active_service_jobs()

            # Update the average number of inferences
            active_inferences = service.get_active_inferences()
            logging.debug(f'Has {active_inferences} active inferences.')

            ## If 1 backend -> 10 open inferences at each given time

            intervals_in_window = ROUTINE_WINDOW / ROUTINE_INTERVAL # 12
            remain_window_weight = intervals_in_window - LAST_INTERVAL_WEIGHT  # 12 - 2 = 10
            weighted_sum = service.average_inferences * remain_window_weight + \
                LAST_INTERVAL_WEIGHT * active_inferences                       # weighed_sum = avg_inf * 10 + 2 * active_inf
            service.average_inferences = weighted_sum / intervals_in_window    # weighted_sum / 12
            # Round
            service.average_inferences = round(service.average_inferences, 4)
            logging.debug(f'Has {service.average_inferences} average inferences.')

            # Calculate target number of instances based on average based on: average / inferences_per_instance
            target_number_instances = int((service.average_inferences + service.inferences_per_instance - 0.001) // service.inferences_per_instance)
            ##target_number_instances = service.average_inferences / service.inferences_per_instance
            target_number_instances = max(target_number_instances, service.minimum_number_instances)
            target_number_instances = min(target_number_instances, service.maximum_number_instances)
            service.target_number_instances = int(target_number_instances)

            # Start new jobs if necessary
            number_required_jobs = service.target_number_instances - active_jobs
            service.number_required_jobs = number_required_jobs
            logging.debug(f'Has {active_jobs} active jobs. Target is {service.target_number_instances}')
            if number_required_jobs > 0:
                logging.info(f'Determined that service {service.id} requires {str(number_required_jobs)} additional jobs.')
            elif scale_to_zero and service.id == service_id and service.minimum_number_instances == 0 and active_jobs == 0:
                service.number_required_jobs = 1
                logging.info(f'Changed number of required jobs for service {service.id} to 1.')

        # Start new jobs equal to the difference and record the job ids
        occupied_ports = service_list.get_all_ports()
        logging.debug(f'The following ports are already occupied: {str(list(occupied_ports))}')
        new_jobs = []
        for service in service_list.services:
            if service.maximum_number_instances == 0:
                logging.info(f"Service {service.id} is set to zero maximum instances. Skipping automatic job creation.")
                continue
            for _ in range(service.number_required_jobs):
                logging.debug("Starting new job")
                port = generate_random_port_number(occupied_ports)
                # Pass the config and port number to use when calling sbatch to backend.sbatch
                batch_output = subprocess.run(
                    [sbatch_path, service.sbatch_path, f"{str(port)}"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.decode('utf-8')
                jobid = int(batch_output.strip())
                logging.info(f'Submitted a new job for {service.id} on port {str(port)}'
                             f' and received jobid {jobid}')
                new_service_job = ServiceJob()
                new_service_job.jobid = jobid
                new_service_job.started_time = datetime.now().strftime(TIME_FORMAT)
                new_service_job.port = port
                new_service_job.ready = False
                new_service_job.job_expiry_time = service.job_expiry_time
                service.service_jobs.append(new_service_job)
                new_jobs.append(new_service_job)

        # Synchronize to cluster.json
        logging.debug('Completed starting new jobs, synchronizing.')
        service_list.save_to_file()

        unready_jobs = []
        # pd_jobs = []
        for service in service_list.services:
            unready_jobs += service.get_unready_jobs()
            # pd_jobs += service.get_pd_jobs()

        if len(unready_jobs) > 0:
            logging.debug(f'Check if {len(unready_jobs)} are ready.\nThis includes'
                         f' {str([job.jobid for job in new_jobs])}')
            ready_jobs = []
            ###time.sleep(3)
            for unready_job in unready_jobs:
                if test_readiness(unready_job.host, unready_job.port):
                    unready_job.ready = True
                    ready_jobs.append(unready_job)
                    logging.info(f'Job {unready_job.jobid} is now ready.')
                    unready_job.job_ready_time = datetime.now().strftime(TIME_FORMAT)

                # Handle jobs that are running but not ready for a long time
                if unready_job.ready == False:
                    job_start_time = datetime.strptime(unready_job.job_start_time, TIME_FORMAT)
                    job_ready_time_limit = squeue_time_to_timedelta(unready_job.job_expiry_time) * 1.5
                    print(job_ready_time_limit)
                    print(job_start_time)
                    if datetime.now() - job_start_time > job_ready_time_limit:
                        logging.warning(f'Job {unready_job.jobid} exceeded ready time limit and will be marked as failed.')
                        cancel_job(unready_job.jobid)
                        unready_job.job_ready_time = datetime.now().strftime(TIME_FORMAT)


            for ready_job in ready_jobs:
                unready_jobs.remove(ready_job)

            squeue_output = get_squeue_status()
            service_list.update_service_job_from_queue(squeue_output)
            logging.debug(f"Current running jobs:\n{squeue_output}")

        # Handle jobs that are running but not ready for a long time
        # if len(pd_jobs) > 0:
        #     # get current time
        #     current_time = datetime.now().strftime(TIME_FORMAT)
        #     for pd_job in pd_jobs:
        #         # convert job_start_time to datetime
        #         job_start_time = datetime.strptime(pd_job.job_start_time, TIME_FORMAT)
        #         job_ready_time = squeue_time_to_timedelta(pd_job.job_expiry_time) * 1.5
        #         # check if job has exceeded ready time limit
        #         if current_time - job_start_time > job_ready_time and service.service_jobs.status == "PD":
        #             logging.warning(f'Job {pd_job.jobid} exceeded ready time limit and will be marked as failed.')
        #             service = match_jobid_to_service_job(pd_job.jobid, service_list)
        #             if service:
        #                 service.service_jobs.remove(pd_job)
        #                 restart_job(pd_job.jobid)
        #             continue

        #     service_list.save_to_file()
        #     logging.debug('Synchronizing changes.')

            # Check if the job is still running, as the backend might have crashed
            ### Commented out
            # failed_jobs = []
            # for unready_job in unready_jobs:
            #     if unready_job.status not in VALID_STATES:
            #         failed_jobs.append(unready_job)
            #         logging.info(f'Job {unready_job.jobid} has failed.')
            # for failed_job in failed_jobs:
            #     unready_jobs.remove(failed_job)
            # for service in service_list.services:
            #     service.drop_expired_jobs()

            # When an API is ready mark it as ready in the cluster.json
            service_list.save_to_file()
            logging.debug('Synchronizing changes.')

        # Remove the mutex file, allowing for another call to check_routine
        return True
    except Exception as e:
        logging.error(e)
    finally:
        if lock_acquired:
            release_lock()
    return False


def health_check():
    """Check the health of all active service jobs and cancel unhealthy jobs."""
    logging.debug('Performing health check on all active service jobs.')

    # Try to acquire the lock to ensure no other instance is running
    lock_acquired = False
    try:
        if not acquire_lock():
            logging.warning("Another health check process is running. Exiting.")
            return
        lock_acquired = True

        # Read the service list from the cluster file
        with open(os.path.join(service_dir, cluster_file), 'r') as f:
            json_data = json.loads(f.read())

        service_list = ServiceList()
        service_list.from_json(json_data)
        squeue_output = get_squeue_status()
        service_list.update_service_job_from_queue(squeue_output)

        # Iterate through all services and their jobs
        for service in service_list.services:
            for service_job in service.service_jobs:
                if service_job.ready and service_job.host and not service_job.is_about_to_expire():
                    # Check the health of the service job
                    logging.debug(f'Checking health for ready job {service_job.jobid} on {service_job.host}:{service_job.port}')
                    if check_service_job_status(service_job.host, service_job.port):
                        logging.debug(f'Job {service_job.jobid} on {service_job.host}:{service_job.port} is healthy.')
                    else:
                        logging.warning(f'Job {service_job.jobid} on {service_job.host}:{service_job.port} is unhealthy. Cancelling.')
                        cancel_job(service_job.jobid)
                

        # Save updated service list back to the file
        service_list.save_to_file()
        logging.debug('Health check completed.')

    except Exception as e:
        logging.error(f"Error during health check: {str(e)}")

    finally:
        if lock_acquired:
            release_lock()


def begin_inference(inference_id, user, app):
    """Add a new inference request"""
    try:
        logging.info('Begin inference - ' + inference_id + ', ' + user + ', ' +  app)
    except Exception as e:
        logging.error(e)
    return


def end_inference(inference_id, user, app):
    """End a current inference request"""
    try:
        logging.info('Ending inference - ' + inference_id + ', ' + user + ', ' +  app)
    except Exception as e:
        logging.error(e)
    return


def init():
    """Builds a base cluster file based on config"""
    # Read cluster.json and create Service and ServiceJob objects from it
    with open(config_file, 'r') as f:
        config = json.loads(f.read())
    new_service_list = ServiceList()
    for service in config["services"]:
        new_service = Service()
        new_service.id = service["id"]
        new_service.name = service["name"]
        new_service.sbatch_path = service["sbatch"]
        new_service.inferences_per_instance = service["inferences_per_instance"]
        new_service.maximum_number_instances = service["maximum_number_instances"]
        new_service.minimum_number_instances = service["minimum_number_instances"]
        new_service.job_expiry_time = service["job_expiry_time"]
        new_service.number_required_jobs = 0
        new_service.target_number_instances = 0
        new_service.owned_by = service["owned_by"]
        new_service_list.services.append(new_service)
        new_service.input = service.get("input", [])
        new_service.output = service.get("output", [])
    json_data = new_service_list.to_json()
    with open(os.path.join(service_dir, cluster_file), "w") as f:
        f.write(json_data)

if __name__ == '__main__':
    main()