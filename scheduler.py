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

##############################################################################
## Configuration                                                            ##
##############################################################################

log_filepath = os.path.expanduser('./log/scheduler.log')
config_file = "config.json"
service_dir = "./services"
cluster_file = "cluster.services"
squeue_path = '/usr/local/slurm/current/install/bin/squeue'
sbatch_path = '/usr/local/slurm/current/install/bin/sbatch'
EXPIRE_LIMIT = "9:30"
FILE_LOCK = ".scheduler.lock"
MIN_PORT = 61001
MAX_PORT = 62000
VALID_STATES = {"R", "PD", "S", "CF"}
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
ROUTINE_INTERVAL = 5     # Period of check_routine
ROUTINE_WINDOW = 60      # For moving average
LAST_INTERVAL_WEIGHT = 2 # Multiplier for number of active inferences in recent interval


def get_squeue_status():
    squeue_output = subprocess.run(
        [squeue_path, '--me', '-h', '--name=service-backend',
         '--format="{\"JOBID\": \"%.18i\", \"STATE\": \"%.2t\", \"TIME\": \"%.10M\", \"TIME_LIMIT\": \"%.9l\", \"BATCHHOST\": \"%B\"}"'],
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


def squeue_time_to_timedelta(time_str):
    if '-' in time_str:
        days_part, time_part = time_str.split('-')
        days = int(days_part)
        # Process the rest as hours:minutes:seconds
        hours, minutes, seconds = map(int, time_part.split(':'))
    else:
        days = 0
        time_parts = time_str.split(':')
        if len(time_parts) == 2:  # MM:SS
            hours = 0
            minutes, seconds = map(int, time_parts)
        elif len(time_parts) == 3:  # HH:MM:SS
            hours, minutes, seconds = map(int, time_parts)
        else:
            raise ValueError("Invalid time format: {time_str}")
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


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


class ServiceJob:
    def __init__(self):
        self.jobid = None
        self.host = None
        self.status = None
        self.started_time = None # When it was submitted
        self.job_start_time = None # timestamp taken by scheduler
        self.job_ready_time = None
        self.job_expiry_time = None
        self.time = None # According to Slurm
        self.time_limit = None
        self.port = None
        self.ready = None

    def from_json(self, data):
        self.jobid = data["jobid"]
        # self.host = data["host"]
        # self.status = data["status"]
        self.started_time = data["started_time"]
        # self.time = data["time"]
        # self.time_limit = data["time_limit"]
        self.port = data["port"]
        self.ready = data["ready"]
        self.job_start_time = data["job_start_time"]
        self.job_ready_time = data["job_ready_time"]
        self.job_expiry_time = data["job_expiry_time"]

    def from_squeue(self, squeue_line):
        squeue_json = json.loads(squeue_line)
        # self.jobid = squeue_json["JOBID"]
        self.status = squeue_json["STATE"].strip()
        self.time = squeue_json["TIME"].strip()
        self.time_limit = squeue_json["TIME_LIMIT"].strip()
        self.host = squeue_json["BATCHHOST"].strip()

    def is_about_to_expire(self):
        #time = squeue_time_to_timedelta(self.time)
        # TODO: Fix it for long queue times, just an approximation
        if self.job_start_time is not None:
            start = datetime.strptime(self.job_start_time, TIME_FORMAT)
        else:
            return False
        time_limit = squeue_time_to_timedelta(self.time_limit)
        expire_time = squeue_time_to_timedelta(self.job_expiry_time)
        current_time = datetime.now()
        passed_time = current_time - start
        remaining_time = time_limit - passed_time
        if remaining_time < expire_time:
            return True
        return False

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class Service:
    def __init__(self):
        service_job: list[ServiceJob]
        self.name = None
        self.type = None
        self.last_updated = None
        self.sbatch_path = None
        self.chunk_size = 8
        self.target_number_instances = 0
        self.minimum_number_instances = 1
        self.maximum_number_instances = 10
        self.inferences_per_instance = 5
        self.service_jobs = []
        self.number_required_jobs = 0
        self.average_inferences = 0
        self.job_expiry_time = None

    def calculate_active_service_jobs(self):
        active_jobs = 0
        for service_job in self.service_jobs:
            if not service_job.is_about_to_expire():
                active_jobs += 1
        return active_jobs

    def get_active_inferences(self):
        """Returns the number of active processes for this service"""
        # Command:  ps aux | grep {app} | wc -l
        cmd = f"ps aux | grep {self.name} | wc -l"
        # Run the subprocess and get the result
        ps_output = subprocess.run(cmd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).stdout.decode('utf-8').strip()
        # Because subprocess, there are two greps
        if len(ps_output) > 0: 
            return int(ps_output) - 2
        logging.error("Failed to get ps output")
        return 0

    def get_unready_jobs(self):
        unready_jobs = []
        for service_job in self.service_jobs:
            if service_job.status == "R" and not service_job.ready:
                unready_jobs.append(service_job)
                if service_job.job_start_time is None:
                    service_job.job_start_time = datetime.now().strftime(TIME_FORMAT)
        return unready_jobs

    def drop_expired_jobs(self):
        expired_jobs = []
        for service_job in self.service_jobs:
            if service_job.status not in VALID_STATES:
                expired_jobs.append(service_job)
        for service_job in expired_jobs:
            self.service_jobs.remove(service_job)
            logging.debug(f'Dropping job:\n{service_job.to_json()}')

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def from_json(self, data):
        self.name = data['name']
        self.type = data['type']
        self.chunk_size = data['chunk_size']
        self.target_number_instances = data['target_number_instances']
        self.minimum_number_instances = data['minimum_number_instances']
        self.maximum_number_instances = data['maximum_number_instances']
        self.inferences_per_instance = data['inferences_per_instance']
        self.sbatch_path = data['sbatch_path']
        self.job_expiry_time = data['job_expiry_time']
        self.number_required_jobs = 0
        self.last_updated = data['last_updated']
        self.average_inferences = data['average_inferences']
        for service_job_data in data['service_jobs']:
            job = ServiceJob()
            job.from_json(service_job_data)
            self.service_jobs.append(job)

class ServiceList:
    def __init__(self):
        services: list[Service]
        self.services = []
        self.last_updated = None

    def get_all_ports(self):
        ports = set()
        for service in self.services:
            for service_job in service.service_jobs:
                ports.add(service_job.port)
        return ports

    def save_to_file(self, update_services = True):
        self.last_updated = datetime.now().strftime(TIME_FORMAT)
        json_data = self.to_json()
        with open(os.path.join(service_dir, cluster_file), "w") as f:
            f.write(json_data)
        # Write service files for cloud_interface
        if update_services:
            for s in self.services:
                with open(os.path.join(service_dir, f"{s.name}.service"), "w") as f:
                    f.write('BACKENDS=(' + ' '.join([f'"{j.host}:{j.port}"' for j in s.service_jobs if j.ready and j.host is not None]) + ')')

    def update_service_job_from_queue(self, squeue_output):
        for line in squeue_output:
            if len(line) == 0:
                continue
            # Match running jobs to service_jobs via jobid
            line_dict = json.loads(line)
            jobid = int(line_dict["JOBID"].strip())
            service_job = match_jobid_to_service_job(jobid, self)
            if service_job is None:
                # logging.warning(f"Job with ID {jobid} is not accounted for.")
                continue
            # Update service_job information
            service_job.from_squeue(line)

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def from_json(self, data):
        for service_dict in data["services"]:
            service = Service()
            service.from_json(service_dict)
            self.services.append(service)

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


def match_jobid_to_service_job(jobid, service_list):
    for service in service_list.services:
        for service_job in service.service_jobs:
            if service_job.jobid == jobid:
                return service_job
    return None


def main():
    logging.basicConfig(filename=log_filepath, level=logging.INFO,
                        format='%(asctime)s.%(msecs)d %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    #logging.info(str(sys.argv[1:]))
    if len(sys.argv) < 2:
        ## ERROR: No arguments given
        return
    if sys.argv[1] == 'check_routine':
        return check_routine()
    if sys.argv[1] == 'init':
        return init()
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


def check_routine():
    """Routine called every 60 seconds or so. Schedules the running backend jobs"""
    logging.debug('Checking routine.')
    lock_acquired = False
    try:
        # Create a mutex via a file signaling that check_routine is already running
        if not acquire_lock():
            return
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
                logging.info(f'Determined that service {service.name} requires {str(number_required_jobs)} additional jobs.')

        # Start new jobs equal to the difference and record the job ids
        occupied_ports = service_list.get_all_ports()
        logging.debug(f'The following ports are already occupied: {str(list(occupied_ports))}')
        new_jobs = []
        for service in service_list.services:
            for _ in range(service.number_required_jobs):
                logging.debug("Starting new job")
                port = generate_random_port_number(occupied_ports)
                # Pass the config and port number to use when calling sbatch to backend.sbatch
                batch_output = subprocess.run(
                    [sbatch_path, service.sbatch_path,
                     f'{service.type}/{service.name}', f"{str(port)}"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.decode('utf-8')
                jobid = int(batch_output.strip())
                logging.info(f'Submitted new job with for {service.type}/{service.name} {str(port)}'
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
        for service in service_list.services:
            unready_jobs += service.get_unready_jobs()

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
            for ready_job in ready_jobs:
                unready_jobs.remove(ready_job)

            squeue_output = get_squeue_status()
            service_list.update_service_job_from_queue(squeue_output)
            logging.debug(f"Current running jobs:\n{squeue_output}")

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
    except Exception as e:
        logging.error(e)
    finally:
        if lock_acquired:
            release_lock()

    return

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
        new_service.type = service["type"]
        new_service.name = service["name"]
        new_service.sbatch_path = service["sbatch"]
        new_service.inferences_per_instance = service["inferences_per_instance"]
        new_service.chunk_size = service["chunk_size"]
        new_service.maximum_number_instances = service["maximum_number_instances"]
        new_service.minimum_number_instances = service["minimum_number_instances"]
        new_service.job_expiry_time = service["job_expiry_time"]
        new_service.number_required_jobs = 0
        new_service.target_number_instances = 0
        new_service_list.services.append(new_service)
    json_data = new_service_list.to_json()
    with open(os.path.join(service_dir, cluster_file), "w") as f:
        f.write(json_data)

if __name__ == '__main__':
    main()
