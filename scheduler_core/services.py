#!/usr/bin/env python3

import json
import subprocess
import logging
import os
from datetime import datetime, timedelta


service_dir = "./services"
cluster_file = "cluster.services"
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
VALID_STATES = {"R", "PD", "S", "CF"}

def squeue_time_to_timedelta(time_str):
    try:
        minutes, seconds = map(int, time_str.split(':'))
    except:
        hours, minutes, seconds = map(int, time_str.split(':'))
        return timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return timedelta(minutes=minutes, seconds=seconds)


def match_jobid_to_service_job(jobid, service_list):
    for service in service_list.services:
        for service_job in service.service_jobs:
            if service_job.jobid == jobid:
                return service_job
    return None


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
        self.host = squeue_json["NODELIST"].strip()

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
        self.id = None
        self.owned_by = 'chat-ai'
        self.sbatch_path = None
        self.target_number_instances = 0
        self.minimum_number_instances = 0
        self.maximum_number_instances = 0
        self.inferences_per_instance = 0
        self.service_jobs = []
        self.number_required_jobs = 0
        self.average_inferences = 0
        self.created_time = None
        self.job_expiry_time = None
        self.input = []
        self.output = []

    def calculate_active_service_jobs(self):
        active_jobs = 0
        for service_job in self.service_jobs:
            if not service_job.is_about_to_expire():
                active_jobs += 1
        return active_jobs

    def get_active_inferences(self):
        """Returns the number of active processes for this service"""
        # Command:  ps aux | grep {app} | wc -l
        cmd = f"ps aux | grep '{self.id} ' | wc -l"
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

    def get_pd_jobs(self):
        pd_jobs = []
        for service_job in self.service_jobs:
            if service_job.status == "PD":
                pd_jobs.append(service_job)
        return pd_jobs

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
        self.id = data['id']
        self.target_number_instances = data['target_number_instances']
        self.minimum_number_instances = data['minimum_number_instances']
        self.maximum_number_instances = data['maximum_number_instances']
        self.inferences_per_instance = data['inferences_per_instance']
        self.sbatch_path = data['sbatch_path']
        self.job_expiry_time = data['job_expiry_time']
        self.number_required_jobs = 0
        self.average_inferences = data['average_inferences']
        self.input = data['input']
        self.output = data['output']
        self.created_time = data['created_time']
        self.owned_by = data['owned_by']
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
                with open(os.path.join(service_dir, f"{s.id}.service"), "w") as f:
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
