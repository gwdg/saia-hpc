# Changelog

## Version: v0.4.0
### Refactored
- Major code refactor to improve maintainability and structure.
- Created a separate module for service-related classes, isolating logic for better clarity and reuse.
- Removed unused parameters from the Service class to streamline the codebase.

### Added
- Introduced a new Python script under tools for managing jobs and services, including functionalities to:
    - Add job
    - Add service
    - Edit service
    - Remove service

## Version: v0.3.1
### Added 
- A retry mechanism for scale-to-zero processes to ensure reliable starts of services.
### Introduced
- A health check endpoint to monitor the status of models, returning 'true' or 'false' for each model based on its /health endpoint response.

## Version: v0.3.0

## Added:
1. New Features:
    - Introduced start_service_job functionality in scheduler.py for on-demand job initiation in case no backend is running, automatically handled by cloud_interface.sh.
    - Implemented a health check mechanism in scheduler.py using the new health_check() function to verify the status of running service jobs and cancel unhealthy jobs. This is triggered periodically along with the routine check.
    - Added on-demand backend scaling capability when no backend is available, starting new jobs as needed via scheduler.py through cloud_interface.sh.

2. Logging Enhancements:

    - Improved logging for job requeue and cancellation operations with detailed error handling for both.
    - Added logs for backend availability status checks, especially after initiating new jobs.

3. Concurrency and Timeout Handling:

    - Enhanced readiness probes in scheduler.py using threading for concurrent execution, improving response times when checking multiple jobs' statuses.
    - Introduced --max-time, --speed-limit, and --speed-time flags in the curl commands in cloud_interface.sh to limit excessive wait times when backend services are unresponsive.

4. Job Cancellation:

    - Implemented the cancel_job() function to cancel jobs that fail to become ready within the expected time frame.

## Changed:

1. Service Auto-Scaling Logic:

    - Refined job scaling to dynamically adjust the number of required jobs based on active inferences. The system can now handle scaling to zero for services with no active jobs when requested.

2. Cluster Configuration:

    - Extended the check_routine() logic to support both periodic and on-demand job checks, allowing better control over job initialization.
    
3. Keep-alive Enhancements:

    - Adjusted the keep-alive mechanism in cloud_interface.sh to ensure routine and health checks only run if the elapsed time since the last check exceeds a specified threshold (10 seconds).
    - Incorporated a system to track the last update time for checks using the .last_update file.

4. Error Handling:

    - Enhanced error handling for backend job failures, especially when services do not respond within the expected time or exceed the readiness threshold.
