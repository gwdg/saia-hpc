#!/bin/bash

##############################################################################
## This script is designed to run on a login node of a slurm-based cluster  ##
## To use with the proxy-hpc, add a forced SSH command to run this script   ##
##############################################################################

##############################################################################
## Configuration                                                            ##
##############################################################################

DEV_MODE=false      # If false, runs the scheduler on keep-alive messages.
STREAM_INPUT=true   # If true, will stream input from stdin unless -d is passed
LOGGING=false       # If true, will log requests to file. Use only in dev setting.

##############################################################################
## Startup                                                                  ##
##############################################################################

## Get script directory
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

## Handle keep-alive requests
if  [[ "$SSH_ORIGINAL_COMMAND" == keep-alive ]];
then
    echo OK
    # Check if last_execution_time file exists
    if [ -f last_update ]; then
        # Read the last execution time from the file
        last_update=$(cat .last_update)
    else
        # If the file doesn't exist, initialize the last execution time to 0
        last_update=0
    fi
    # Calculate the elapsed time since the last execution
    current_time=$(date +%s)
    elapsed_time=$((current_time - last_update))
    if [ "$elapsed_time" -ge "5" ]; then
        if [ "$DEV_MODE" = false ]; then
            echo "$current_time" > last_update
            ./scheduler.py check_routine
        fi
    fi
    exit
fi

##############################################################################
## Logging in dev mode                                                      ##
##############################################################################
if [ "$LOGGING" = true ]; then
  ## Create a datetime variable to include in the filename
  CURRENT_DATETIME=$(date +"%Y%m%d-%H%M%S")
  LOG_DIR=./log
  mkdir -p $LOG_DIR
  LOG_FILE="$LOG_DIR/${CURRENT_DATETIME}.txt"
  echo "$CURRENT_DATETIME" >> $LOG_FILE
  printf "%s\n" "$SSH_ORIGINAL_COMMAND" >> $LOG_FILE

  ## Verify the file was created successfully
  if [[ ! -f $LOG_FILE ]]
  then
    echo "Error creating log file"
  fi
fi

##############################################################################
## Parse input                                                              ##
##############################################################################

## Read the inference_id, user_id, service name, request_path, and data from input
IFS=$'\n' read -d '' -r -a lines <<< "$SSH_ORIGINAL_COMMAND"
INFERENCE_ID=${lines[0]}
USER_ID=${lines[1]}
APP=${lines[2]}
REQUEST_PATH=${lines[3]}
RAW_DATA=""
for ((i=4; i<${#lines[@]}; i++))
do
    RAW_DATA+="${lines[i]}"
done

##############################################################################
## Security Check                                                           ##
##############################################################################

# validate INFERENCE_ID and APP
for val in "$INFERENCE_ID" "$APP"; do
    if [[ $val =~ [^a-zA-Z0-9._-\+] ]] || [ ${#val} -gt 256 ]; then
        printf "HTTP/1.1 400 Bad Request\r\nContent-Type: text/html; charset=UTF-8\r\nDate: $(date -R)\r\nServer: KISSKI\r\n\r\nError: Only alphanumeric characters are allowed for INFERENCE_ID and APP, and they should not exceed 256 characters."
        exit 1
    fi
done

# validate USER_ID
if [[ "$USER_ID" =~ [^a-zA-Z0-9._-@\+] ]] || [ ${#USER_ID} -gt 256 ]; then 
    printf "HTTP/1.1 400 Bad Request\r\nContent-Type: text/html; charset=UTF-8\r\nDate: $(date -R)\r\nServer: KISSKI\r\n\r\nError: Only alphanumeric characters and '._@-' are allowed for USER_ID, and it should not exceed 256 characters."
    exit 1
fi

# validate REQUEST_PATH
if [[ "$REQUEST_PATH" =~ [^a-zA-Z0-9_/\.\-\+\?\=\&] ]] || [ ${#REQUEST_PATH} -gt 256 ]; then
    printf "HTTP/1.1 400 Bad Request\r\nContent-Type: text/html; charset=UTF-8\r\nDate: $(date -R)\r\nServer: KISSKI\r\n\r\nError: the REQUEST_PATH provided contains illegal characters or exceeds the 256 character limit."
    exit
fi

# Handle error cases
BACKENDS=()
if [ -f ./services/$APP.service ]; then
   source ./services/$APP.service
else
   printf "HTTP/1.1 404 Not Found\r\nContent-Type: text/html; charset=UTF-8\r\nDate: $(date -R)\r\nServer: KISSKI\r\n\r\nModel Not Found\r\n"
   exit
fi

BACKEND_CNT=${#BACKENDS[@]}
if [ "$BACKEND_CNT" -eq 0 ]; then
  printf "HTTP/1.1 404 Not Found\r\nContent-Type: text/html; charset=UTF-8\r\nDate: $(date -R)\r\nServer: KISSKI\r\n\r\nModel Not Loaded\r\n"
  exit
fi

RANDOM_BACKEND=$((( $$ + $(date +%N) ) % BACKEND_CNT))
BACKEND=${BACKENDS[$RANDOM_BACKEND]}

##############################################################################
## Start parsing headers                                                    ##
##############################################################################

read -r -a raw_array <<< "$RAW_DATA"
set -- "${raw_array[@]}"

# Parse arguments
while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        -X)
        CURL_METHOD="$2"
        shift # past argument
        shift # past value
        ;;
        -H)
        if [ ! -z "$CURRENT_HEADER" ]; then
            HEADERS+=("$CURRENT_HEADER")
            CURRENT_HEADER=""
        fi
        shift # past argument
        ;;
        -d)
        if [ ! -z "$CURRENT_HEADER" ]; then
            STREAM_INPUT=false
            HEADERS+=("$CURRENT_HEADER")
            CURRENT_HEADER=""
        fi
        DATA="${@:2}"
        break
        ;;
        *)
        if [ -z "$CURRENT_HEADER" ]; then
            CURRENT_HEADER="$1"
        else
            CURRENT_HEADER="$CURRENT_HEADER $1"
        fi
        shift # past argument
        ;;
    esac
done

# Add the last header if it exists
if [ ! -z "$CURRENT_HEADER" ]; then
    HEADERS+=("$CURRENT_HEADER")
fi

# Build curl command
curl_headers=()
for header in "${HEADERS[@]}"
do
    header="${header%\"}"  # Removes trailing quote
    header="${header#\"}"  # Removes leading quote
    curl_headers+=("$header")
done

if [ "$BACKEND" = "localhost" ]; then
    echo OK
    exit
fi
##############################################################################
## Send request                                                             ##
##############################################################################

$SCRIPT_DIR/scheduler.py begin_inference "$INFERENCE_ID" "$USER_ID" "$APP" > /dev/null 2>&1 &

if [ "$STREAM_INPUT" = false ]; then
  if curl -i -f -N -g --connect-timeout 5 --max-time 120 --speed-limit 1 --speed-time 20 -X "$CURL_METHOD" "${curl_headers[@]/#/-H}" "$BACKEND$REQUEST_PATH" -d "@/dev/stdin" <<< "$DATA"; then
    $SCRIPT_DIR/scheduler.py end_inference "$INFERENCE_ID" "$USER_ID" "$APP" > /dev/null 2>&1 &
  else
    printf "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/html; charset=UTF-8\r\nDate: $(date -R)\r\nServer: KISSKI\r\n\r\nConnection to model broke\r\n"
  fi
else
  if curl -i -f -N -g --connect-timeout 5 --max-time 120 --speed-limit 1 --speed-time 20  -X "$CURL_METHOD" "${curl_headers[@]/#/-H}" "$BACKEND$REQUEST_PATH" -T - ; then
    $SCRIPT_DIR/scheduler.py end_inference "$INFERENCE_ID" "$USER_ID" "$APP" > /dev/null 2>&1 &
  else
    printf "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/html; charset=UTF-8\r\nDate: $(date -R)\r\nServer: KISSKI\r\n\r\nConnection to model broke\r\n"
  fi
fi