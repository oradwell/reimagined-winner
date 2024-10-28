#!/usr/bin/env bash

# Our queue consumers are not designed to run for a long time
# This is against Kubernetes principles if it exits it's considered a pod failure
# With this script commands can run with max-runtime and restart immediately

if [[ -z "$@" ]]
then
  echo >&2 "Usage: $0 <command> [<arguments>]"
  exit 1
fi

while [ 1 ]
do
  # Run the command
  $@
  exit_code=$?

  # If command exited with a failure
  # exit the loop to allow pod restart
  if [ "${exit_code}" -gt 0 ]
  then
    exit "${exit_code}"
  fi

  sleep 1
done
