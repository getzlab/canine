#!/bin/bash
service munge start

while ! [[ -f /mnt/nfs/controller.ready ]]
do
  echo $(hostname) waiting for controller to start
  sleep 10
done

slurmd -D
