#!/bin/bash

CMD_TMP=$(mktemp)

# entrypoint exports (global canine variables)
sed -n '/^export CANINE/p' ../../entrypoint.sh > $CMD_TMP

# setup.sh exports (job-specific variables)
sed -n '/^export/p' setup.sh >> $CMD_TMP

# run localization.sh
echo "./localization.sh" >> $CMD_TMP

# run script to get into Docker
if grep -q "^#WOLF_DOCKERIZED_TASK" ../../script.sh; then
	sed -n '3,/^docker run/p' ../../script.sh | sed -e '$s/ - <<.*$//' -e '$s/-i/-ti/' >> $CMD_TMP
else
	# TODO: enter Slurm docker if no Docker is specified, and we used the Docker
	# backend
	echo "bash -i" >> $CMD_TMP
fi

bash $CMD_TMP
rm $CMD_TMP
