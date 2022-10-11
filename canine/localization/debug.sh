#!/bin/bash

CMD_TMP=$(mktemp)

echo "export CANINE_DEBUG_MODE=1" > $CMD_TMP

# entrypoint exports (global canine variables)
sed -n '/^export CANINE/p' ../../entrypoint.sh >> $CMD_TMP

# setup.sh exports (job-specific variables)
sed -n '/^export/p' setup.sh >> $CMD_TMP

# if workspace and inputs directories don't exist, create them
# this is ordinarily part of setup.sh, but it doesn't do existence checks
cat <<"EOF" >> $CMD_TMP
[ ! -d $CANINE_JOB_INPUTS ] && mkdir -p $CANINE_JOB_INPUTS || :
[ ! -d $CANINE_JOB_ROOT ] && mkdir -p $CANINE_JOB_ROOT || :
chmod 755 $CANINE_JOB_LOCALIZATION
EOF

# run localization.sh
echo "./localization.sh" >> $CMD_TMP

# run script to get into Docker
if grep -q "^#WOLF_DOCKERIZED_TASK" ../../script.sh; then
	sed -n '/^#WOLF_DOCKERIZED_TASK/,/#DEBUG_END/p' ../../script.sh | \
	sed -e '/#DEBUG_OMIT/d' -e '$s/ - <<.*$//' -E -e 's/sudo (-E )?podman/docker/' \
	  -e 's/inspect -t image/inspect/' -e 's/docker run/docker run --rm -ti/' >> $CMD_TMP
else
	# TODO: enter Slurm docker if no Docker is specified, and we used the Docker
	# backend
	echo "bash -i" >> $CMD_TMP
fi

echo "echo 'Running teardown script, please wait ...'" >> $CMD_TMP
echo "./teardown.sh" >> $CMD_TMP

bash $CMD_TMP
rm $CMD_TMP
