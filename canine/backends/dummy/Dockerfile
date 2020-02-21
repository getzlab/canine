FROM gcr.io/broad-getzlab-workflows/slurm_gcp_docker_base
MAINTAINER Aaron Graubert

RUN apt-get update && apt-get install -y \
  apt-transport-https \
  ca-certificates \
  curl \
  wget \
  gnupg-agent \
  vim \
  libmysqlclient-dev \
  ssh \
  software-properties-common

# Munge fixes
RUN echo exit 0 > /usr/sbin/policy-rc.d

# Docker
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - && \
  apt-key fingerprint 0EBFCD88 && echo "deb [arch=amd64] https://download.docker.com/linux/ubuntu disco stable" > /etc/apt/sources.list.d/docker-ce.list && \
  apt-get update && apt-get install -y docker-ce

# Python packages
RUN pip3 install psutil

COPY controller.py /
COPY worker.py /
COPY conf /conf_templates

RUN chmod 755 controller.py worker.py

ENTRYPOINT
CMD /bin/bash
