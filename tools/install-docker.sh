#!/bin/bash
set -ex

if ! command -v docker; then

  # add docker repo
  dnf -y install dnf-plugins-core
  dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo

  # install docker-ce
  dnf install docker-ce docker-ce-cli containerd.io

  # docker doesn't work with cgroups v2
  grubby --update-kernel=ALL --args="systemd.unified_cgroup_hierarchy=0"

  # start daemon
  systemctl start docker

  # enable start on boot
  sudo systemctl enable docker

  # add docker group and add current user
  groupadd docker
  usermod -aG docker $USER

  echo "docker been installed and configured. Please close this terminal"
  echo "session and open a new one so you can run docker commands."

fi
