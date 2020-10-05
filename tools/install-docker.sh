#!/bin/bash
set -ex

distro=$(lsb_release -s -i | tr '[:upper:]' '[:lower:]')

fedora_os_version=$(grep VERSION_ID /etc/os-release | cut -d '=' -f 2)

this_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/" && pwd)"

if ! command -v docker; then

  if [ ${distro} = "fedora" ]; then

    if [ "${fedora_os_version}" -ge 31 ]; then
      # Error out if systemd.unified_cgroup_hierarcy != 0
      "${this_dir}"/cgroups-hierarchy-test.sh
    fi

    if [ "${fedora_os_version}" -ge 32 ]; then
      dnf install -y moby-engine docker-compose
    else
      # add docker repo
      dnf -y install dnf-plugins-core
      dnf config-manager -y --add-repo https://download.docker.com/linux/fedora/docker-ce.repo

      # install docker-ce
      if [ "${fedora_os_version}" -eq 31 ]; then
        # Install via this repository
        dnf install -y --enablerepo=docker-ce-stable --releasever=31 docker-ce-cli docker-ce
      else
        dnf install -y docker-ce docker-ce-cli containerd.io
      fi
    fi

  elif [ ${distro} = "ubuntu" ]; then

    apt-get --assume-yes install \
      apt-transport-https \
      ca-certificates \
      curl \
      gnupg-agent \
      software-properties-common

    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

    sudo add-apt-repository \
      "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
     $(lsb_release -cs) \
     stable"

    sudo apt-get --assume-yes install docker-ce docker-ce-cli containerd.io

  fi

  # start daemon
  systemctl start docker

  # enable start on boot
  sudo systemctl enable docker

  # add current user to docker group
  usermod -aG docker $USER

  echo "docker been installed and configured. Please close this terminal"
  echo "session and open a new one so you can run docker commands."
fi
