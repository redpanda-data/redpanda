#!/usr/bin/env bash

set -e

packagecloud_token=$1

set_up_rpm () {
    # Configure the Vectorized packagecloud RPM repo
    curl -s "https://${packagecloud_token}:@packagecloud.io/install/repositories/vectorizedio/v/script.rpm.sh" | sudo bash

    # Install redpanda
    sudo yum install -y redpanda
}

set_up_deb () {
    # Configure the Vectorized packagecloud DEB repo
    curl -s "https://${packagecloud_token}:@packagecloud.io/install/repositories/vectorizedio/v/script.deb.sh" | sudo bash
    sudo apt install -y redpanda
}

release_file=/etc/os-release

if [ -f  ${release_file} ]
then
    . ${release_file}
else
    echo "ERROR: ${release_file} doesn't exist. Can't determine what the current distribution is"
    exit 1
fi

case ${ID} in
    "debian" | "ubuntu")
        set_up_deb
        ;;
    "rhel" | "fedora" | "amzn" | "centos")
        set_up_rpm
        ;;
    *)
        echo "ERROR: Unsupported distro: ${ID}"
        exit
        ;;
esac

sudo systemctl start redpanda-tuner
sudo systemctl start redpanda
