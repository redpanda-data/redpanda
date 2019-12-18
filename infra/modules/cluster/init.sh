#!/usr/bin/env bash

set -ex

packagecloud_token=$1

set_up () {
    distro=$1
    existing_pkg=$(find . | grep 'deb\|rpm' | sort | tail -1)
    if [ -n "${existing_pkg}" ]; then
        rpm_cmd="sudo rpm -ivh ${existing_pkg}"
        deb_cmd="sudo apt install -y ${existing_pkg}"
        run_for_distro "${distro}" "${rpm_cmd}" "${deb_cmd}"
    else
        # Configure the Vectorized packagecloud DEB repo
        rpm_repo_cmd='set_up_repo rpm'
        deb_repo_cmd='set_up_repo deb'
        run_for_distro "${distro}" "${rpm_repo_cmd}" "${deb_repo_cmd}"
        rpm_cmd='sudo yum install -y redpanda'
        deb_cmd='sudo apt install -y redpanda'
        run_for_distro "${distro}" "${rpm_cmd}" "${deb_cmd}"
    fi
}

set_up_repo () {
    pkg=$1
    curl -s "https://${packagecloud_token}:@packagecloud.io/install/repositories/vectorizedio/v/script.${pkg}.sh" | sudo bash
}

run_for_distro () {
    distro=$1
    rpm_cmd=$2
    deb_cmd=$3
    case ${distro} in
        "debian" | "ubuntu")
            ${deb_cmd}
            ;;
        "rhel" | "fedora" | "amzn" | "centos")
            ${rpm_cmd}
            ;;
        *)
            echo "ERROR: Unsupported distro: ${ID}"
            return 1
            ;;
    esac
}

release_file=/etc/os-release

if [ -f  ${release_file} ]; then
    . ${release_file}
else
    echo "ERROR: ${release_file} doesn't exist. Can't determine what the current distribution is"
    exit 1
fi

set_up "${ID}"

sudo systemctl start redpanda-tuner
sudo systemctl start redpanda
