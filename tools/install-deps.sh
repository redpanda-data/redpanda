#!/bin/bash

set -ex

function golang_dep() {
	if [[ "$(which go)" == "" ]]; then
        echo "golang"
    else
        echo ""
    fi
}

function debs() {
    apt-get update -y
    apt-get install -y \
            ccache \
            python3-distutils-extra \
            clang \
            $(golang_dep) \
            libudev-dev
}

function rpms() {
    yumdnf="yum"
    if command -v dnf > /dev/null; then
        yumdnf="dnf"
    fi

    ${yumdnf} install -y redhat-lsb-core
    case $(lsb_release -si) in
        CentOS)
            MAJOR_VERSION=$(lsb_release -rs | cut -f1 -d.)
            $SUDO yum-config-manager --add-repo https://dl.fedoraproject.org/pub/epel/$MAJOR_VERSION/x86_64/
            $SUDO yum install --nogpgcheck -y epel-release
            $SUDO rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-$MAJOR_VERSION
            $SUDO rm -f /etc/yum.repos.d/dl.fedoraproject.org*
            ;;
    esac
    ${yumdnf} install -y \
              ccache \
              python3-distutils-extra \
              clang \
              $(golang_dep) \
              systemd-devel
}


source /etc/os-release
case $ID in
    debian|ubuntu|linuxmint)
        debs
        ;;

    centos|fedora)
        rpms
        ;;

    *)
        echo "$ID not supported. Install dependencies manually."
        exit 1
        ;;
esac
