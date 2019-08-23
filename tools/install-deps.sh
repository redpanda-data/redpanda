#!/bin/bash

set -ex

function debs() {
    apt-get update -y
    apt-get install -y \
            ccache \
            python3-distutils-extra \
            clang \
            pigz \
            zlib1g-dev \
            python3-requests \
            devscripts \
            debhelper \
            fakeroot \
            rpm \
            python3-jinja2
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
              pigz \
              zlib-devel \
              python3-requests \
              devscripts \
              debhelper \
              fakeroot \
              rpm-build \
              python3-jinja2
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
