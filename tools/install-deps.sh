#!/bin/bash
set -ex

function debs() {
    if ! command -v add-apt-repository; then
        apt-get update -y
        apt-get -y install software-properties-common
    fi
    add-apt-repository ppa:ubuntu-toolchain-r/test -y
    apt update -y
    apt-get install -y \
            build-essential \
            binutils-dev \
            gcc-9 \
            g++-9 \
            libtool \
            m4 \
            ninja-build \
            automake \
            cmake \
            pkg-config \
            xfslibs-dev \
            systemtap-sdt-dev \
            ragel \
            ccache \
            pigz \
            zlib1g-dev \
            devscripts \
            debhelper \
            fakeroot \
            rpm \
            libsystemd-dev \
            python3-jinja2 \
            python3-pip \
            python3-venv
    update-alternatives \
      --install /usr/bin/gcc gcc /usr/bin/gcc-9 60 \
      --slave /usr/bin/g++ g++ /usr/bin/g++-9
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
    cmake="cmake"
    case ${ID} in
        centos|rhel)
            MAJOR_VERSION="$(echo "$VERSION_ID" | cut -d. -f1)"
            if test "$MAJOR_VERSION" = 7 ; then
                cmake="cmake3"
            fi
    esac


    ${yumdnf} install -y \
              ${cmake} \
              binutils-devel \
              gcc-c++ \
              ninja-build \
              m4 \
              libtool \
              make \
              ragel \
              xfsprogs-devel \
              systemtap-sdt-devel \
              libasan \
              libubsan \
              libatomic \
              ccache \
              doxygen \
              pigz \
              zlib-devel \
              devscripts \
              debhelper \
              fakeroot \
              rpm-build \
              systemd-devel \
              python3-jinja2 \
              python3-pip
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
