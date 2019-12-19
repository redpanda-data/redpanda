#!/bin/bash
set -ex

function debs() {
    apt-get update -y
    if [ "${UBUNTU_CODENAME}" == "xenial" ] && [ -n "${CI}" ]; then
        cmake_version="3.14.0"
        cmake_full_name="cmake-${cmake_version}-Linux-x86_64.sh"
        apt-get install -y wget
        wget https://github.com/Kitware/CMake/releases/download/v${cmake_version}/${cmake_full_name} -O /tmp/${cmake_full_name}
        chmod +x /tmp/${cmake_full_name}
        /tmp/${cmake_full_name} --skip-license --prefix=/usr
    else
        apt-get install -y cmake
    fi
    apt-get install -y build-essential
    if ! command -v add-apt-repository; then
        apt-get -y install software-properties-common
        apt-get update -y
    fi
    apt-get install -y \
            build-essential \
            libtool \
            m4 \
            ninja-build \
            automake \
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
            python3-pip
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
