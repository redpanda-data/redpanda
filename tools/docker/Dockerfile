ARG BASE_IMAGE_OS_NAME=fedora
ARG BASE_IMAGE_OS_VERSION=38
FROM ${BASE_IMAGE_OS_NAME}:${BASE_IMAGE_OS_VERSION}

COPY --chown=0:0 install-dependencies.sh /

RUN CLEAN_PKG_CACHE=true /install-dependencies.sh && \
    rm install-dependencies.sh
