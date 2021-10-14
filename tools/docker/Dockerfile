FROM fedora:34

COPY install-dependencies.sh /

RUN /install-dependencies.sh && \
    dnf clean all && \
    rm install-dependencies.sh
