FROM fedora:34

COPY --chown=0:0 install-dependencies.sh /

RUN /install-dependencies.sh && \
    dnf clean all && \
    rm install-dependencies.sh
