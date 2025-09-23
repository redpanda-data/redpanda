# This Dockerfile supports running the type-check.py script in a container,
# so it can work everywhere. It should be a very cut-down version of
# tests/docker/Dockerfile which only installs the python deps and bootstraps
# pyright. All the slow stuff should be in the docker build so that we can
# then use the image to do quick type checking.

# this is ubuntu:jammy-20240911.1, but using hash makes it instant/offline to check the image metadata
# should keep this in sync with tests/docker/Dockerfile
FROM public.ecr.aws/docker/library/ubuntu@sha256:0e5e4a57c2499249aafc3b40fcd541e9a456aab7296681a3994d631587203f97 AS base

FROM base AS with-python

ENV TZ="UTC" DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip rsync && \
    rm -rf /var/lib/apt/lists/*

# ==========================
# python
FROM with-python AS build

RUN apt-get update && \
    apt-get install --no-install-recommends -y python3-pip python3-dev git gcc && \
    rm -rf /var/lib/apt/lists/*
COPY setup.py /tmp/rptest-setup/

# install rptest and run pyright --version to prompt it to download its embedded nodejs
RUN python3 -m pip install --no-cache-dir -e /tmp/rptest-setup


FROM with-python AS final

###### install pyright
COPY type-checking/requirements.txt /tmp/pyright.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/pyright.txt

# Copy-in *only* the parts of the python packages needed for type checking,
# leaving the rest behind, which is a huge savings in final image size:
# note that this leaves the packages "broken", but that's fine in this dockerfile
# This step is effectively an optimization of:
# COPY --from=build / /
RUN --mount=from=build,source=/usr/local/lib/,target=/build-lib \
    rsync -av \
        --include="*/" \
        --include="*.py" \
        --include="*.pyi" \
        --include="py.typed" \
        # confluent_kafka includes imports from the C extension module in its API
        --include="confluent_kafka/**" \
        # crc32c is a pure binary component
        --include="crc32c*" \
        --exclude="*" \
        /build-lib/python3.1*/dist-packages/ /usr/local/lib/python3.1*/dist-packages/


COPY type-checking/type-check.* /type-checking/

ENTRYPOINT [ "/type-checking/type-check.py" ]





