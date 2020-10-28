FROM debian:buster-slim

LABEL maintainer="Vectorized.io"

ENV DEBIAN_FRONTEND=noninteractive
ARG redpanda_pkg

COPY $redpanda_pkg redpanda.deb
RUN dpkg -i redpanda.deb && \
  rm -r redpanda.deb

USER redpanda
ENTRYPOINT ["rpk"]
CMD ["start", "--overprovisioned"]