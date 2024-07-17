FROM public.ecr.aws/docker/library/ubuntu:jammy-20230816 as base

RUN apt update && \
    apt install -y dnsmasq && \
    rm -rf /var/lib/apt/lists/*

EXPOSE 53/udp

ENTRYPOINT ["dnsmasq", "-k", "-q"]
