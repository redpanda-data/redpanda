FROM public.ecr.aws/docker/library/golang:1.20.5

WORKDIR /workspace
# Copy the bin
COPY ./bin/cluster-to-redpanda-migration /bin

ENTRYPOINT ["/bin/cluster-to-redpanda-migration"]
