FROM public.ecr.aws/docker/library/golang:1.20.5

WORKDIR /workspace
# Copy the bin
COPY ./bin/migration-cli /bin

ENTRYPOINT ["/bin/migration-cli"]
