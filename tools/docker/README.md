# Docker Image with Redpanda Toolchain

The `Dockerfile` available in this folder defines the contents of the image published to <https://hub.docker.com/r/vectorized/redpanda-toolchain>. 
To build Redpanda using this image:

```bash
docker pull vectorized/redpanda-toolchain

cd redpanda/
docker run --rm -ti -v $PWD:$PWD:Z -w $PWD vectorized/redpanda-toolchain ./build.sh
```

You can add the `--ipc=host` flag to share the host's `/dev/shm` folder, so that the `CCACHE_DIR` folder (`/dev/shm/redpanda` by default) can be available across `docker run` invocations. 

In systems with SELinux enabled, the `:Z` bind-mount option might cause the container to take too long to setup (due to SELinux labels being applied to individual files). 
To avoid this, you can add the `--privileged` flag, and that will instruct Docker (or Podman) to skip applying SELinux profiles. 

If you need rebuild the toolchain image locally:

```bash
docker build \
  -t vectorized/redpanda-toolchain \
  -f tools/docker/Dockerfile \
  .
```
