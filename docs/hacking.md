# hacking

## to build: 

`./tools/build.py --deps=true --build=release`

change `--build=debug` for debug builds and remove `--deps=true` if you don't want to 
install the deps. 

To build one particular target do:

`cd build/Release && ninja redpanda` it will build the `redpanda` target and all the transitive
dependencies and nothing else. 


## gdb

We build our DEBUG binaries w/ `--split-dwarf`

More info at https://gcc.gnu.org/wiki/DebugFission.

This effectively splits debug info files to a `.dwo`

Note that distcc is *not* compatible with it, but icecream
(https://github.com/icecc/icecream) is.

## ci/cd

This is a quick guide on how to configure and run the CI pipeline 
locally on your machine (linux/macOS only).

 1. Install the Google Cloud SDK, i.e. install the `gcloud` command 
    line utility. [See here][gcloud-install] for instructions.

 2. Configure the SDK (run `gcloud init`) so that it associates your 
    machine to the `redpandaci` project of the `vectorized.io` 
    organization. If you already have a project associated currently, 
    you can change to `redpandaci` by running:

    ```bash
    gcloud config set project redpandaci
    ```

 3. Install the `cloud-build-local` command. See [installation 
    instructions here][gcb-local-install].

 4. Configure your local Docker daemon:

    ```bash
    gcloud auth configure-docker
    ```

 5. Trigger a build locally:

    ```bash
    cloud-build-local \
      --config tools/ci/gcbuild.yml \
      --substitutions=SHORT_SHA="1234567",TAG_NAME="na" \
      --dryrun=false .
    ```

    The `SHORT_SHA` variable is needed in order to tag the docker 
    images associated to the build, while the `TAG_NAME` variable is 
    used by the packaging steps.

Builds on GCB are triggered automatically by push events to the 
`vectorizedio/v` repository and its forks. Take a look at the [build 
pipeline](../tools/ci/gcbuild.yml) for more details on what the 
pipeline does.

[gcloud-install]: https://cloud.google.com/sdk/install
[gcb-local-install]: https://cloud.google.com/cloud-build/docs/build-debug-locally

## reproduce a build

We achieve bitwise reproducibility via Docker and only support 
(re)building commits that have previously gone through the GCB 
pipeline (see previous section), as this process generates a docker 
image that has all build dependencies needed to reproduce the build. 
Note that we only persist images for tagged commits and only 
clang-release builds. To build the `redpanda` binary for an arbitrary 
commit (e.g. `release-0.1`):

```bash
cd v
git checkout release-0.1
vtools dbuild --ref release-0.1
```

The above builds the `release-0.1` commit using the container image 
that was used to produce that build. The same frozen toolchain can be 
used to build other commits but keep in mind that successfully 
building depends on how far off the checked out working directory is 
with respect to the docker image (e.g. a new 3rd-party dependency 
might not be available in the image).
