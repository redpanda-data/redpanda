# Redpanda Data Transform SDKs

This directory holds the various libraries that we support for Data Transforms. The code here is compiled
into the user's transform and abstracts away the implmentation details of how the WebAssembly virtual machine
is used within the Redpanda broker.

See each language directory for more info.

### Deployment

Deploying data transforms SDKs is automated using GitHub Actions. All SDKs are versioned together at the moment,
so one just needs to push a tag in the form of `transform-sdk/v1.0.0` then all SDKs will be released using that
version. To see the GitHub action responsible for this, check out `.github/workflows/transfrom-sdk-release.yml`
