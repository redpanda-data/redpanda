# Transform Logging Library

This module is responsible for collecting logs coming from writes on stdout/stderr from the Wasm virtual machine. These logs sit in a (configurable) buffer size per transform, then we batch them up and write them to the `_redpanda.transform_logs`. Writes happen by using the transform data path RPCs to produce logs. The major entry point to this subsystem is the `log_manager`, which does the routing of logs and collecting into buffers.
