# Data Transforms Library

This library contains the control and data plane that powers Data Transforms. Data Transforms as an entire component is divided into 4 major parts:

1. Wasm virtual machine (Wasmtime)
1. Wasm integration for data transforms (src/v/wasm/ module)
1. Data transforms distributed system layer (this module)
1. Data transforms SDKs that run inside the VM (src/transform-sdk)

At a high level this library defines the architecture for how data flows through transforms. There are two major components: transform_manager and transform_processor representing the control plane and the data plane respectively.

The two planes are loosely coupled in an event emitting pattern. The control plane is a single fiber loop that responds to events such as raft leadership notifications for partitions, and data plane errors. The control plane loop then acts accordingly by scheduling, starting and shutting down corresponding transform_processors. The data plane is 3 fibers to read, transform and write data, all connected via buffers (defined in `transform_queue.h`).

Other parts of the subsystem are the commit batcher, which recieves requests to commit processed offsets for individual transform processors. The batcher periodically (according to cluster configuration) commits all the offsets on that core at once, so that the number of commits scales with the number of cores, not the number of partitions.

Most IO in this subsystem is abstracted into interfaces so that they can be swapped out and unit tested. The actual implementation and stitching up of the production components is all done in `api.cc`. The underlying operations are mostly implemented in the transform/rpc subsystem, which defines the internal data path for transforms (to circumvent the kafka API) over our internal RPC protocol.
