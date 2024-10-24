# Transform RPC Libary

Data Transforms has it's own set of RPCs for both the data path of writing transform output, as well as various control plane actions. To use these RPCs, there is a client available that gives a nicer interface to these RPCs.

## Data Plane RPCs

These are the RPCs used on the data plane, to understand more see the documentation in `client.h`.

* produce
* offset_fetch
* batch_offset_commit
* find_coordinator

## Control Plane RPCs

These are the RPCs used on the control plane, to understand more see the documentation in `client.h`.

* store_wasm_binary
* delete_wasm_binary
* load_wasm_binary
* generate_report
* list_committed_offsets
* delete_committed_offsets

## Client implementation

The client takes each operation to be performed and sees if this node can service the request or if
it needs to make the request on the network. If it can service the request on this node, then the
request is directly forwarded to the local service without a network hop, otherwise it uses our RPC
framework to make the request on the appropriate node. Both kinds of requests are automatically retried
for specific error codes.

## Service implementation

The service handles interacting with the local system as a whole on behalf of the transform subsystem.
For fetch/produce data plane actions these look similar to our kafka handlers, while other actions will
directly interact with the correct subsystem to perform actions (such as getting the offset STM).

## System dependencies

All of the client/service is able to be unit tested, which is handled by creating interfaces for the
rest of the system. In production usage we have implemented these interface uses the appropriate cluster
component (controller, partition manager, etc), while in unit tests we inject fake implementations.
