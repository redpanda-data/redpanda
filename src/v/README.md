# High Level Overview

| Folder          | Description |
--------------    | ----------- |
ssx             | Seastar extensions |        
cluster         | Redpanda brains. The oracle of truth / cluster management |
bytes           | Low level byte manipulations |
config          | cluster settings |
test_utils      | testing utilities |
platform        | Machine dependent settings like ssse3 instructions |
coproc          | WASM / Coprocessor engine for lambda transforms |
resource_mgmt   | CPU and IO priority | 
utils           | code utils |
hashing         | hashing utility adaptors often used in cryptography or checksumming |
storage         | low level bits of the storage api |
redpanda        | high level program - main entry point |
finjector       | failure injector framework for testing and correctness |
json            | json manipulation utilities |
http            | HTTP conversion and utilities |
kafka           | Kafka compatibility protocol layer |
compression     | utilities for supporting multiple compressor types |
model           | internal code modeling utilities - fundamental types |
raft            | consensus module |
prometheus      | telemetry utilities |
random          | pseudo-random number generation utils|
syschecks       | system sanity checks |
rpc             | remote procedure call protocol for machine-to-machine communication |
pandaproxy      | HTTP Gateway module | 
reflection      | C++ static reflection and type traversal utils |
s3              | Amazon S3 object storage integration |


> Note: use `find . -type d -print -maxdepth 1` to generate
