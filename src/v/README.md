# High Level Overview

| Folder               | Description |
---------------------- | ----------- |
archival               | Archival to cloud object storage subsystem |
base                   | Redpanda foundational library |
bytes                  | Low level byte manipulations and containers |
cloud_roles            | Authentication mechanisms with various cloud vendors |
cloud_storage          | Tiered storage integration for Redpanda topics |
cloud_storage_clients  | Clients for various vendor object storage implementations |
cluster                | Redpanda brains. The oracle of truth / cluster management |
compat                 | Compatibility checks |
compression            | utilities supporting compression/decompression of many types |
config                 | Redpanda cluster level and node level configuration options |
container              | Generic Redpanda specific containers and data structures |
crypto                 | Middleware library used to perform cryptographic operations |
datalake               | Writing Redpanda data to Iceberg |
features               | Cluster feature flags for rolling upgrades |
finjector              | Failure injector framework for testing and correctness |
hashing                | hashing utility adaptors often used in cryptography or checksumming |
http                   | A custom HTTP client written in seastar |
io                     | Low level subsystem providing disk/persistence access |
json                   | JSON manipulation utilities |
kafka                  | Kafka compatibility protocol layer |
metrics                | Internal metrics reporting mechanisms |
migrations             | Migrations  for previous versions of Redpanda |
model                  | internal code modeling utilities - funamental types |
net                    | Wrapper over seastar networking code |
pandaproxy             | HTTP Gateway |
prometheus             | telemtry utilities |
raft                   | consensus protocol |
random                 | pseudo-random number generation utils |
redpanda               | high level program - main entry point |
reflection             | C++ static reflection and type traversal utils |
resource_mgmt          | CPU, Disk and IO management |
rp_util                | CLI utility for Redpanda |
rpc                    | Remote procedure call protocol for internal cluster communication |
schema                 | High-level interface for interacting with schemas and the schema registry |
security               | Authentication and authorization in Redpanda |
serde                  | Serialization framework |
ssx                    | Custom extensions for seastar |
storage                | Low level storage interface |
strings                | String manipulation routines |
syschecks              | system sanity checks |
test_utils             | Testing utilities |
transform              | The control and data plane for in-broker data transforms |
utils                  | Code utilities |
v8_engine              | Legacy v8 integration code |
wasm                   | WebAssembly engine that powers transforms |

> Note: use `find . -type d -print -maxdepth 1` to generate
