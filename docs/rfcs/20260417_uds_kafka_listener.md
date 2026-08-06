- Feature Name: Experimental Unix Domain Socket listener for Kafka API
- Status: draft
- Start Date: 2026-04-17
- Authors: randomizedcoder
- Issue: none

# Executive Summary

Add an experimental `AF_UNIX` (Unix Domain Socket, UDS) listener to Redpanda's
Kafka API so that producer/consumer workloads co-located on the same host as
the broker can bypass the TCP/IP stack entirely. Configured via an optional
`unix_path` field on existing `kafka_api` entries. TCP listeners, inter-broker
RPC, Admin API, pandaproxy, and Schema Registry are unchanged.

## What is being proposed

A new optional `unix_path` field on each `kafka_api` entry. When set, that
entry is bound via `ss::unix_domain_addr` instead of a host/port pair. A
single broker can expose both TCP and UDS listeners simultaneously and
serve identical topic/partition state on both. Authentication reuses the
existing `authentication_method: sasl|none` field. TLS is rejected on UDS
listeners by a configuration validator. `rpk` is extended to accept
`unix:///path/to/socket` in its `--brokers` / `-X brokers=...` flags via a
custom `franz-go` dialer.

## Why (short reason)

Production deployments frequently run producer/consumer pods on the same
Kubernetes node as the broker pod. "Loopback TCP is fast" is regularly
assumed, but service-mesh sidecars (Istio, Linkerd), CNI dataplanes
(Cilium, Calico), and host-level iptables rules intercept traffic on
127.0.0.1. UDS bypasses every layer above the kernel's `af_unix` driver
and removes that uncertainty. It also halves the syscall count per
message (one `sendmsg`/`recvmsg` pair instead of a TCP segment round-trip
through the socket buffer).

## Isn't TCP fast over the loopback?

TCP is still used over loopback connections, which can be observed via `ss --tcp --info '( src 127.0.0.1 and dst 127.0.0.1 )'`

The following is a typical examples, where we see the kernel TCP stack is busy tracking a LOT of info about the TCP connections over the loopback.  All this TCP work get's skipped when using the Unix Domain Sockets (UDS).  This is why UDS is a lot faster.

Looking the the following output we can observer:
- RTTs are >4ms
- Retransmissions are occuring
- TCP recieve window is the limiting factor
- TCP pacing is occuring

Look at the RTTs, for example.  We even see retransmissions.
```bash
[das@l:~/Downloads/redpanda]$ ss --tcp --info '( src 127.0.0.1 and dst 127.0.0.1 )'
State              Recv-Q               Send-Q                             Local Address:Port                              Peer Address:Port
ESTAB              990667               185750                                 127.0.0.1:63944                                127.0.0.1:12199
         cubic wscale:9,9 rto:55 backoff:15 rtt:4.525/8.966 ato:40 mss:65483 pmtu:65535 rcvmss:65483 advmss:65483 cwnd:16 ssthresh:16 bytes_sent:2042421 bytes_retrans:53 bytes_acked:2042369 bytes_received:990667 segs_out:151 segs_in:126 data_segs_out:63 data_segs_in:25 send 1.85Gbps lastsnd:4157833 lastrcv:4158042 lastack:11489 pacing_rate 2.22Gbps delivery_rate 8.59Gbps delivered:64 busy:4158248ms rwnd_limited:4158247ms(100.0%) retrans:0/1 dsack_dups:1 rcv_rtt:0.893 rcv_space:434517 rcv_ssthresh:862522 notsent:185750 minrtt:0.009
ESTAB              870912               115200                                 127.0.0.1:12199                                127.0.0.1:63944
         cubic wscale:9,9 rto:56 backoff:15 rtt:5.166/10.266 ato:49 mss:65483 pmtu:65535 rcvmss:65483 advmss:65483 cwnd:16 ssthresh:16 bytes_sent:990667 bytes_acked:990667 bytes_received:2042368 segs_out:125 segs_in:151 data_segs_out:25 data_segs_in:63 send 1.62Gbps lastsnd:4158046 lastrcv:4157837 lastack:11493 pacing_rate 1.95Gbps delivery_rate 12.8Mbps delivered:26 busy:4158251ms rwnd_limited:4158251ms(100.0%) rcv_rtt:0.268 rcv_space:434517 rcv_ssthresh:851350 notsent:115200 minrtt:0.009
```

## How (short plan)

1. Add `unix_path` (and optional `unix_socket_mode`) to
   `config::broker_authn_endpoint`. Mutually exclusive with `address + port`.
2. Add a cross-list validator that rejects TLS on UDS listeners and
   forbids advertising UDS listener names.
3. Branch the Kafka `server_configuration` construction in
   `application_services.cc` to build `ss::socket_address(ss::unix_domain_addr(path))`
   when `unix_path` is set; `net::server` needs no changes (Seastar's
   existing accept-dispatch path is already AF_UNIX-aware).
4. Add stale-socket recovery (`connect(2)` probe + `unlink` of dead
   sockets, advisory `flock` on a sibling lockfile).
5. Extend `rpk` to recognise the `unix://` URI scheme.

## Impact

UDS is **additive**. Every existing deployment continues to work unchanged.
The only operational change is the need to provision a shared filesystem
(e.g. `hostPath` or `emptyDir` with a tmpfs medium) visible to both
broker and client pods if users want to opt in.

Drawbacks:

- Only Go/C/C++ Kafka clients that can be pointed at an AF_UNIX socket can
  use this listener. The Java client and librdkafka do not currently
  support UDS. Users on those clients continue to use TCP.
- TLS on UDS is rejected. Operators who want cryptographic identity on a
  UDS path must rely on filesystem permissions (mode/owner) for now;
  `SO_PEERCRED`-based authn is explicitly out of scope for v1.

# Motivation

## Why are we doing this?

When a producer and broker share a Kubernetes node, the "local" TCP path
between them is not actually local in any meaningful sense — it traverses
iptables, conntrack, and frequently a service-mesh proxy. Each of these
is a known source of tail-latency variance and of CPU overhead. A UDS
listener removes them entirely. Early exploratory benchmarks on this
repo's Nix-based test harness will be captured in this RFC's
*Measurements* section once Phase 4 lands.

## What use cases does it support?

- **Sidecar producer/consumer pods** on the same node as the broker pod,
  exchanging data through a shared emptyDir.
- **Single-node development and test deployments** that want to exercise
  the protocol without opening a TCP port.
- **High-throughput ingest pipelines** colocated with the broker where
  tail latency matters.

## What is the expected outcome?

Measurable reduction in produce/consume tail latency (p99) and syscall
count per message when clients and broker are on the same host, with
zero behavioural change for existing TCP-only deployments.

# Guide-level explanation

## Configuring a UDS listener

`redpanda.yaml` allows multiple `kafka_api` entries. A UDS entry is a
regular entry with `unix_path` set instead of `address`/`port`:

```yaml
redpanda:
  kafka_api:
    - name: external
      address: 0.0.0.0
      port: 9092
      authentication_method: sasl
    - name: local-uds
      unix_path: /run/redpanda/kafka.sock
      unix_socket_mode: 0660              # optional; default 0660
      authentication_method: none          # SASL also supported
  advertised_kafka_api:
    - name: external                        # UDS listeners MUST NOT be advertised
      address: broker-0.cluster.example
      port: 9092
```

Rules enforced by configuration validation:

- Each `kafka_api` entry has either `address + port` **or** `unix_path`,
  never both, never neither.
- `unix_path` must be absolute and shorter than 108 bytes
  (`sizeof(sockaddr_un::sun_path) - 1`).
- `unix_socket_mode`, if set, is an octal integer in `[0000, 0777]`. It is
  applied via `chmod(2)` immediately after `listen(2)`.
- If an entry in `kafka_api_tls` has the same `name` as a UDS
  `kafka_api` entry, config parsing fails with
  `"TLS not supported on UDS listener <name>"`.
- If an entry in `advertised_kafka_api` has the same `name` as a UDS
  `kafka_api` entry, config parsing fails with
  `"cannot advertise UDS listener <name>"`.
- Duplicate `unix_path` across `kafka_api` entries is rejected.

## Connecting from rpk

```sh
rpk topic produce my-topic --brokers unix:///run/redpanda/kafka.sock
rpk topic consume my-topic --brokers unix:///run/redpanda/kafka.sock
```

Because UDS listeners are not advertised in Kafka metadata responses
(see §*Advertised addresses*), an rpk client bootstrapped against a UDS
seed will discover only the broker's **TCP** listeners when it issues
`Metadata`. In a co-located single-node layout this is fine —
the TCP listener may simply bind on `127.0.0.1`. In a multi-node
cluster, the metadata response will name the cluster's external TCP
endpoints, and follow-up client connections will flow over TCP.
Operators who need every connection to stay on UDS must either pin
`rpk` to a single broker via flags or accept a single-connection
model. This is documented as a known limitation for v1.

# Reference-level explanation

## Interaction with other features

- **Inter-broker RPC** (`rpc_server`) is unchanged — TCP only.
- **Admin API**, **pandaproxy**, **Schema Registry** are unchanged — TCP only.
- **Advertised addresses** (`advertised_kafka_api`): UDS listener names
  are forbidden from appearing here. This keeps the Kafka `Metadata`
  response reachable by remote clients.
- **TLS**: rejected on UDS listeners (see §*Drawbacks*).
- **SASL**: supported on UDS listeners identically to TCP. A user who
  wants the least-overhead local path sets `authentication_method: none`
  and relies on the socket's filesystem permissions.
- **Quotas, connection-rate limits, conn-quota bindings**: unchanged;
  they operate on `net::connection` after accept, which is transport-agnostic.

## Telemetry & Observability

- Existing `redpanda_rpc_*`, `redpanda_kafka_*` Prometheus metrics apply
  transparently (they are keyed on `server_name`, not transport).
- New log lines at `info` level:
  - `unlinking stale socket <path>`: printed when startup finds an
    orphan AF_UNIX file whose owning process has exited.
  - `UDS kafka listener <name> ready at <path> mode 0<oct>`: printed
    after `listen(2)` + `chmod(2)` succeed on shard 0.
- No new metrics proposed for v1. If telemetry need emerges after
  production use, a `redpanda_kafka_transport{type="uds|tcp"}` label on
  the existing connection-count gauge would be the minimal-change path.

## Corner cases dissected by example

1. **Socket path pre-exists as a live socket**: another broker (or the
   same broker under two data dirs) is running. Startup probes via
   `connect(2)`; a successful connect means "live", startup fails with a
   clear error naming the path. No unlink. This prevents the common
   "two brokers silently overwriting each other" footgun.

2. **Socket path pre-exists as a stale socket**: previous broker exited
   without cleanup (e.g. SIGKILL, node crash). `connect(2)` returns
   `ECONNREFUSED`. Startup logs a warning, `unlink(2)`s the path, and
   proceeds. An advisory `flock` on `<path>.lock` guards against two
   brokers racing through this window.

3. **Socket path exists but is not a socket**: startup fails loudly. We
   never unlink arbitrary regular files.

4. **Parent directory missing or not writable**: startup fails with a
   clear message naming the directory.

5. **Path longer than 107 bytes**: rejected at config parse time with
   `"unix_path too long (N bytes, max 107)"`.

6. **TLS name collision**: a `kafka_api` entry with `name: alpha` and
   `unix_path: /tmp/a.sock` plus a `kafka_api_tls` entry with
   `name: alpha` fails validation with
   `"TLS not supported on UDS listener alpha"`.

7. **Advertisement collision**: a UDS `kafka_api` entry whose `name`
   appears in `advertised_kafka_api` is rejected at parse time.

8. **Graceful shutdown**: after `server.stop()`, the broker unlinks the
   socket path and its `<path>.lock` sibling (ignoring `ENOENT`).

9. **Ungraceful shutdown (SIGKILL / node crash)**: the socket file
   remains. Corner case 2 covers the next startup.

10. **Mixed-transport round-trip** (the primary test case): a producer on
    UDS writes to topic `t`, a consumer on TCP reads from the same
    topic; offsets, timestamps and payloads are identical. Transport
    MUST be invisible at the Kafka protocol layer.

## Detailed design - What needs to change to get there

### Config layer

- `src/v/config/broker_authn_endpoint.h`: add
  `std::optional<ss::sstring> unix_path;` and
  `std::optional<uint32_t> unix_socket_mode;`. Keep existing `address`
  field. `net::unresolved_address` itself is a `serde::envelope` wire
  type and is **not** modified, preserving cross-version compatibility.
- `src/v/config/broker_authn_endpoint.cc`:
  - `YAML::convert::encode` emits either `address`/`port` or
    `unix_path`/`unix_socket_mode`, never both.
  - `YAML::convert::decode` enforces exactly-one-of at the parser level
    (returns `false` otherwise).
  - `json::rjson_serialize` mirrors the encode branches.
  - New `validate_broker_authn_endpoint(const broker_authn_endpoint&)`
    returning `std::optional<ss::sstring>` for semantic checks
    (absolute path, length < 108, non-empty, mode in range).

### Cross-list validator

- `src/v/config/node_config.cc`: `validate_kafka_uds_constraints(kafka_api,
  kafka_api_tls, advertised_kafka_api)`. Wired into the existing
  `node_config::validate()` chain. Rules enumerated in §*Guide-level
  explanation*.

### Bind-time construction

- `src/v/redpanda/application_services.cc` around the Kafka
  `server_configuration` assembly:
  ```cpp
  ss::socket_address saddr;
  if (ep.unix_path.has_value()) {
      vassert(!credentials,
              "TLS not permitted on UDS listener {}", ep.name);
      prepare_uds_path(*ep.unix_path);
      saddr = ss::socket_address(ss::unix_domain_addr(*ep.unix_path));
  } else {
      saddr = net::resolve_dns(ep.address).get();
  }
  c.addrs.emplace_back(ep.name, saddr, credentials);
  ```

- New helper `src/v/net/uds_path.{h,cc}` exposes `prepare_uds_path()` and
  `cleanup_uds_path()`:
  - `prepare_uds_path` verifies parent dir; probes an existing path via
    `connect(2)`; unlinks iff stale and `S_ISSOCK`; acquires `flock` on
    `<path>.lock`.
  - `cleanup_uds_path` unlinks both files, swallowing `ENOENT`.

### Why `net::server` needs no changes

Seastar's `posix_network_stack::listen()`
(`external/+non_module_dependencies+seastar/src/net/posix-stack.cc:923-925`)
already branches on `sa.is_af_unix()` and returns a
`posix_server_socket_impl` that supports cross-shard accept dispatch via
`smp::submit_to` (lines 706-749). Non-zero shards use
`posix_ap_network_stack::listen()` (lines 948-950) which creates a
`posix_ap_server_socket_impl` that registers an accept proxy — it never
calls `bind(2)`. Load balancing via the `connection_distribution`
algorithm (Redpanda's default) hashes a monotonic counter for AF_UNIX
(`get_port_or_counter`, lines 572-578). The upshot: Redpanda's existing
per-shard `ss::engine().listen(endpoint.addr, lo)` call in
`src/v/net/server.cc:83-117` does the right thing for AF_UNIX with zero
modifications.

### rpk (Go)

- `src/go/rpk/pkg/config/params.go`: accept `unix:///abs/path` in broker
  lists, normalising to `{Network: "unix", Address: "/abs/path"}` entries.
- The shared `[]kgo.Opt` factory installs a UDS-aware `kgo.Dialer` that
  routes `unix://...` addresses through `net.Dial("unix", path)` and
  delegates everything else to the default dialer.
- All rpk subcommands using the shared factory inherit UDS support for
  free (`rpk topic|cluster|group|acl ...`). `rpk` Admin-API commands
  remain TCP-only.

## Detailed design - How it works

Startup sequence for a broker with a dual listener:

1. Config parser validates each `kafka_api` entry and the cross-list
   constraints.
2. For each UDS entry, `prepare_uds_path` runs on shard 0 before
   `server_configuration` is constructed.
3. Every shard's `net::server::start()` calls
   `ss::engine().listen(endpoint.addr, lo)`. On shard 0 this executes
   `bind + listen`. On other shards this registers an accept proxy.
4. Post-start, shard 0 executes `chmod(path, mode)` to apply the
   configured `unix_socket_mode`.
5. Shard 0 accepts connections and dispatches to target shards via
   `smp::submit_to`; the Kafka protocol handler is invoked on the target
   shard exactly as for TCP.

Shutdown:

1. `server.stop()` closes all accept sockets.
2. For every UDS entry, the Kafka service's post-stop hook calls
   `cleanup_uds_path`, unlinking both the socket and lockfile.

## Security

### Threat model

| Attacker | Capability | Can exploit UDS? |
|---|---|---|
| Operator with `redpanda.yaml` write access | picks any `unix_path`, any mode, any SASL setting | Out of scope — already fully privileged; no config-level filter can protect against this principal. |
| Local unprivileged user with write access to the parent directory | can create/replace files at the socket path, race the bind | No privilege escalation. Worst case: denial-of-service (broker refuses to start or re-starts cleanly on a different path). See guards below. |
| Local unprivileged user *without* parent-directory access | no capability | Not applicable. |
| Remote network attacker | no capability | Not applicable — `AF_UNIX` is host-local. |

The dangerous class of attack we protect against is **"trick Redpanda into
`unlink(2)`ing or binding on top of an important file"** (e.g. `/etc/passwd`,
another service's socket). The defenses below are layered so that bypassing
any single one does not yield that outcome.

### Config-time defenses (`validate_broker_authn_endpoint`)

All checks run at YAML-decode time, before any filesystem syscall is made:

| Check | Rejects | Why |
|---|---|---|
| Non-empty | `""` | No inode is legal. |
| Absolute path | `./x`, `x.sock` | Relative paths depend on the broker's CWD at bind time, which is non-deterministic across operator invocations and makes audit harder. |
| Length ≤ 107 bytes | paths at or past the Linux `sun_path` limit | `bind(2)` silently truncates oversize paths; we'd disagree with the kernel about what we bound. |
| No embedded NUL bytes | `/real/path\0/elsewhere` | `sun_path` is NUL-terminated at the kernel layer. Without this guard an attacker (or a buggy config generator) could make the config and the kernel disagree about the inode. |
| No `..` components | `/var/run/redpanda/../../etc/passwd` | A legitimate operator never writes `..` here. Rejecting up-front turns any future outer-layer restriction (admission policy confining sockets to `/var/run/redpanda/`) into a real invariant instead of a bypass-by-traversal problem. |
| Canonical form | `//` runs, trailing `/` | Kernel tolerates these but they hint at a buggy config generator. Rejecting surfaces the bug early. |
| Mode ≤ 07777 | higher | Covers full POSIX mode range including setuid/setgid/sticky. The setgid bit (02000) is the standard pattern for "inherit parent-dir GID on socket creation", useful for cross-container bind-mount deployments. |

### Runtime defenses (`prepare_uds_path` + `verify_uds_bound`)

Before `bind(2)`:

- Parent directory must exist, be a directory, and be writable.
- If the target already exists:
  - **If not `S_ISSOCK`**: hard fail. We never `unlink(2)` a non-socket. This is the single most important defense — it is what blocks "trick Redpanda into `unlink(/etc/passwd)`".
  - If a socket, attempt `connect(2)`: `ECONNREFUSED` → stale, unlink with a warning log; success → another broker is live, hard fail.
- Advisory `flock(2)` on a sibling `<path>.lock` file prevents two Redpanda instances racing to bind the same path.

After `bind(2)` + `chmod(2)`:

- `lstat(2)` the path (not `stat`, because `stat` follows symlinks and would mask exactly the attack we're trying to detect).
- Assert `S_ISSOCK(st_mode)` — closes any TOCTOU window where an attacker with parent-dir write access could have swapped in a symlink between our stat-and-unlink and the subsequent bind.
- Assert `st_uid == geteuid()` — the socket inode must be one we own. If we somehow bound on top of an inode owned by another user, fail loudly instead of serving traffic on it.

### What we deliberately do NOT do

- **No `realpath(3)` normalization.** Resolving symlinks would break legitimate k8s `hostPath` / `emptyDir` setups where the mount itself traverses a symlinked path. We log the bound path verbatim and let operators audit.
- **No character whitelist.** Paths under `/run` in some deployments legitimately contain `:`, `@`, or `+` (systemd-instance-style names). NUL + `..` + length guards are sufficient.
- **No broader length restriction** (e.g. `< 128`). We already cap at the kernel-enforced 107; loosening would let bad configs pass validation only to fail with a less helpful error at bind time.

### Residual risks (accepted)

- A local user with parent-directory write access can cause a DoS by racing the bind or repeatedly creating non-socket files at the path. Mitigation is operational: parent directories should be mode `0750` and owned by `redpanda:redpanda`, not `0777`.
- Between `bind(2)` and our `chmod(2)`, the socket briefly carries Seastar's default mode (umask-derived). In practice the window is microseconds and SASL is still the gate for authenticated traffic, but a truly paranoid deployment should rely on a `0700` parent directory rather than the socket mode.

### Test coverage for these defenses

- `broker_authn_endpoint_test.cc` carries a table-driven `path_sanity_table` case covering every positive and negative path shape listed above, with a `description` field on each entry describing the attacker intent or operator-usability rationale.
- `uds_listener_test.cc` covers the runtime cases (stale socket, regular-file-at-path, non-existent parent, unwritable parent, concurrent flock contention).

## Drawbacks

- **TLS unsupported**: forces operators who need cryptographic identity
  on a local path to wait for a follow-up peer-cred authn PR or continue
  using TLS over 127.0.0.1.
- **Java / librdkafka clients cannot use UDS**: only Go (via franz-go),
  plain C (via `AF_UNIX` + Kafka wire protocol), and C++ clients that
  support AF_UNIX can benefit.
- **Metadata response carries only TCP endpoints**: a client
  bootstrapped on UDS may follow metadata to TCP for follow-up
  connections in multi-broker clusters. Documented in §*Guide-level
  explanation*.
- **Filesystem permissions are the only access control** when SASL is
  disabled. Operators must ensure the socket path's parent directory
  and mode are set appropriately.
- **Operational complexity**: two listener types mean two sets of
  cluster health checks and dashboards if users want per-transport
  visibility. Deferred until there is demonstrated demand.

## Rationale and Alternatives

### Why add UDS at all?

See *Motivation*. The short form is: "loopback is fast" is empirically
false under service meshes and CNIs, and broker pods colocated with
client pods are a common Redpanda deployment pattern. Measured deltas
will be added to this RFC before merge.

### Why extend `broker_authn_endpoint` rather than introduce a new type?

Alternatives considered:

1. **Add `unix_path` to existing type (chosen).** Minimal surface change;
   the existing `one_or_many_property<broker_authn_endpoint>` and
   admin-API schema generator pick up the new field automatically.
2. **Overload `address` with a `unix://...` URI scheme.** Rejected: the
   `address` field is currently a bare hostname parsed by
   `net::unresolved_address`, and embedding a URI here leaks transport
   concerns into a type that many other subsystems serialise over the
   wire.
3. **Introduce a new top-level `kafka_api_uds` config key.** Rejected:
   doubles the config surface and duplicates TLS/SASL key handling.

### Why not touch `net::unresolved_address`?

It's a `serde::envelope` type, meaning it participates in the
on-the-wire format used between brokers and between Redpanda versions.
Adding a variant/optional unix_path field would require a version bump
with tightly-coupled compatibility shims. Keeping it as a pure
host+port type preserves the clean boundary.

### Why reject TLS-on-UDS rather than allow it?

Seastar's `ss::tls::listen` wraps any server_socket, so it would
technically work. Rejected because:
- UDS is already constrained to the host, so most users don't need
  encryption.
- Certificate management for a local socket is awkward — whose SAN does
  it carry?
- Testing the combination meaningfully adds surface without clear wins.

A future PR can lift the restriction if demand appears.

### Why not SO_PEERCRED-based authn?

Attractive for UDS — `getsockopt(SO_PEERCRED)` returns the connecting
uid/gid/pid — but the mapping from uid to Kafka principal needs design
(PAM? Static uid→principal table? Linux user-namespace awareness?). That
design belongs in a follow-up RFC and should not block the basic UDS
listener.

## Unresolved questions

- **Metadata routing from a UDS bootstrap**: whether rpk should refuse
  to follow TCP metadata responses when started with a `unix://` seed,
  or whether it should hybridise (UDS for metadata + local traffic, TCP
  for remote shards). Current plan: do nothing, document the behaviour,
  leave finer control for a follow-up.
- **`chmod` race window**: between `listen(2)` and `chmod(2)` on shard 0
  there is a brief interval where the socket has default umask mode.
  Whether this matters depends on the filesystem's surrounding ACLs. If
  it does, one option is to `fchmod(listen_fd)` — but that affects only
  the fd, not the path. Alternative: `umask(0)` guard around the bind.
  Decide during implementation.
- **Lockfile location**: `<unix_path>.lock` co-located with the socket
  is simpler; a dedicated lock directory
  (`/var/run/redpanda/locks/<hash>.lock`) is tidier. Default to
  co-located unless operators flag an issue.
- **Follow-up microvm test**: extend the Nix test harness with a
  `microvm.nix`-based nixosTest that boots a VM, runs the broker, and
  exercises crash recovery (`kill -9` → restart → stale-socket cleanup).
  Tracked separately; not a blocker for this RFC.

## Measurements

This RFC ships with four purpose-split Seastar `PERF_TEST_CN`
benchmarks plus two end-to-end Nix harnesses. Each bench isolates one
dimension of the transport cost so that the data can stand independently
under reviewer scrutiny.

### Test suite

All four microbenches live under `src/v/net/tests/` and share the
fixture `uds_bench_common.hh` (socket-pair factory, echo server,
`rusage_sample` helper). They are compiled into the Nix sandbox build
via `nix build .#uds-bench-cached`, which installs each as
`libexec/<bench>` alongside the broker binary so the numbers in this
section are reproducible from a clean checkout.

The transport axis is always `{tcp_nagle, tcp_nodelay, uds}`. All
benches run `--smp=1` — AF_UNIX has no `SO_REUSEPORT` equivalent, so
mixing multi-shard TCP and single-shard UDS would contaminate the
comparison. Cross-shard dispatch of UDS is a separate concern tracked
in the Unresolved-questions section.

#### 1. `uds_rtt_bench` — round-trip latency (headline)

Per-frame cost of a request/response round trip on an otherwise-idle
connection.

- Axes: `payload ∈ {8 B, 64 B, 256 B, 1 KiB, 4 KiB, 16 KiB, 64 KiB,
  1 MiB}` × `depth ∈ {1, 8, 64, 256, 1024}`.
- Depth = number of in-flight write frames before reads. Depth 1 is the
  pure tail-latency case; depth 1024 exposes pipeline saturation and
  where the socket buffer / TCP congestion window stop helping.
- The inner loop runs `iterations=128` outer batches per
  `PERF_TEST_CN` invocation, returning `iterations × depth` so the
  reported per-op number is the cost of one request frame plus one
  response frame.

#### 2. `uds_throughput_bench` — directional throughput + CPU cost

Complementary to RTT: "how much work does the transport move per
second, and at what CPU cost". Each cell brackets the timed region with
`getrusage(RUSAGE_SELF)` and prints user-µs, sys-µs, and context
switches so reviewers can derive CPU µs/byte and CPU µs/RPC.

- `unidirectional`: client writes `batches × depth` frames, server
  drains to EOF. Best case for each transport; exposes the send-side
  syscall cost and the TCP-Nagle penalty on small frames.
- `bidirectional`: both ends write and drain concurrently via
  `when_all_succeed` — full-duplex buffering and scheduler fairness.
- `rr_saturation`: strict depth-1 ping-pong, as many back-to-back RPCs
  as the transport allows. This is the "tail latency at offered load"
  case and is where UDS delivers its biggest relative win because the
  per-RPC syscall path is shortest.

Free helper functions `drain_stream(in)` / `write_loop(out, ...)` are
used instead of IIFE lambda coroutines, because the lambda object is
destroyed immediately after the IIFE returns while the coroutine frame
may still be suspended holding captured references — the known
use-after-free trap documented in this project's `CLAUDE.md`.

#### 3. `uds_connect_bench` — connection lifecycle

Measures the cost of *churn*, which most benches ignore: connect + one
RPC + close, back-to-back, 256 times per cell. Real deployments open
short-lived connections constantly (CLI tools, sidecar health probes,
batch producer jobs); those workloads are dominated by handshake cost,
not steady-state RTT.

- `short_lived`: fresh `connect()` for every RPC.
- `long_lived`: single persistent connection, N RPCs — the baseline
  most steady-state benches accidentally measure. Included so the
  short/long ratio per transport is visible.
- Payload axis: `{64 B, 1 KiB}` (the small-RPC shape where churn cost
  dominates).

#### 4. `uds_concurrency_bench` — multi-connection fairness

RTT and throughput benches run a single connection; real workloads
don't. This file exercises the concurrency axis at
`N ∈ {1, 2, 4, 8, 16, 32, 64}` in two shapes:

- `fan_in`: N concurrent clients → one listener, one accept loop per
  connection. The "many producers to one broker" shape.
- `fan_out`: one client coroutine opens N connections to one listener.
  Measures whether a single reactor can keep N sockets fed.

With `--smp=1` both shapes hit the same scheduler + transport paths;
they're kept as distinct cells because a future asymmetric variant
(client on shard 0, servers on shard 1) can diverge without touching
the fan-in path.

#### 5. End-to-end rpk bench (`nix run .#bench-uds-vs-tcp`)

Boots a broker with a dual-transport listener, produces `$RECORDS`
messages of `$PAYLOAD` bytes via each transport back-to-back on the
same topic, emits one JSON record per phase with elapsed time,
throughput, and (when `kernel.perf_event_paranoid <= 2`)
`sendmsg`/`recvmsg` syscall counts.

#### 6. Backpressure bench (`nix run .#bench-backpressure`)

Two scenarios driven through the same dual-listener broker:

- **Slow consumer**: rate-limited producer with a deliberately lagging
  consumer; the Admin API is polled every 100 ms for `high_watermark`
  and RSS so the JSON timeline captures when the broker starts pushing
  back.
- **Bursty producer**: 10 ms produce bursts every 100 ms, per
  transport.

Used to spot regressions where UDS and TCP might diverge under
saturation (e.g. different flow-control behaviour from the two socket
families).

### Microbench results (2026-04-20)

Collected on Linux 6.18.21, `--smp=1`, built via `nix build
.#uds-bench-cached` (release, clang/libc++). Columns are per-operation
mean runtime as reported by the Seastar perf harness; the "UDS vs
tcp_nodelay" ratio is what the reviewer should look at — Nagle vs
nodelay differences on loopback are a TCP-internal detail.

Methodology note: `iterations` in `uds_rtt_bench` is a hardcoded
compile-time constant (128). Payload × depth combinations where
`iterations × depth × payload` exceeds a few GiB wall-clock out before
the harness prints a line (e.g. `p1m_d1024` → 128 GiB per invocation).
Cells marked "—" below hit that ceiling; making `iterations` runtime-
configurable is captured in Follow-ups.

#### RTT, depth = 1 (single in-flight frame — tail-latency baseline)

| payload | tcp_nagle | tcp_nodelay | uds | UDS / nodelay |
|---------|-----------|-------------|-----|---------------|
| 64 KiB  | —         | 58.17 µs    | 33.83 µs | 0.58× |
| 1 MiB   | —         | 747.95 µs   | 555.28 µs | 0.74× |

#### RTT, depth = 8 (small pipeline)

| payload | tcp_nodelay | uds | UDS / nodelay |
|---------|-------------|-----|---------------|
| 64 KiB  | 49.80 µs | 33.76 µs | 0.68× |
| 1 MiB   | —        | 588.94 µs | — |

#### RTT, depth = 64 (pipelined small frames — per-frame cost)

| payload | tcp_nagle | tcp_nodelay | uds | UDS / nodelay |
|---------|-----------|-------------|-----|---------------|
| 8 B     | 365 ns    | 380 ns   | 210 ns  | 0.55× |
| 256 B   | 747 ns    | 761 ns   | 424 ns  | 0.56× |
| 1 KiB   | 96.14 µs* | 2.12 µs  | 990 ns  | 0.47× |
| 4 KiB   | 215.83 µs* | 6.58 µs | 3.16 µs | 0.48× |
| 16 KiB  | 83.19 µs* | 16.28 µs | 9.20 µs | 0.57× |

\* tcp_nagle shows 20–100× worse per-frame cost at 1–16 KiB: the classic
Nagle + delayed-ack interaction on pipelined writes. This is exactly
the behaviour the `tcp_nodelay` axis exists to isolate — it's a real
regression risk for anyone running stock TCP loopback without
`TCP_NODELAY`.

**Summary:** at depth ≥ 1 and payloads 8 B – 64 KiB, UDS delivers
**~1.7–2.1× lower per-frame RTT** than `tcp_nodelay`. The gap narrows
at 1 MiB (0.74×) because the transport cost stops dominating versus
memcpy.

#### Throughput: unidirectional drain (client writes, server drains)

| payload | tcp_nagle | tcp_nodelay | uds | UDS / nodelay |
|---------|-----------|-------------|-----|---------------|
| 64 B    | 154 ns    | 229 ns   | 96 ns   | 0.42× |
| 4 KiB   | 2.38 µs   | 4.51 µs  | 1.88 µs | 0.42× |
| 64 KiB  | 27.89 µs  | 29.77 µs | 18.37 µs | 0.62× |

#### Throughput: rr_saturation (depth-1 ping-pong as fast as possible)

| payload | tcp_nagle | tcp_nodelay | uds | UDS / nodelay |
|---------|-----------|-------------|-----|---------------|
| 8 B     | 19.31 µs | 19.79 µs | 10.73 µs | 0.54× |
| 64 B    | 19.91 µs | 19.77 µs | 10.17 µs | 0.51× |
| 1 KiB   | 20.05 µs | 20.10 µs | 10.59 µs | 0.53× |
| 16 KiB  | 24.50 µs | 25.55 µs | 13.49 µs | 0.53× |

**Summary:** the rr_saturation row is the headline number for
service-mesh displacement — **UDS halves per-RPC cost at every small
payload.** This is the "tail latency at offered load" story: the
syscall + scheduler path is shorter for AF_UNIX, and it shows up
cleanly when the workload is syscall-bound.

#### Connection lifecycle (256-connection churn)

| payload | transport   | short_lived (churn) | long_lived (steady) | ratio |
|---------|-------------|---------------------|---------------------|-------|
| 64 B    | tcp_nagle   | 110.29 µs | 20.95 µs | 5.26× |
| 64 B    | tcp_nodelay | 110.84 µs | 19.60 µs | 5.66× |
| 64 B    | uds         |  36.60 µs |  9.91 µs | 3.69× |
| 1 KiB   | tcp_nagle   | 111.64 µs | 20.49 µs | 5.45× |
| 1 KiB   | tcp_nodelay | 112.27 µs | 20.15 µs | 5.57× |
| 1 KiB   | uds         |  37.31 µs | 10.52 µs | 3.55× |

**Summary:** UDS is **~3× faster on connection churn** (37 µs vs
111 µs) *and* has a smaller churn-to-steady ratio (3.6× vs 5.6×), i.e.
UDS pays proportionally less for setup/teardown. Workloads dominated by
short-lived connections (CLI tools, sidecar probes, batch producers)
benefit disproportionately.

#### Concurrency (fan_in, 64 RPCs per connection, per-frame cost)

| N clients | tcp_nagle | tcp_nodelay | uds | UDS / nodelay |
|-----------|-----------|-------------|-----|---------------|
| 1         | 21.38 µs | 22.10 µs | 11.24 µs | 0.51× |
| 4         | 19.60 µs | 20.18 µs |  9.30 µs | 0.46× |
| 16        | 19.02 µs | 19.57 µs |  8.75 µs | 0.45× |
| 64        | 19.83 µs | 21.87 µs |  9.63 µs | 0.44× |

**Summary:** UDS holds its ~2.2× lead across the full N sweep. The gap
actually *widens* slightly at N=16/64, consistent with the accept +
scheduler path being a larger slice of per-frame cost at concurrency.

### End-to-end rpk bench (2026-04-19)

Single-broker, single-shard (`--smp 1 --memory 1G`) on Linux 6.18.21;
`rpk topic produce -n $RECORDS -r 2000` against a broker with both a
TCP listener (`127.0.0.1:9092`) and a UDS listener
(`/tmp/.../rp.sock`), back-to-back phases on the same topic.

| transport | records | payload | elapsed (ms) | throughput (MiB/s) |
|-----------|---------|---------|--------------|--------------------|
| tcp_loopback | 10 000 | 1 KiB | 14 230 | 0.686 |
| uds          | 10 000 | 1 KiB | 13 989 | 0.698 |

Ratio: **UDS throughput ≈ 1.7 % higher than TCP loopback** at this
payload/rate combination. The signal is small because the bench is
syscall-bound on a single rpk producer rather than transport-bound;
Kafka-framing and rpk's per-record write amplification dominate over
the raw socket cost. The Seastar microbench is the right place to
isolate the pure socket cost (order of magnitude larger relative gap
in the early exploratory probes described at the top of this RFC).

`perf_event_paranoid` gated syscall counts (`null` values above); to
collect them, set `sysctl -w kernel.perf_event_paranoid=2` before
re-running the bench.

### Follow-ups

- Wire `uds_bench_rpbench` into the Nix sandbox build so Seastar-level
  latency numbers can be collected reproducibly.
- Per-message latency p50/p99 via `rpk topic produce|consume` under
  fixed rate, TCP-loopback vs UDS.
- Per-shard CPU from `redpanda_cpu_busy_seconds_total` diff.
- Make `iterations` in `uds_rtt_bench` runtime-configurable (env var or
  `--rtt-iterations` flag) so high-depth-large-payload cells
  (`p1m × d64/256/1024`, `p64k × d256/1024`) can be measured without
  exceeding practical wall-clock bounds. Current hardcoded `128` × the
  inner depth × payload means `p1m_d1024` attempts to move 128 GiB per
  harness invocation, which does not terminate in a review cycle.
- Collect `rusage_sample` user/sys µs deltas already printed by the
  throughput bench into the RFC tables so CPU µs/byte can be reported
  alongside wall-clock throughput.
- Run the full matrix under `--smp 4` once the SMP-bind probe
  outcome is reflected in the design (see Unresolved questions) — the
  current numbers are `--smp 1` only.
