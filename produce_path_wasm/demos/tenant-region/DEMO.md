# Demo: Tenant-Region Entitlement on the Produce Path

Records produced to Redpanda are accepted or rejected inline based on
a commercial contract between tenant and data-residency region. The
contract is evaluated by a WASM transform running on the produce path,
before replication, using two signals the broker attests to -- the
authenticated principal and the listener the client connected to.
Neither signal can be forged by the client.

## Setup

### 1. Build Redpanda

```bash
cd ~/co/rp1
bazel build --config=fastbuild //:redpanda
```

### 2. Build the transform

```bash
cd ~/co/rp1/produce_path_wasm/demos/tenant-region
rpk transform build
```

This produces `tenant-region.wasm` in the current directory.

### 3. Start a dev cluster

In a separate terminal, wipe any previous demo data and start the
cluster with two named SASL listeners (one per region) and produce-path
transforms enabled.

### 4. Enable SASL auth and create the tenant users

Create `acme-us`, `acme-eu`, and `acme-global`; make them superusers so
they can produce without additional ACL setup.

### 5. Verify connectivity

Confirm you can reach both listeners.

---

## Setup (copy/paste)

Separate terminal, from the repo root:

```bash
rm -rf produce_path_wasm/demos/tenant-region/data
bazel run --config=fastbuild //tools:dev_cluster -- --nodes 1 \
  -d produce_path_wasm/demos/tenant-region/data \
  --config-overrides '{
    "kafka_api": [
      {"name": "us-listener", "address": "0.0.0.0", "port": 9094, "authentication_method": "sasl"},
      {"name": "eu-listener", "address": "0.0.0.0", "port": 9093, "authentication_method": "sasl"}
    ],
    "advertised_kafka_api": [
      {"name": "us-listener", "address": "127.0.0.1", "port": 9094},
      {"name": "eu-listener", "address": "127.0.0.1", "port": 9093}
    ],
    "data_transforms_enabled": true,
    "data_transforms_produce_path_enabled": true
  }' \
  -- -m 4G -c 1
```

Once the cluster is up, in the demo terminal enable SASL and create
the tenant users:

```bash
rpk -X brokers=127.0.0.1:9094 -X admin.hosts=127.0.0.1:9644 \
  cluster config set enable_sasl true
rpk -X brokers=127.0.0.1:9094 -X admin.hosts=127.0.0.1:9644 \
  acl user create acme-us --password demo123 --mechanism SCRAM-SHA-256
rpk -X brokers=127.0.0.1:9094 -X admin.hosts=127.0.0.1:9644 \
  acl user create acme-eu --password demo123 --mechanism SCRAM-SHA-256
rpk -X brokers=127.0.0.1:9094 -X admin.hosts=127.0.0.1:9644 \
  acl user create acme-global --password demo123 --mechanism SCRAM-SHA-256
rpk -X brokers=127.0.0.1:9094 -X admin.hosts=127.0.0.1:9644 \
  cluster config set superusers '["acme-us","acme-eu","acme-global"]'
```

### Convenience aliases

Paste these into your shell so the script below stays readable. Each
alias pins a (tenant, listener) pair:

```bash
alias rpk_admin='rpk -X brokers=127.0.0.1:9094 -X admin.hosts=127.0.0.1:9644 -X sasl.mechanism=SCRAM-SHA-256 -X user=acme-global -X pass=demo123'

alias rpk_acme_us_on_us='rpk -X brokers=127.0.0.1:9094 -X admin.hosts=127.0.0.1:9644 -X sasl.mechanism=SCRAM-SHA-256 -X user=acme-us -X pass=demo123'
alias rpk_acme_us_on_eu='rpk -X brokers=127.0.0.1:9093 -X admin.hosts=127.0.0.1:9644 -X sasl.mechanism=SCRAM-SHA-256 -X user=acme-us -X pass=demo123'

alias rpk_acme_eu_on_us='rpk -X brokers=127.0.0.1:9094 -X admin.hosts=127.0.0.1:9644 -X sasl.mechanism=SCRAM-SHA-256 -X user=acme-eu -X pass=demo123'
alias rpk_acme_eu_on_eu='rpk -X brokers=127.0.0.1:9093 -X admin.hosts=127.0.0.1:9644 -X sasl.mechanism=SCRAM-SHA-256 -X user=acme-eu -X pass=demo123'

alias rpk_acme_global_on_us='rpk -X brokers=127.0.0.1:9094 -X admin.hosts=127.0.0.1:9644 -X sasl.mechanism=SCRAM-SHA-256 -X user=acme-global -X pass=demo123'
alias rpk_acme_global_on_eu='rpk -X brokers=127.0.0.1:9093 -X admin.hosts=127.0.0.1:9644 -X sasl.mechanism=SCRAM-SHA-256 -X user=acme-global -X pass=demo123'
```

---

## Terminal Layout

Two panes side by side (tmux: `Ctrl-b %`, iTerm: `Cmd-D`):

- **Left pane**: commands
- **Right pane**: consumer

---

## Script

### 1. Create the topic

**Left pane:**

```bash
rpk_admin topic create events -p 1
```

### 2. Deploy the transform

**Left pane:**

```bash
rpk_admin transform deploy
```

(Uses `transform.yaml` in the current directory.) Wait ~3 seconds for
the WASM engine to compile before producing.

### 3. Start the consumer

**Right pane:**

```bash
rpk_admin topic consume events -f 'offset=%o key=%k headers=%h{%k=%v }\n'
```

### 4. Walk the scenarios

Each row is one produce attempt. The left-pane column shows what the
producer sees (OK means the produce RPC succeeded; INVALID_RECORD
means the broker rejected the record). The right-pane column shows
what (if anything) makes it into the log.

| # | Command | Expected left pane | Expected right pane |
|---|---------|--------------------|---------------------|
| 1 | `echo 'event1' \| rpk_acme_us_on_us topic produce events -k k1` | OK | `offset=0 key=k1 headers=tenant=acme-us residency=us` |
| 2 | `echo 'event2' \| rpk_acme_us_on_eu topic produce events -k k2` | INVALID_RECORD | (nothing) |
| 3 | `echo 'event3' \| rpk_acme_eu_on_us topic produce events -k k3` | INVALID_RECORD | (nothing) |
| 4 | `echo 'event4' \| rpk_acme_global_on_us topic produce events -k k4` | OK | `offset=1 key=k4 headers=tenant=acme-global residency=us` |
| 5 | `echo 'event5' \| rpk_acme_global_on_eu topic produce events -k k5` | OK | `offset=2 key=k5 headers=tenant=acme-global residency=eu` |

Rows 1, 4, and 5 are contract-satisfying: the tenant's entitlement
covers the region advertised by the listener, so the transform stamps
`tenant` and `residency` headers and lets the write through. Rows 2
and 3 are contract-violating: `acme-us` tried to write via the EU
listener, and `acme-eu` tried to write via the US listener. The
transform returned an error, and the broker rejected the batch with
INVALID_RECORD before it ever hit the log.

### 5. Key point

"The broker enforced a commercial contract at the produce path. Two
pieces of information decided whether to accept the batch: the
authenticated principal and the listener the client connected to.
Both are attested by the broker -- the client cannot spoof either.
The transform just read them from the metadata bag and applied a
policy. Policy violations never replicated, never consumed disk, and
never showed up for downstream consumers. A misbehaving or compromised
client cannot punch through the contract by setting a header or
editing the record, because the enforcement is upstream of the
record's content entirely."

### 6. Clean up

```bash
rpk_admin transform delete tenant-region --no-confirm
rpk_admin topic delete events
```
