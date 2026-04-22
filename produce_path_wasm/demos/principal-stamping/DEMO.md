# Demo: Principal Stamping on the Produce Path

Records produced to Redpanda are stamped with the authenticated
principal's identity as a record header -- before replication, by a
WASM transform running inline on the produce path. The header is
unforgeable: the broker sets it, not the client.

## Setup

### 1. Build Redpanda

```bash
cd ~/co/rp1
bazel build --config=fastbuild //:redpanda
```

### 2. Build the transform

```bash
cd ~/co/rp1/produce_path_wasm/demos/principal-stamping
rpk transform build
```

This produces `principal-stamper.wasm` in the current directory.

### 3. Start a dev cluster

In a separate terminal, wipe any previous demo data and start the
cluster with SASL and produce-path transforms enabled.

### 4. Enable SASL auth and create a user

Create the `demo` user and mark it as a superuser.

### 5. Verify connectivity

Confirm the profile is wired up correctly.

---

## Setup (copy/paste)

Separate terminal, from the repo root:

```bash
rm -rf produce_path_wasm/demos/principal-stamping/data
bazel run --config=fastbuild //tools:dev_cluster -- --nodes 1 \
  -d produce_path_wasm/demos/principal-stamping/data \
  --config-overrides '{
    "data_transforms_enabled": true,
    "data_transforms_produce_path_enabled": true,
    "enable_sasl": true
  }' \
  -- -m 4G -c 1
```

Once the cluster is up, in the demo terminal:

```bash
rpk acl user create demo --password demo123 --mechanism SCRAM-SHA-256
rpk cluster config set superusers '["demo"]'
rpk cluster info
```

---

## Terminal Layout

Two panes side by side (tmux: `Ctrl-b %`, iTerm: `Cmd-D`):

- **Left pane**: commands
- **Right pane**: consumer

---

## Script

### 1. Show the SASL users

**Left pane:**

```bash
rpk acl user list
```

Narration: "The cluster has SASL authentication enabled. Here are the
configured users -- any producer has to authenticate as one of these
before it can write."

### 2. Create the topic

**Left pane:**

```bash
rpk topic create events -p 1
```

### 3. Start the consumer

**Right pane:**

```bash
rpk topic consume events -f 'key=%k value=%v headers=%h{%k:%v }\n'
```

### 4. Produce a record WITHOUT the transform

**Left pane:**

```bash
echo '{"event":"login","user":"alice"}' | rpk topic produce events -k alice
```

**Right pane shows:** Record with no headers.

### 5. Deploy the transform

**Left pane:**

```bash
rpk transform deploy
```

(Uses `transform.yaml` in the current directory.)

### 6. Wait for the engine, then produce

Wait ~3 seconds for the WASM engine to compile. Then:

**Left pane:**

```bash
echo '{"event":"purchase","item":"widget"}' | rpk topic produce events -k bob
echo '{"event":"logout","user":"alice"}' | rpk topic produce events -k alice
```

### 7. Observe the difference

**Right pane shows:**

```
key=alice value={"event":"login","user":"alice"} headers=
key=bob value={"event":"purchase","item":"widget"} headers=principal:demo
key=alice value={"event":"logout","user":"alice"} headers=principal:demo
```

The pre-transform record has no header. Post-transform records have
`principal:demo` -- the authenticated SASL user.

### 8. Key point

"The principal header was set by the broker, not the client. There's
no way for a producer to forge this -- it's injected inline during
the produce request, before replication."

### 9. Clean up

```bash
rpk transform delete principal-stamper --no-confirm
rpk topic delete events
```
