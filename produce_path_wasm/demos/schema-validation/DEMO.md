# Demo: Schema Validation on the Produce Path

Records that don't conform to the registered Avro schema are rejected
before they're written -- the invalid data never hits the log. The
transform fetches the schema from the schema registry by ID, so
schema updates are picked up automatically.

## Setup

### 1. Build Redpanda

```bash
cd ~/co/rp1
bazel build --config=fastbuild //:redpanda
```

### 2. Build the transform

```bash
cd ~/co/rp1/produce_path_wasm/demos/schema-validation
rpk transform build
```

This produces `schema-validator.wasm` in the current directory.

### 3. Start a dev cluster

In a separate terminal:

```bash
cd ~/co/rp1
bazel run --config=fastbuild //tools:dev_cluster -- --nodes 1 \
  -d produce_path_wasm/demos/schema-validation/data \
  --config-overrides '{
    "data_transforms_enabled": true,
    "data_transforms_produce_path_enabled": true
  }' \
  -- -m 4G -c 1
```

### 4. Verify connectivity

```bash
rpk cluster info
```

---

## Terminal Layout

Two panes side by side (tmux: `Ctrl-b %`, iTerm: `Cmd-D`):

- **Left pane**: commands
- **Right pane**: consumer

---

## Script

### 1. Show the Avro schema

**Left pane:**

```bash
cat avro/schema.avsc
```

Output:

```json
{
  "type": "record",
  "name": "Example",
  "fields": [
    {"name": "a", "type": "long", "default": 0},
    {"name": "b", "type": "string", "default": ""}
  ]
}
```

Narration: "This is our Avro schema -- a record with a long field 'a'
and a string field 'b'. Let's register it in the schema registry."

### 2. Create the topic and register the schema

**Left pane:**

```bash
rpk topic create sensor-data -p 1
rpk registry schema create sensor-data-value --schema avro/schema.avsc
```

### 3. Start the consumer

**Right pane:**

```bash
rpk topic consume sensor-data -f 'offset=%o key=%k\n'
```

### 4. Deploy the schema validation transform

**Left pane:**

```bash
rpk transform deploy
```

(Uses `transform.yaml` in the current directory.)

Wait ~3 seconds for the WASM engine to compile.

### 5. Produce a valid Avro record

**Left pane:**

```bash
echo '{"a": 42, "b": "hello"}' | rpk topic produce sensor-data -k valid1 --schema-id=topic
```

rpk encodes the JSON as Avro using the registered schema and adds
the Confluent wire format header automatically.

**Right pane shows:** `offset=0 key=valid1`

### 6. Produce another valid record

```bash
echo '{"a": 99, "b": "world"}' | rpk topic produce sensor-data -k valid2 --schema-id=topic
```

**Right pane shows:** `offset=1 key=valid2`

### 7. Produce an INVALID record -- plain text

```bash
echo 'this is not avro' | rpk topic produce sensor-data -k invalid1
```

**Left pane shows:** `INVALID_RECORD` -- rejected.

**Right pane shows:** Nothing new.

### 8. Produce a record with the WRONG Avro schema

Register a different schema under a separate subject, then produce
with it. The record is valid Avro, but it doesn't match the schema
the transform expects.

```bash
cat > /tmp/wrong.avsc << 'EOF'
{
  "type": "record",
  "name": "Wrong",
  "fields": [
    {"name": "x", "type": "double"},
    {"name": "y", "type": "double"}
  ]
}
EOF
rpk registry schema create sensor-data-wrong --schema /tmp/wrong.avsc
```

Now produce with the wrong schema:

```bash
echo '{"x": 1.0, "y": 2.0}' | rpk topic produce sensor-data -k invalid2 --schema-id=2
```

(Use the schema ID returned by the `create` command above. If it
was `2`, use `--schema-id=2`.)

**Left pane shows:** `INVALID_RECORD` -- rejected. The record was
valid Avro, but encoded with the wrong schema.

**Right pane shows:** Nothing new.

### 9. Show only valid records exist

```bash
rpk topic consume sensor-data -f 'offset=%o key=%k\n' -n 10
```

Output:

```
offset=0 key=valid1
offset=1 key=valid2
```

The invalid records were never written -- not the plain text, and
not the wrong-schema Avro.

### 10. Key point

"The invalid records were rejected at produce time -- they never hit
the log. The transform fetched the schema from the schema registry
and validated each record's Avro encoding inline, before replication.
Even properly encoded Avro with the wrong schema was caught. With
the sidecar model, all of this invalid data would have been written
first. With produce-path transforms, bad data never exists."

### 11. Clean up

```bash
rpk transform delete schema-validator --no-confirm
rpk topic delete sensor-data
```
