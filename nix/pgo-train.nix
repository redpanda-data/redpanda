# PGO training derivation.
#
# Runs a single-node Redpanda instance (developer mode) with rpk-based
# produce/consume workloads to generate LLVM profile data.  The output
# is a merged .profdata file suitable for --fdo_optimize.
#
# The workload mirrors a realistic Kafka message-size distribution:
#   ~45% tiny (0.1–1 KB)   metrics, events, log lines
#   ~30% small (1–5 KB)    JSON application events, small Avro
#   ~15% medium (10–50 KB) enriched events, nested documents
#    ~8% large (50–500 KB) bulk data, images, aggregates
#    ~2% XL (500 KB–1 MB)  near-max payloads, batch splitting
#
# This exercises core hot paths (Kafka protocol, Raft, log storage) at
# every size tier, giving the compiler realistic branch-probability
# data for batch splitting, memory allocation, compression, and fetch
# chunking.  ~15k total messages across 5 topics.
#
# Not covered: schema registry, Iceberg, multi-node replication.  For
# production-quality profiles, use the full train_pgo.py pipeline and
# pass the result via lib.mkRedpandaPgo.
{
  lib,
  runCommand,
  redpandaInstrumented, # derivation built with pgoMode = "instrument"
  rpkDrv, # rpk Go CLI derivation
  llvmPackages_20,
  coreutils,
  bash,
  gnugrep,
  procps,
  gawk,
  iproute2,
  yaml-cpp,
}:

runCommand "redpanda-pgo-profile"
  {
    nativeBuildInputs = [
      redpandaInstrumented
      rpkDrv
      llvmPackages_20.llvm # provides llvm-profdata
      coreutils
      bash
      gnugrep
      procps
      gawk
      iproute2
    ];
    requiredSystemFeatures = [ "big-parallel" ];
  }
  ''
    set -euo pipefail

    WORK=$(mktemp -d)
    PROFILE_DIR="$WORK/profiles"
    DATA_DIR="$WORK/data"
    mkdir -p "$PROFILE_DIR" "$DATA_DIR" "$out"

    # Instrumented binary writes profile data here on exit.
    export LLVM_PROFILE_FILE="$PROFILE_DIR/data-%p.profraw"

    echo "=== Starting single-node Redpanda (developer mode) ==="
    cat > "$WORK/redpanda.yaml" <<YAML
    redpanda:
      data_directory: $DATA_DIR
      developer_mode: true
      node_id: 0
      rpc_server:
        address: 127.0.0.1
        port: 33145
      kafka_api:
        - address: 127.0.0.1
          port: 9092
      admin:
        - address: 127.0.0.1
          port: 9644
      seed_servers: []
    YAML
    ${redpandaInstrumented}/bin/redpanda \
      --redpanda-cfg "$WORK/redpanda.yaml" \
      --smp 2 --memory 2G &
    RP_PID=$!

    # Wait for the node to become healthy.
    echo "=== Waiting for Redpanda to start ==="
    for i in $(seq 1 120); do
      if ${rpkDrv}/bin/rpk cluster health \
        --api-urls 127.0.0.1:9644 2>/dev/null | grep -q "Healthy"; then
        echo "Redpanda healthy after ''${i}s"
        break
      fi
      if [ "$i" -eq 120 ]; then
        echo "ERROR: Redpanda did not become healthy within 120s"
        kill -TERM $RP_PID 2>/dev/null || true
        wait $RP_PID || true
        exit 1
      fi
      sleep 1
    done

    # ---------------------------------------------------------------
    # Workload design: realistic Kafka message-size distribution.
    #
    # Real Kafka traffic spans several orders of magnitude:
    #   ~45% metrics/events/logs    0.1 – 1 KB
    #   ~30% application events     1   – 10 KB  (JSON, small Avro)
    #   ~15% enriched events        10  – 50 KB
    #    ~8% bulk / batch           50  – 500 KB  (large JSON, images)
    #    ~2% near-max               500 KB – 1 MB
    #
    # We mirror that distribution across ~15k total messages so the
    # compiler sees realistic branch weights for batch splitting,
    # memory allocation, compression, and fetch chunking at every
    # size tier.  Each tier uses its own topic with partition counts
    # chosen to exercise the partition-routing hot path.
    #
    # llvm-profdata merge combines all .profraw files (one per shard)
    # into a single weighted .profdata.  More diverse profiles →
    # better branch-probability estimates → better PGO.
    # ---------------------------------------------------------------

    RPK="${rpkDrv}/bin/rpk"
    BROKERS="--brokers 127.0.0.1:9092"

    # Helper: generate a payload of exactly N bytes (base64 from urandom).
    gen_payload() {
      local size=$1
      head -c "$size" /dev/urandom | base64 -w0
    }

    echo "=== Creating topics (5 size tiers) ==="
    $RPK topic create pgo-tiny   -p 12 -r 1 $BROKERS   # 0.1–1 KB
    $RPK topic create pgo-small  -p 8  -r 1 $BROKERS   # 1–10 KB
    $RPK topic create pgo-medium -p 6  -r 1 $BROKERS   # 10–50 KB
    $RPK topic create pgo-large  -p 4  -r 1 $BROKERS   # 50–500 KB
    $RPK topic create pgo-xlarge -p 3  -r 1 $BROKERS   # 500 KB–1 MB

    echo "=== Producing messages (realistic size distribution) ==="

    # Tier 1: tiny messages — 0.1–1 KB  (~45% = 6,750 messages)
    # Simulates metrics, events, and log lines.
    echo "  Tier 1: 6750 tiny messages (0.1–1 KB)..."
    for i in $(seq 1 6750); do
      # ~200 bytes average: short key + small payload
      printf "key-%05d\tmsg-%d-ts-%s-$(head -c 128 /dev/urandom | base64 -w0 | head -c 150)" "$i" "$i" "$(date +%s%N)"
      echo
    done | $RPK topic produce pgo-tiny -f '%k\t%v\n' $BROKERS

    # Tier 2: small messages — 1–5 KB  (~30% = 4,500 messages)
    # Simulates JSON application events and small Avro records.
    echo "  Tier 2: 4500 small messages (1–5 KB)..."
    for i in $(seq 1 4500); do
      printf "evt-%05d\t" "$i"
      gen_payload 2048  # ~2.7 KB after base64
      echo
    done | $RPK topic produce pgo-small -f '%k\t%v\n' $BROKERS

    # Tier 3: medium messages — 10–50 KB  (~15% = 2,250 messages)
    # Simulates enriched events, nested JSON documents.
    echo "  Tier 3: 2250 medium messages (10–50 KB)..."
    for i in $(seq 1 2250); do
      printf "enrich-%05d\t" "$i"
      gen_payload 20480  # ~27 KB after base64
      echo
    done | $RPK topic produce pgo-medium -f '%k\t%v\n' $BROKERS

    # Tier 4: large messages — 50–500 KB  (~8% = 1,200 messages)
    # Simulates bulk data, images, large aggregates.
    echo "  Tier 4: 1200 large messages (50–500 KB)..."
    for i in $(seq 1 1200); do
      printf "bulk-%05d\t" "$i"
      gen_payload 131072  # ~175 KB after base64
      echo
    done | $RPK topic produce pgo-large -f '%k\t%v\n' $BROKERS

    # Tier 5: extra-large messages — 500 KB–1 MB  (~2% = 300 messages)
    # Simulates near-max payloads that stress batch splitting.
    echo "  Tier 5: 300 XL messages (500 KB–1 MB)..."
    for i in $(seq 1 300); do
      printf "xl-%05d\t" "$i"
      gen_payload 524288  # ~700 KB after base64
      echo
    done | $RPK topic produce pgo-xlarge -f '%k\t%v\n' $BROKERS

    echo "=== Consuming messages (exercises fetch path at every size) ==="
    $RPK topic consume pgo-tiny   -n 6750 $BROKERS > /dev/null
    $RPK topic consume pgo-small  -n 4500 $BROKERS > /dev/null
    $RPK topic consume pgo-medium -n 2250 $BROKERS > /dev/null
    $RPK topic consume pgo-large  -n 1200 $BROKERS > /dev/null
    $RPK topic consume pgo-xlarge -n 300  $BROKERS > /dev/null

    echo "=== Exercising admin/metadata paths ==="
    $RPK topic list $BROKERS
    for t in pgo-tiny pgo-small pgo-medium pgo-large pgo-xlarge; do
      $RPK topic describe "$t" $BROKERS
    done
    $RPK cluster info $BROKERS
    $RPK cluster health --api-urls 127.0.0.1:9644

    echo "=== Shutting down Redpanda (SIGTERM flushes profile data) ==="
    kill -TERM $RP_PID
    # Wait for graceful shutdown and profile flush.
    wait $RP_PID || true

    echo "=== Merging profile data ==="
    PROFILES=("$PROFILE_DIR"/*.profraw)
    if [ ''${#PROFILES[@]} -eq 0 ]; then
      echo "ERROR: No .profraw files found in $PROFILE_DIR"
      exit 1
    fi
    echo "Found ''${#PROFILES[@]} profile file(s)"

    llvm-profdata merge \
      -o "$out/pgo_profile.profdata" \
      "''${PROFILES[@]}"

    echo "=== PGO profile generated: $out/pgo_profile.profdata ==="
    ls -lh "$out/pgo_profile.profdata"
  ''
