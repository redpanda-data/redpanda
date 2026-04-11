# Lightweight PGO training derivation.
#
# Runs a single-node Redpanda instance (developer mode) with rpk-based
# produce/consume workloads to generate LLVM profile data.  The output
# is a merged .profdata file suitable for --fdo_optimize.
#
# This exercises the core hot paths (Kafka protocol, Raft, log storage)
# but not schema registry, Iceberg, or multi-node replication.  For
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

    echo "=== Creating topics ==="
    ${rpkDrv}/bin/rpk topic create pgo-train-1 -p 12 -r 1 \
      --brokers 127.0.0.1:9092
    ${rpkDrv}/bin/rpk topic create pgo-train-2 -p 6 -r 1 \
      --brokers 127.0.0.1:9092
    ${rpkDrv}/bin/rpk topic create pgo-train-3 -p 3 -r 1 \
      --brokers 127.0.0.1:9092

    echo "=== Producing messages (exercises Kafka protocol, batching, storage) ==="
    # Topic 1: small messages (typical Kafka workload)
    for i in $(seq 1 10000); do
      echo "msg-$i-small-$(date +%s%N)"
    done | ${rpkDrv}/bin/rpk topic produce pgo-train-1 --brokers 127.0.0.1:9092

    # Topic 2: medium messages with key-value pairs
    for i in $(seq 1 3000); do
      printf "key-%04d\tvalue-payload-%d-$(head -c 100 /dev/urandom | base64 | head -c 80)" "$i" "$i"
      echo
    done | ${rpkDrv}/bin/rpk topic produce pgo-train-2 -f '%k\t%v\n' \
      --brokers 127.0.0.1:9092

    # Topic 3: larger messages
    for i in $(seq 1 2000); do
      head -c 500 /dev/urandom | base64
    done | ${rpkDrv}/bin/rpk topic produce pgo-train-3 --brokers 127.0.0.1:9092

    echo "=== Consuming messages (exercises fetch path) ==="
    ${rpkDrv}/bin/rpk topic consume pgo-train-1 -n 10000 \
      --brokers 127.0.0.1:9092 > /dev/null
    ${rpkDrv}/bin/rpk topic consume pgo-train-2 -n 3000 \
      --brokers 127.0.0.1:9092 > /dev/null
    ${rpkDrv}/bin/rpk topic consume pgo-train-3 -n 2000 \
      --brokers 127.0.0.1:9092 > /dev/null

    echo "=== Exercising admin/metadata paths ==="
    ${rpkDrv}/bin/rpk topic list --brokers 127.0.0.1:9092
    ${rpkDrv}/bin/rpk topic describe pgo-train-1 --brokers 127.0.0.1:9092
    ${rpkDrv}/bin/rpk cluster info --brokers 127.0.0.1:9092
    ${rpkDrv}/bin/rpk cluster health --api-urls 127.0.0.1:9644

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
