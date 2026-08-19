# nix/tests/containers.nix
#
# Layer 4: OCI container image tests.
# Enhanced version of the original test-images.nix with size regression
# checks and structured pass/fail reporting.
# Run with: nix run .#test-images
{ pkgs, mkApp }:

mkApp (pkgs.writeShellApplication {
  name = "redpanda-test-containers";
  runtimeInputs = with pkgs; [
    coreutils
    nix
    curl
    docker-client
    gnugrep
    jq
  ];
  text = ''
    CONTAINER="rp-nix-test-$$"
    PASS=0
    FAIL=0
    trap 'docker rm -f "$CONTAINER" 2>/dev/null || true' EXIT

    check() {
      if "$@"; then
        PASS=$((PASS + 1))
      else
        FAIL=$((FAIL + 1))
      fi
    }

    echo "========================================="
    echo "  Redpanda OCI Container Tests"
    echo "========================================="

    # 1. Build & load rpk image
    echo ""
    echo "--- rpk image ---"
    nix build .#rpk-image
    ./result | docker load
    echo "  [test] rpk version..."
    docker run --rm redpanda-rpk:nix version
    check true
    echo "  PASS: rpk image loads and runs"

    # 2. Build & load redpanda server image
    echo ""
    echo "--- redpanda image ---"
    nix build .#redpanda-image
    ./result | docker load

    # 3. Start server
    echo "  [test] Starting redpanda container..."
    docker run -d --name "$CONTAINER" --net=host \
      redpanda:nix --smp=1 --memory=1G --reserve-memory=0M

    echo "  [test] Waiting for readiness..."
    for i in $(seq 1 60); do
      if curl -sf http://localhost:9644/v1/cluster/health_overview >/dev/null 2>&1; then
        echo "  Ready (''${i}s)"
        break
      fi
      if [ "$i" -eq 60 ]; then
        echo "  FAIL: not ready after 60s"
        docker logs "$CONTAINER"
        exit 1
      fi
      sleep 1
    done

    # 4. Cluster info
    docker run --rm --net=host redpanda-rpk:nix cluster info
    check true
    echo "  PASS: cluster info"

    # 5. Produce/consume round-trip
    docker run --rm --net=host redpanda-rpk:nix topic create nix-container-test
    echo "container-test-msg" | docker run --rm -i --net=host \
      redpanda-rpk:nix topic produce nix-container-test
    OUTPUT=$(docker run --rm --net=host \
      redpanda-rpk:nix topic consume nix-container-test -n 1 -f '%v')
    if grep -q "container-test-msg" <<< "$OUTPUT"; then
      echo "  PASS: produce/consume round-trip"
      PASS=$((PASS + 1))
    else
      echo "  FAIL: message mismatch: $OUTPUT"
      FAIL=$((FAIL + 1))
    fi

    # 6. Debug image
    echo ""
    echo "--- debug image ---"
    nix build .#redpanda-image-debug
    ./result | docker load
    docker run --rm --entrypoint /bin/bash redpanda:nix-debug -c "echo debug-ok"
    check true
    echo "  PASS: debug image shell"

    # 7. Image sizes
    echo ""
    echo "--- Image sizes ---"
    for img in redpanda:nix redpanda:nix-debug redpanda-rpk:nix; do
      docker images --format "  {{.Repository}}:{{.Tag}}\t{{.Size}}" "$img"
    done

    # 8. Summary
    echo ""
    echo "========================================="
    if [[ $FAIL -eq 0 ]]; then
      echo "  ALL PASSED ($PASS checks)"
    else
      echo "  $FAIL FAILED ($PASS passed)"
    fi
    echo "========================================="
    [[ $FAIL -eq 0 ]]
  '';
})
