{ pkgs, mkApp }:

mkApp (pkgs.writeShellApplication {
    name = "redpanda-test-images";
    runtimeInputs = with pkgs; [
      coreutils
      nix
      curl
      docker-client
    ];
    text = ''
      CONTAINER="rp-nix-test-$$"
      trap 'docker rm -f "$CONTAINER" 2>/dev/null || true' EXIT

      # 1. Build & load rpk image
      echo "[test] Building rpk image..."
      nix build .#rpk-image
      ./result | docker load

      # 2. rpk version smoke test
      echo "[test] rpk version check..."
      docker run --rm redpanda-rpk:nix version

      # 3. Build & load redpanda server image
      echo "[test] Building redpanda image..."
      nix build .#redpanda-image
      ./result | docker load

      # 4. Start redpanda server
      echo "[test] Starting redpanda..."
      docker run -d --name "$CONTAINER" --net=host \
        redpanda:nix --smp=1 --memory=1G --reserve-memory=0M

      # 5. Wait for readiness (poll admin API health endpoint)
      echo "[test] Waiting for readiness..."
      for i in $(seq 1 60); do
        if curl -sf http://localhost:9644/v1/cluster/health_overview >/dev/null 2>&1; then
          echo "[test] Ready (''${i}s)"
          break
        fi
        if [ "$i" -eq 60 ]; then
          echo "FAIL: redpanda not ready after 60s"
          docker logs "$CONTAINER"
          exit 1
        fi
        sleep 1
      done

      # 6. Cluster info
      docker run --rm --net=host redpanda-rpk:nix cluster info
      echo "[test] Cluster info OK"

      # 7. Create topic, produce, consume
      docker run --rm --net=host redpanda-rpk:nix topic create nix-smoke-test
      echo "[test] Topic created"

      echo "hello-from-nix" | docker run --rm -i --net=host \
        redpanda-rpk:nix topic produce nix-smoke-test
      echo "[test] Message produced"

      OUTPUT=$(docker run --rm --net=host \
        redpanda-rpk:nix topic consume nix-smoke-test -n 1 -f '%v')
      if ! grep -q "hello-from-nix" <<< "$OUTPUT"; then
        echo "FAIL: consumed message does not match"
        echo "Got: $OUTPUT"
        exit 1
      fi
      echo "[test] Message consumed — round-trip OK"

      # 8. Debug image: build, load, verify shell
      echo "[test] Building debug image..."
      nix build .#redpanda-image-debug
      ./result | docker load
      docker run --rm --entrypoint /bin/bash redpanda:nix-debug -c "echo debug-ok"
      echo "[test] Debug image shell OK"

      # 9. Image sizes
      echo ""
      echo "[test] Image sizes:"
      for img in redpanda:nix redpanda:nix-debug redpanda-rpk:nix; do
        docker images --format "  {{.Repository}}:{{.Tag}}\t{{.Size}}" "$img"
      done
      echo ""

      echo "[test] All tests passed"
    '';
  })
