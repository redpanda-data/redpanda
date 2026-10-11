# Admin API endpoint checks.
{ constants }:

let
  adminBase = "http://127.0.0.1:${toString constants.ports.admin}";
in
{
  mkHealthCheck = ''
    health_start=$(time_ms)
    body=$(curl -sf "${adminBase}/v1/cluster/health_overview" 2>/dev/null || echo "")
    if echo "$body" | jq -e '.is_healthy' >/dev/null 2>&1; then
      result_pass "cluster health_overview" "$(elapsed_ms "$health_start")"
      record_pass
    else
      result_fail "cluster health_overview" "$(elapsed_ms "$health_start")"
      record_fail
    fi
  '';

  mkClusterConfigCheck = ''
    cfg_start=$(time_ms)
    body=$(curl -sf "${adminBase}/v1/cluster_config" 2>/dev/null || echo "")
    if [ -n "$body" ] && echo "$body" | jq -e '.' >/dev/null 2>&1; then
      result_pass "cluster config endpoint" "$(elapsed_ms "$cfg_start")"
      record_pass
    else
      result_fail "cluster config endpoint" "$(elapsed_ms "$cfg_start")"
      record_fail
    fi
  '';

  mkBrokersCheck = ''
    broker_start=$(time_ms)
    body=$(curl -sf "${adminBase}/v1/brokers" 2>/dev/null || echo "")
    if echo "$body" | jq -e '.[0].node_id' >/dev/null 2>&1; then
      result_pass "brokers endpoint" "$(elapsed_ms "$broker_start")"
      record_pass
    else
      result_fail "brokers endpoint" "$(elapsed_ms "$broker_start")"
      record_fail
    fi
  '';

  mkStatusCheck = ''
    status_start=$(time_ms)
    status_code=$(curl -sf -o /dev/null -w '%{http_code}' "${adminBase}/v1/status/ready" 2>/dev/null || echo "000")
    if [[ "$status_code" == "200" ]]; then
      result_pass "status/ready endpoint" "$(elapsed_ms "$status_start")"
      record_pass
    else
      result_fail "status/ready endpoint (HTTP $status_code)" "$(elapsed_ms "$status_start")"
      record_fail
    fi
  '';
}
