# nix/tests/lib.nix
#
# Reusable bash helper functions for Redpanda Nix tests.
# Adapted from KwaaiNet's lifecycle/lib.nix, focused on localhost testing.
#
# Each attribute returns a bash string fragment that can be interpolated
# into writeShellApplication scripts.
{ redpandaDrv, rpkDrv }:

let
  constants = import ./constants.nix;
in
{
  inherit constants;

  colorHelpers = ''
    _reset='\033[0m'
    _bold='\033[1m'
    _red='\033[31m'
    _green='\033[32m'
    _yellow='\033[33m'
    _cyan='\033[36m'

    info()    { echo -e "''${_cyan}$*''${_reset}"; }
    success() { echo -e "''${_green}$*''${_reset}"; }
    warn()    { echo -e "''${_yellow}$*''${_reset}"; }
    error()   { echo -e "''${_red}$*''${_reset}"; }
    bold()    { echo -e "''${_bold}$*''${_reset}"; }

    phase_header() {
      local phase="$1"; local name="$2"; local timeout="$3"
      echo ""
      echo -e "''${_bold}--- Phase $phase: $name (timeout: ''${timeout}s) ---''${_reset}"
    }

    result_pass() {
      local msg="$1"; local time_ms="''${2:-0}"
      echo -e "  ''${_green}PASS''${_reset}: $msg (''${time_ms}ms)"
    }

    result_fail() {
      local msg="$1"; local time_ms="''${2:-0}"
      echo -e "  ''${_red}FAIL''${_reset}: $msg (''${time_ms}ms)"
    }

    result_skip() {
      local msg="$1"
      echo -e "  ''${_yellow}SKIP''${_reset}: $msg"
    }
  '';

  timingHelpers = ''
    time_ms() { echo $(($(date +%s%N) / 1000000)); }
    elapsed_ms() { local start="$1"; echo $(( $(time_ms) - start )); }
    format_ms() {
      local ms="$1"
      if [[ $ms -lt 1000 ]]; then echo "''${ms}ms"
      elif [[ $ms -lt 60000 ]]; then echo "$((ms / 1000)).$((ms % 1000 / 100))s"
      else echo "$((ms / 60000))m$(( (ms % 60000) / 1000 ))s"; fi
    }
  '';

  counterHelpers = ''
    TOTAL_PASSED=0
    TOTAL_FAILED=0
    record_pass() { TOTAL_PASSED=$((TOTAL_PASSED + 1)); }
    record_fail() { TOTAL_FAILED=$((TOTAL_FAILED + 1)); }
  '';

  processHelpers = ''
    wait_for_ready() {
      local admin_port="''${1:-${toString constants.ports.admin}}"
      local timeout="''${2:-${toString constants.timeouts.startup}}"
      local elapsed=0
      while [[ $elapsed -lt $timeout ]]; do
        if curl -sf "http://127.0.0.1:$admin_port/v1/cluster/health_overview" >/dev/null 2>&1; then
          return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
      done
      return 1
    }

    graceful_shutdown() {
      local pid="$1"
      local timeout="''${2:-${toString constants.timeouts.shutdown}}"
      kill -TERM "$pid" 2>/dev/null || true
      local elapsed=0
      while [[ $elapsed -lt $timeout ]]; do
        if ! kill -0 "$pid" 2>/dev/null; then return 0; fi
        sleep 1
        elapsed=$((elapsed + 1))
      done
      kill -9 "$pid" 2>/dev/null || true
      return 1
    }

    start_redpanda() {
      local config_file="$1"
      ${redpandaDrv}/bin/redpanda \
        --redpanda-cfg "$config_file" \
        --smp 1 --memory 1G --reserve-memory 0M &
      RP_PID=$!
    }
  '';

  assertionHelpers = ''
    assert_http_ok() {
      local url="$1"; local desc="$2"
      local start; start=$(time_ms)
      local status
      status=$(curl -sf -o /dev/null -w '%{http_code}' "$url" 2>/dev/null || echo "000")
      if [[ "$status" == "200" ]]; then
        result_pass "$desc" "$(elapsed_ms "$start")"
        record_pass
      else
        result_fail "$desc (HTTP $status)" "$(elapsed_ms "$start")"
        record_fail
      fi
    }

    assert_http_json_field() {
      local url="$1"; local field="$2"; local desc="$3"
      local start; start=$(time_ms)
      local body
      body=$(curl -sf "$url" 2>/dev/null || echo "")
      if echo "$body" | jq -e ".$field" >/dev/null 2>&1; then
        result_pass "$desc" "$(elapsed_ms "$start")"
        record_pass
      else
        result_fail "$desc (field .$field missing)" "$(elapsed_ms "$start")"
        record_fail
      fi
    }

    assert_rpk_succeeds() {
      local desc="$1"; shift
      local start; start=$(time_ms)
      if "$@" >/dev/null 2>&1; then
        result_pass "$desc" "$(elapsed_ms "$start")"
        record_pass
      else
        result_fail "$desc" "$(elapsed_ms "$start")"
        record_fail
      fi
    }
  '';

  # Summary block to include at the end of each test script.
  summaryBlock = ''
    TOTAL_ELAPSED=$(elapsed_ms "$TOTAL_START")
    echo ""
    bold "========================================"
    if [[ $TOTAL_FAILED -eq 0 ]]; then
      success "  ALL PASSED ($TOTAL_PASSED checks)"
      success "  Total time: $(format_ms "$TOTAL_ELAPSED")"
    else
      error "  $TOTAL_FAILED FAILED ($TOTAL_PASSED passed)"
    fi
    bold "========================================"
    [[ $TOTAL_FAILED -eq 0 ]]
  '';
}
