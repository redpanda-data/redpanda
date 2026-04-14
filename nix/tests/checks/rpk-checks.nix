# rpk CLI checks: offline (no cluster) and live (against running cluster).
{ rpkDrv, constants }:

let
  rpk = "${rpkDrv}/bin/rpk";
  brokers = "--brokers 127.0.0.1:${toString constants.ports.kafka}";
  admin = "--api-urls 127.0.0.1:${toString constants.ports.admin}";
in
{
  mkRpkOfflineChecks = ''
    start=$(time_ms)
    if ${rpk} version >/dev/null 2>&1; then
      result_pass "rpk version" "$(elapsed_ms "$start")"
      record_pass
    else
      result_fail "rpk version" "$(elapsed_ms "$start")"
      record_fail
    fi

    help_start=$(time_ms)
    if ${rpk} --help >/dev/null 2>&1; then
      result_pass "rpk --help" "$(elapsed_ms "$help_start")"
      record_pass
    else
      result_fail "rpk --help" "$(elapsed_ms "$help_start")"
      record_fail
    fi

    for subcmd in topic cluster group registry; do
      sub_start=$(time_ms)
      if ${rpk} --help 2>&1 | grep -qi "$subcmd"; then
        result_pass "rpk help lists $subcmd" "$(elapsed_ms "$sub_start")"
        record_pass
      else
        result_fail "rpk help missing $subcmd" "$(elapsed_ms "$sub_start")"
        record_fail
      fi
    done
  '';

  mkRpkLiveChecks = ''
    assert_rpk_succeeds "rpk cluster info"     ${rpk} cluster info ${brokers}
    assert_rpk_succeeds "rpk cluster health"   ${rpk} cluster health ${admin}
    assert_rpk_succeeds "rpk topic list"       ${rpk} topic list ${brokers}
    assert_rpk_succeeds "rpk cluster config"   ${rpk} cluster config export ${admin}
  '';
}
