/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "base/vlog.h"
#include "base/vlog_callsite.h"
#include "base/vlog_filter.h"

#include <seastar/util/log.hh>

#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <string_view>

namespace {

using cs_state = vlog::detail::callsite_base::state;

// Each TEST body that references a callsite by line intentionally keeps the
// callsite literal inline so the reported __LINE__ is deterministic under
// formatting. The helper below looks up a registered callsite by filename
// basename + line + format literal.
const vlog::detail::callsite_base*
find_site(const char* file, unsigned line, const char* fmt) {
    const vlog::detail::callsite_base* found = nullptr;
    vlog::for_each_callsite([&](vlog::detail::callsite_base& cs) {
        if (found != nullptr) {
            return;
        }
        if (std::string_view(cs.file()) != file) {
            return;
        }
        if (cs.line() != line) {
            return;
        }
        if (std::string_view(cs.fmt()) != fmt) {
            return;
        }
        found = &cs;
    });
    return found;
}

} // namespace

TEST(VlogFilter, CallsiteSelfRegisters) {
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/base/tests/vlog_filter_test.cc", 100, "hello {}")>
      cs = {};
    EXPECT_EQ(cs.resolved_state(), cs_state::default_);
    const auto* found = find_site(
      "src/v/base/tests/vlog_filter_test.cc", 100, "hello {}");
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found, &cs);
}

TEST(VlogFilter, FileGlobRule) {
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/raft/consensus.cc", 10, "raft message")>
      cs = {};
    ASSERT_EQ(cs.resolved_state(), cs_state::default_);

    vlog::apply_rules({vlog::rule{
      .file = std::string{"src/v/raft/*"}, .state = cs_state::force_off}});
    EXPECT_EQ(cs.resolved_state(), cs_state::force_off);

    vlog::reset_rules();
    EXPECT_EQ(cs.resolved_state(), cs_state::default_);
}

TEST(VlogFilter, FileLineRule) {
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/raft/consensus.cc", 42, "fmt a")>
      cs_a = {};
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/raft/consensus.cc", 43, "fmt b")>
      cs_b = {};

    vlog::apply_rules({vlog::rule{
      .file = std::string{"src/v/raft/consensus.cc"},
      .line = std::pair{42u, 42u},
      .state = cs_state::force_off}});

    EXPECT_EQ(cs_a.resolved_state(), cs_state::force_off);
    EXPECT_EQ(cs_b.resolved_state(), cs_state::default_);

    vlog::reset_rules();
}

TEST(VlogFilter, LineRangeRule) {
    // Inclusive [lo, hi] — endpoints match, out-of-band doesn't.
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/raft/heartbeat.cc", 50, "below range")>
      cs_below = {};
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/raft/heartbeat.cc", 100, "range low")>
      cs_lo = {};
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/raft/heartbeat.cc", 150, "range mid")>
      cs_mid = {};
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/raft/heartbeat.cc", 200, "range high")>
      cs_hi = {};
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/raft/heartbeat.cc", 201, "above range")>
      cs_above = {};

    vlog::apply_rules({vlog::rule{
      .file = std::string{"src/v/raft/heartbeat.cc"},
      .line = std::pair{100u, 200u},
      .state = cs_state::force_off}});

    EXPECT_EQ(cs_below.resolved_state(), cs_state::default_);
    EXPECT_EQ(cs_lo.resolved_state(), cs_state::force_off);
    EXPECT_EQ(cs_mid.resolved_state(), cs_state::force_off);
    EXPECT_EQ(cs_hi.resolved_state(), cs_state::force_off);
    EXPECT_EQ(cs_above.resolved_state(), cs_state::default_);

    vlog::reset_rules();
}

TEST(VlogFilter, ContainsRule) {
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/kafka/server/handler.cc", 7, "entering slow path for {}")>
      cs_hit = {};
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/kafka/server/handler.cc", 8, "normal processing")>
      cs_miss = {};

    vlog::apply_rules({vlog::rule{
      .contains = std::string{"slow path"}, .state = cs_state::force_off}});

    EXPECT_EQ(cs_hit.resolved_state(), cs_state::force_off);
    EXPECT_EQ(cs_miss.resolved_state(), cs_state::default_);

    vlog::reset_rules();
}

TEST(VlogFilter, LaterRuleOverridesEarlier) {
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/storage/disk_log.cc", 100, "compacting segment {}")>
      cs = {};

    vlog::apply_rules({
      vlog::rule{
        .file = std::string{"src/v/storage/*"}, .state = cs_state::force_off},
      vlog::rule{
        .file = std::string{"src/v/storage/disk_log.cc"},
        .line = std::pair{100u, 100u},
        .state = cs_state::default_},
    });
    EXPECT_EQ(cs.resolved_state(), cs_state::default_);

    vlog::apply_rules({
      vlog::rule{
        .file = std::string{"src/v/storage/disk_log.cc"},
        .line = std::pair{100u, 100u},
        .state = cs_state::default_},
      vlog::rule{
        .file = std::string{"src/v/storage/*"}, .state = cs_state::force_off},
    });
    EXPECT_EQ(cs.resolved_state(), cs_state::force_off);

    vlog::reset_rules();
}

TEST(VlogFilter, EmptyRuleMatchesEverything) {
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/unrelated/file.cc", 1, "anything")>
      cs = {};

    vlog::apply_rules({vlog::rule{.state = cs_state::force_off}});
    EXPECT_EQ(cs.resolved_state(), cs_state::force_off);

    vlog::reset_rules();
    EXPECT_EQ(cs.resolved_state(), cs_state::default_);
}

TEST(VlogFilter, RulesAppliedBeforeRegistrationAffectNewSites) {
    // Apply a rule that disables a pattern, then register a new callsite
    // matching that pattern. The new site must start disabled — this is the
    // cold-start case: a log line is filtered before it has ever fired.
    vlog::apply_rules({vlog::rule{
      .file = std::string{"src/v/kafka/cold/*"},
      .state = cs_state::force_off}});

    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/kafka/cold/new_handler.cc", 1, "freshly registered")>
      cs = {};

    EXPECT_EQ(cs.resolved_state(), cs_state::force_off);

    vlog::reset_rules();
    EXPECT_EQ(cs.resolved_state(), cs_state::default_);
}

TEST(VlogFilter, SitesNotMatchedKeepDefault) {
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/cluster/foo.cc", 5, "cluster log")>
      cs = {};

    vlog::apply_rules({vlog::rule{
      .file = std::string{"src/v/raft/*"}, .state = cs_state::force_off}});
    EXPECT_EQ(cs.resolved_state(), cs_state::default_);

    vlog::reset_rules();
}

namespace {

// Captures a seastar logger's output to a stringstream for duration of
// the scope. The destructor restores the ostream to std::cerr so the
// logger does not hold a dangling pointer after the captured buffer
// goes out of scope.
class logger_capture {
public:
    logger_capture() {
        seastar::logger::set_ostream(_captured);
        seastar::logger::set_ostream_enabled(true);
    }
    ~logger_capture() { seastar::logger::set_ostream(std::cerr); }
    std::string str() const { return _captured.str(); }
    void clear() { _captured.str(""); }

private:
    std::ostringstream _captured;
};

// A dedicated logger for this test file's force_on/force_off tests. Each
// test sets its level explicitly; other tests in this binary don't share
// it.
seastar::logger vlog_filter_test_log("vlog_filter_test");

} // namespace

TEST(VlogFilter, ForceOnBypassesLevelGate) {
    logger_capture cap;
    vlog_filter_test_log.set_level(seastar::log_level::warn);

    vlog::reset_rules();

    // The vlog() macro declares a static callsite at the call line. We
    // compute the macro's __LINE__ ahead of time so the rule pinpoints
    // it. `__LINE__ + N` below points at the vlog line; adjust N if you
    // re-indent the macro expansion.
    constexpr auto* this_file = "src/v/base/tests/vlog_filter_test.cc";
    const auto site_line = static_cast<unsigned>(__LINE__) + 5;
    vlog::apply_rules({vlog::rule{
      .file = std::string{this_file},
      .line = std::pair{site_line, site_line},
      .state = cs_state::force_on}});
    vlog(vlog_filter_test_log.trace, "trace_forced_on_payload"); // target line

    EXPECT_NE(cap.str().find("trace_forced_on_payload"), std::string::npos);
    vlog::reset_rules();
}

TEST(VlogFilter, ForceOffSuppressesRegardless) {
    logger_capture cap;
    vlog_filter_test_log.set_level(seastar::log_level::trace);

    vlog::reset_rules();

    const auto site_line = static_cast<unsigned>(__LINE__) + 5;
    vlog::apply_rules({vlog::rule{
      .file = std::string{"src/v/base/tests/vlog_filter_test.cc"},
      .line = std::pair{site_line, site_line},
      .state = cs_state::force_off}});
    vlog(vlog_filter_test_log.trace, "trace_forced_off_payload"); // target line

    EXPECT_EQ(cap.str().find("trace_forced_off_payload"), std::string::npos);
    vlog::reset_rules();
}

TEST(VlogFilter, LastRuleWinsAcrossAllThreeStates) {
    static vlog::detail::callsite<vlog::detail::make_site_nttp(
      "src/v/cluster/ordering.cc", 1, "ordering")>
      cs = {};

    vlog::apply_rules({
      vlog::rule{
        .file = std::string{"src/v/cluster/*"}, .state = cs_state::force_off},
      vlog::rule{
        .file = std::string{"src/v/cluster/ordering.cc"},
        .state = cs_state::force_on},
    });
    EXPECT_EQ(cs.resolved_state(), cs_state::force_on);

    vlog::apply_rules({
      vlog::rule{
        .file = std::string{"src/v/cluster/*"}, .state = cs_state::force_on},
      vlog::rule{
        .file = std::string{"src/v/cluster/ordering.cc"},
        .state = cs_state::default_},
    });
    EXPECT_EQ(cs.resolved_state(), cs_state::default_);

    vlog::reset_rules();
}
