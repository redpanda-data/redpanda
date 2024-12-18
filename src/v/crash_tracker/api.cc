/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "crash_tracker/api.h"

#include "base/vassert.h"
#include "config/node_config.h"
#include "crash_tracker/logger.h"
#include "crash_tracker/service.h"
#include "hashing/xx.h"
#include "serde/rw/envelope.h"
#include "utils/file_io.h"

#include <seastar/core/memory.hh>
#include <seastar/util/print_safe.hh>

#include <fmt/core.h>

#include <chrono>
#include <iterator>
#include <stdio.h>

using namespace std::chrono_literals;

namespace crash_tracker {

namespace {

service& instance() {
    static service inst;
    return inst;
}

} // namespace

ss::future<> initialize() { return instance().start(); }

ss::future<> record_clean_shutdown() { return instance().stop(); }

namespace {
std::string_view failure_msg_prefix(int signo) {
    if (signo == SIGSEGV) {
        return "Segmentation fault";
    } else if (signo == SIGILL) {
        return "Illegal instruction";
    } else {
        return "Aborting";
    }
}
} // namespace

void record_signal_crash(crash_description& cd, int signo) {
    auto& format_buf = cd._crash_reason;
    fmt::format_to_n(
      format_buf.begin(),
      format_buf.size(),
      "{} on shard {}. Backtrace: {}",
      failure_msg_prefix(signo),
      ss::this_shard_id(),
      ss::current_backtrace());

    ss::backtrace([&cd](ss::frame f) { cd._stacktrace.push_back(f.addr); });
}

void record_sigsegv_crash() {
    instance().record_crash(
      [](crash_description& cd) { record_signal_crash(cd, SIGSEGV); });
}

void record_sigabrt_crash() {
    instance().record_crash(
      [](crash_description& cd) { record_signal_crash(cd, SIGABRT); });
}

void record_sigill_crash() {
    instance().record_crash(
      [](crash_description& cd) { record_signal_crash(cd, SIGILL); });
}

} // namespace crash_tracker
