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
#include "test_utils/seastar_fuzz.h"

#include "base/seastarx.h"
#include "base/vassert.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>

#include <algorithm>
#include <array>
#include <csignal>
#include <cstdio>
#include <exception>
#include <iterator>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#if defined(__SANITIZE_ADDRESS__)
#define RP_SEASTAR_FUZZ_LSAN 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define RP_SEASTAR_FUZZ_LSAN 1
#endif
#endif

#ifdef RP_SEASTAR_FUZZ_LSAN
#include <sanitizer/lsan_interface.h>
#endif

/// Runs libFuzzer's whole loop -- flags, corpus, mutation, crash reporting --
/// calling \p cb once per input.
extern "C" int LLVMFuzzerRunDriver(
  int* argc, char*** argv, int (*cb)(const uint8_t* data, size_t size));

namespace seastar_fuzz::internal {

namespace {

struct handler_slot {
    int sig;
    struct sigaction action{};
};

/// What a vassert, an abort or a memory fault arrives as. Seastar takes only
/// SIGSEGV, SIGABRT and SIGILL at boot; the other two are carried in case
/// that changes.
std::array<handler_slot, 5>& crash_handlers() {
    static std::array<handler_slot, 5> handlers{
      handler_slot{.sig = SIGILL},
      handler_slot{.sig = SIGABRT},
      handler_slot{.sig = SIGSEGV},
      handler_slot{.sig = SIGBUS},
      handler_slot{.sig = SIGFPE}};
    return handlers;
}

/// Seastar rejects an option given twice. Long forms only.
bool has_option(
  const std::vector<std::string>& args, const std::string_view option) {
    return std::ranges::any_of(args, [option](const std::string& arg) {
        return arg == option || arg.starts_with(std::string{option} + "=");
    });
}

/// libFuzzer's passthrough marker: it ignores everything after it, and its
/// -fork/-jobs machinery keeps the tail verbatim when it rebuilds a worker's
/// command line -- which is how reactor args survive into workers.
char* ignore_remaining_args_flag() {
    static char flag[] = "-ignore_remaining_args=1";
    return flag;
}

/// Where libFuzzer's arguments end and the reactor's begin, or \p argc if
/// there is no separator. "--" is the interface; workers re-executed by
/// libFuzzer arrive with the marker instead.
size_t reactor_args_begin(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg{argv[i]};
        if (arg == "--" || arg == ignore_remaining_args_flag()) {
            return static_cast<size_t>(i);
        }
    }
    return static_cast<size_t>(argc);
}

/// True if \p args has the smp option, as --smp or as the -c short form
/// (glued value included: -c4).
bool has_smp_option(const std::vector<std::string>& args) {
    return has_option(args, "--smp")
           || std::ranges::any_of(args, [](const std::string& arg) {
                  return arg.starts_with("-c");
              });
}

/// The user's reactor options: whatever follows the separator.
std::vector<std::string> user_reactor_args(int argc, char** argv, size_t from) {
    std::vector<std::string> args;
    for (size_t i = from + 1; i < static_cast<size_t>(argc); ++i) {
        args.emplace_back(argv[i]);
    }
    return args;
}

/// argv for the reactor: the binary, the user's options, some defaults.
std::vector<std::string>
reactor_args(char* argv0, std::vector<std::string> user_args) {
    std::vector<std::string> args{argv0};
    args.insert(
      args.end(),
      std::move_iterator{user_args.begin()},
      std::move_iterator{user_args.end()});
    static constexpr auto defaults = std::to_array<std::string_view>(
      {"--smp=1",
       "--overprovisioned",
       "--blocked-reactor-notify-ms=2000000",
       "--default-log-level=warn"});
    for (const auto option : defaults) {
        if (!has_option(args, option.substr(0, option.find('=')))) {
            args.emplace_back(option);
        }
    }
    return args;
}

/// libFuzzer checks for leaks on the fiber, where LSan cannot see the roots
/// owning reactor-boot allocations -- the fiber switch repoints the thread's
/// stack bounds and detaches its ASan fake stack, and LSan has no fiber API
/// to compensate -- so every check would report seastar's boot as a leak of
/// the current input.
///
/// If a reported leak's allocation stack points into reactor internals rather
/// than the fuzz body, an allocation escaped this bracket: suppress that
/// stack, never disable detection.
void lsan_ignore_boot_allocations() {
#ifdef RP_SEASTAR_FUZZ_LSAN
    __lsan_disable();
#endif
}

/// The counter is per OS thread -- one reason the harness rejects --smp/-c.
void lsan_track_from_here() {
#ifdef RP_SEASTAR_FUZZ_LSAN
    __lsan_enable();
#endif
}

void save_crash_handlers() {
    for (auto& slot : crash_handlers()) {
        std::ignore = ::sigaction(slot.sig, nullptr, &slot.action);
    }
}

/// libFuzzer claims only a signal that has no handler, and seastar takes some
/// at boot. Restoring the pre-boot state lets a crash reach libFuzzer and
/// land in an artifact.
void restore_crash_handlers() {
    for (const auto& slot : crash_handlers()) {
        std::ignore = ::sigaction(slot.sig, &slot.action, nullptr);
    }
}

/// Seastar also blocks most signals on the reactor thread. A synchronous
/// SIGBUS or SIGTRAP raised while blocked kills the process with no artifact,
/// and libFuzzer's control signals (SIGUSR1/2, SIGXFSZ) would never arrive.
/// Only these are unblocked: the rest of the mask, SIGPIPE included, is
/// seastar's business.
void unblock_fuzzer_signals() {
    sigset_t sigs;
    sigemptyset(&sigs);
    for (const int sig : {SIGTRAP, SIGBUS, SIGXFSZ, SIGUSR1, SIGUSR2}) {
        sigaddset(&sigs, sig);
    }
    vassert(
      ::pthread_sigmask(SIG_UNBLOCK, &sigs, nullptr) == 0,
      "failed to unblock libFuzzer's signals");
}

} // namespace

/// The reactor's counter is cumulative and bumps when the failed future is
/// destroyed, so the input this fails on can be one later than the code that
/// dropped the future -- the artifact bounds the culprit, it does not name it.
void check_no_abandoned_failed_futures() {
    vassert(
      ss::engine().abandoned_failed_futures() == 0,
      "a failed future was abandoned at or before this input; seastar logged "
      "the exception it dropped");
}

void abort_on_exception(std::exception_ptr e) {
    try {
        std::rethrow_exception(std::move(e));
    } catch (const std::exception& ex) {
        vunreachable("fuzz body threw: {}", ex.what());
    } catch (...) {
        vunreachable("fuzz body threw an unknown exception");
    }
}

/// Brings the reactor up on this thread and runs libFuzzer's loop inside it.
int run_driver(
  int argc, char** argv, int (*callback)(const uint8_t* data, size_t size)) {
    save_crash_handlers();
    lsan_ignore_boot_allocations();

    const size_t split = reactor_args_begin(argc, argv);

    std::vector<std::string> user_args = user_reactor_args(argc, argv, split);
    if (has_smp_option(user_args)) {
        std::ignore = std::fputs(
          "the harness runs one shard, remove --smp/-c: the LSan exemption "
          "and the abandoned-future check cover only shard 0\n",
          stderr);
        return 1;
    }

    std::vector<std::string> args = reactor_args(argv[0], std::move(user_args));
    std::vector<char*> app_argv = args
                                  | std::views::transform(
                                    [](std::string& arg) { return arg.data(); })
                                  | std::ranges::to<std::vector>();

    // libFuzzer gets the whole command line with the separator rewritten to
    // its passthrough marker: it then neither warns about reactor options nor
    // mistakes one for a corpus path, and -fork/-jobs workers inherit them.
    std::vector<char*> fuzzer_argv{argv, argv + argc};
    if (split < static_cast<size_t>(argc)) {
        fuzzer_argv[split] = ignore_remaining_args_flag();
    }

    // Left with seastar, SIGINT/SIGTERM would stop the reactor instead of
    // reaching libFuzzer's interrupt path (final stats, corpus flush).
    ss::app_template::config app_cfg;
    app_cfg.auto_handle_sigint_sigterm = false;
    ss::app_template app{std::move(app_cfg)};

    return app.run(
      static_cast<int>(app_argv.size()),
      app_argv.data(),
      [callback, &fuzzer_argv] {
          restore_crash_handlers();
          unblock_fuzzer_signals();
          return ss::async([callback, &fuzzer_argv] {
              lsan_track_from_here();
              int fuzz_argc = static_cast<int>(fuzzer_argv.size());
              char** fuzz_argv = fuzzer_argv.data();
              return LLVMFuzzerRunDriver(&fuzz_argc, &fuzz_argv, callback);
          });
      });
}

} // namespace seastar_fuzz::internal
