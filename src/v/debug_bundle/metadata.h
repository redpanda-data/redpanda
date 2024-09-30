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

#pragma once

#include "bytes/bytes.h"
#include "container/fragmented_vector.h"
#include "debug_bundle/types.h"
#include "serde/envelope.h"
#include "serde/rw/bytes.h"
#include "serde/rw/envelope.h"
#include "serde/rw/named_type.h"
#include "serde/rw/uuid.h"
#include "serde/rw/variant.h"
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/process.hh>
#include <seastar/util/variant_utils.hh>

#include <chrono>
#include <variant>

namespace debug_bundle {

struct process_output
  : serde::
      envelope<process_output, serde::version<0>, serde::compat_version<0>> {
    chunked_vector<ss::sstring> cout;
    chunked_vector<ss::sstring> cerr;

    auto serde_fields() { return std::tie(cout, cerr); }

    friend bool operator==(const process_output&, const process_output&)
      = default;
};

struct process_wait_exited
  : serde::envelope<
      process_wait_exited,
      serde::version<0>,
      serde::compat_version<0>> {
    int exit_code;

    auto serde_fields() { return std::tie(exit_code); }

    friend auto
    operator<=>(const process_wait_exited&, const process_wait_exited&)
      = default;
};

struct process_wait_signaled
  : serde::envelope<
      process_wait_signaled,
      serde::version<0>,
      serde::compat_version<0>> {
    int signal;

    auto serde_fields() { return std::tie(signal); }

    friend auto
    operator<=>(const process_wait_signaled&, const process_wait_signaled&)
      = default;
};

using serde_process_result
  = serde::variant<process_wait_exited, process_wait_signaled>;
struct metadata
  : serde::envelope<metadata, serde::version<0>, serde::compat_version<0>> {
    using time_resolution = clock::duration;
    time_resolution::rep created_at{};
    time_resolution::rep finished_at{};
    job_id_t job_id;
    ss::sstring debug_bundle_file_path;
    ss::sstring process_output_file_path;
    bytes sha256_checksum;
    serde_process_result process_result;

    metadata() = default;

    metadata(const metadata&) = delete;
    metadata(metadata&&) = default;
    metadata& operator=(const metadata&) = delete;
    metadata& operator=(metadata&&) = default;

    explicit metadata(
      clock::time_point created_at,
      clock::time_point finished_at,
      job_id_t job_id,
      const std::filesystem::path& debug_bundle_file_path,
      const std::filesystem::path& process_output_file_path,
      bytes sha256_checksum,
      ss::experimental::process::wait_status process_result)
      : created_at(created_at.time_since_epoch() / time_resolution{1})
      , finished_at(finished_at.time_since_epoch() / time_resolution{1})
      , job_id(job_id)
      , debug_bundle_file_path(debug_bundle_file_path.native())
      , process_output_file_path(process_output_file_path.native())
      , sha256_checksum(std::move(sha256_checksum))
      , process_result([process_result]() {
          return ss::visit(
            process_result,
            [](const ss::experimental::process::wait_exited& e)
              -> serde_process_result {
                return process_wait_exited{.exit_code = e.exit_code};
            },
            [](const ss::experimental::process::wait_signaled& s)
              -> serde_process_result {
                return process_wait_signaled{.signal = s.terminating_signal};
            });
      }()) {}

    ~metadata() = default;

    friend bool operator==(const metadata&, const metadata&) = default;

    auto serde_fields() {
        return std::tie(
          created_at,
          finished_at,
          job_id,
          debug_bundle_file_path,
          process_output_file_path,
          sha256_checksum,
          process_result);
    }

    clock::time_point get_created_at() const {
        return clock::time_point(time_resolution(created_at));
    }

    clock::time_point get_finished_at() const {
        return clock::time_point(time_resolution(finished_at));
    }

    ss::experimental::process::wait_status get_wait_status() const {
        return ss::visit(
          process_result,
          [](const process_wait_exited& e)
            -> ss::experimental::process::wait_status {
              return ss::experimental::process::wait_exited{
                .exit_code = e.exit_code};
          },
          [](const process_wait_signaled& s)
            -> ss::experimental::process::wait_status {
              return ss::experimental::process::wait_signaled{
                .terminating_signal = s.signal};
          });
    }
};
} // namespace debug_bundle
