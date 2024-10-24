/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/node_config.h"
#include "json/document.h"
#include "model/metadata.h"
#include "serde/rw/chrono.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/optional.h"
#include "serde/rw/scalar.h"
#include "serde/rw/sstring.h"
#include "serde/rw/uuid.h"
#include "serde/rw/vector.h"
#include "utils/uuid.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/scheduling.hh>

namespace cluster {

enum class self_test_status : int8_t { idle = 0, running, unreachable };

ss::sstring self_test_status_as_string(self_test_status sts);

std::ostream& operator<<(std::ostream& o, self_test_status sts);

enum class self_test_stage : int8_t { idle = 0, disk, net, cloud };

ss::sstring self_test_stage_as_string(self_test_stage sts);

std::ostream& operator<<(std::ostream& o, self_test_stage sts);

struct diskcheck_opts
  : serde::
      envelope<diskcheck_opts, serde::version<0>, serde::compat_version<0>> {
    /// Descriptive name given to test run
    ss::sstring name{"unspecified"};
    /// Where files this benchmark will read/write to exist
    std::filesystem::path dir{config::node().disk_benchmark_path()};
    /// Open the file with O_DSYNC flag option
    bool dsync{true};
    /// Set to true to disable the write portion of the benchmark
    bool skip_write{false};
    /// Set to true to disable the read portion of the benchmark
    bool skip_read{false};
    /// Total size of all benchmark files to exist on disk
    uint64_t data_size{10ULL << 30}; // 10GiB
    /// Size of individual read and/or write requests
    size_t request_size{512 << 10}; // 512KiB
    /// Total duration of the benchmark
    ss::lowres_clock::duration duration{std::chrono::milliseconds(5000)};
    /// Amount of fibers to run per shard
    uint16_t parallelism{10};
    /// Scheduling group that the benchmark will operate under
    ss::scheduling_group sg;

    /// Total size a single shard will write/read to disk
    uint64_t file_size() const { return data_size / ss::smp::count; }
    /// Address where allocated memory is placed will be a multiple of this
    uint64_t alignment() const { return request_size >= 4096 ? 4096ULL : 512; }

    static diskcheck_opts from_json(const json::Value& obj) {
        /// The application using these parameters will perform any validation
        diskcheck_opts opts;
        if (obj.HasMember("name")) {
            opts.name = obj["name"].GetString();
        }
        if (obj.HasMember("dsync")) {
            opts.dsync = obj["dsync"].GetBool();
        }
        if (obj.HasMember("skip_write")) {
            opts.skip_write = obj["skip_write"].GetBool();
        }
        if (obj.HasMember("skip_read")) {
            opts.skip_read = obj["skip_read"].GetBool();
        }
        if (obj.HasMember("data_size")) {
            opts.data_size = obj["data_size"].GetUint64();
        }
        if (obj.HasMember("request_size")) {
            opts.request_size = obj["request_size"].GetUint64();
        }
        if (obj.HasMember("duration_ms")) {
            opts.duration = std::chrono::milliseconds(
              obj["duration_ms"].GetInt());
        }
        if (obj.HasMember("parallelism")) {
            opts.parallelism = obj["parallelism"].GetUint();
        }
        return opts;
    }

    auto serde_fields() {
        return std::tie(
          name,
          dsync,
          skip_write,
          skip_read,
          data_size,
          request_size,
          duration,
          parallelism);
    }

    friend std::ostream&
    operator<<(std::ostream& o, const diskcheck_opts& opts) {
        fmt::print(
          o,
          "{{name: {} dsync: {} skip_write: {} skip_read: {} data_size: {} "
          "request_size: {} duration: {} parallelism: {}}}",
          opts.name,
          opts.dsync,
          opts.skip_write,
          opts.skip_read,
          opts.data_size,
          opts.request_size,
          opts.duration,
          opts.parallelism);
        return o;
    }
};

struct netcheck_opts
  : serde::
      envelope<netcheck_opts, serde::version<0>, serde::compat_version<0>> {
    /// Descriptive name given to test run
    ss::sstring name{"Network Test 8192b packet size"};
    /// Node ids of servers to run the test against
    std::vector<model::node_id> peers;
    /// Size of individual request
    size_t request_size{2 << 12}; // 8192 bytes
    /// Total duration of an individual benchmark
    ss::lowres_clock::duration duration{std::chrono::milliseconds(5000)};
    /// Total duration of the entire benchmark across all nodes, ensures no
    /// broker will be stuck in a blocked state for infinite time. This is
    /// automatically set to the number of network benchmarks multiplied by the
    /// above duration of a single netcheck run
    ss::lowres_clock::duration max_duration;
    /// Number of fibers per shard used to make network requests
    uint16_t parallelism{10};
    /// Scheduling group that the benchmark will operate under
    ss::scheduling_group sg;

    static netcheck_opts from_json(const json::Value& obj) {
        /// The application using these parameters will perform any validation
        netcheck_opts opts;
        if (obj.HasMember("name")) {
            opts.name = obj["name"].GetString();
        }
        if (obj.HasMember("request_size")) {
            opts.request_size = obj["request_size"].GetUint64();
        }
        if (obj.HasMember("duration_ms")) {
            opts.duration = std::chrono::milliseconds(
              obj["duration_ms"].GetInt());
        }
        if (obj.HasMember("parallelism")) {
            opts.parallelism = obj["parallelism"].GetInt();
        }
        return opts;
    }

    auto serde_fields() {
        return std::tie(
          name, peers, request_size, duration, max_duration, parallelism);
    }

    friend std::ostream&
    operator<<(std::ostream& o, const netcheck_opts& opts) {
        fmt::print(
          o,
          "{{name: {} peers: {} request_size: {} duration: "
          "{} max_duration: {} parallelism: {}}}",
          opts.name,
          opts.peers,
          opts.request_size,
          opts.duration.count(),
          opts.max_duration.count(),
          opts.parallelism);
        return o;
    }
};

struct cloudcheck_opts
  : serde::
      envelope<cloudcheck_opts, serde::version<0>, serde::compat_version<0>> {
    // Descriptive name given to test run
    ss::sstring name{"Cloud credentials check"};

    // Timeout duration for cloud storage requests.
    ss::lowres_clock::duration timeout{std::chrono::milliseconds(5000)};

    // Backoff duration for cloud storage requests.
    ss::lowres_clock::duration backoff{std::chrono::milliseconds(10)};

    // Scheduling group that the benchmark will operate under.
    ss::scheduling_group sg;

    static cloudcheck_opts from_json(const json::Value& obj) {
        // The application using these parameters will perform any validation
        cloudcheck_opts opts;
        if (obj.HasMember("name")) {
            opts.name = obj["name"].GetString();
        }
        if (obj.HasMember("timeout_ms")) {
            opts.timeout = std::chrono::milliseconds(
              obj["timeout_ms"].GetInt());
        }
        if (obj.HasMember("backoff_ms")) {
            opts.backoff = std::chrono::milliseconds(
              obj["backoff_ms"].GetInt());
        }
        return opts;
    }

    auto serde_fields() { return std::tie(name, timeout, backoff); }

    friend std::ostream&
    operator<<(std::ostream& o, const cloudcheck_opts& opts) {
        fmt::print(
          o,
          "{{name: {} timeout: {} backoff: {}}}",
          opts.name,
          opts.timeout,
          opts.backoff);
        return o;
    }
};

// Captures unparsed test types passed to self test backend.
struct unparsed_check
  : serde::
      envelope<unparsed_check, serde::version<0>, serde::compat_version<0>> {
    ss::sstring test_type;
    ss::sstring test_json;
    auto serde_fields() { return std::tie(test_type, test_json); }

    friend std::ostream&
    operator<<(std::ostream& o, const unparsed_check& unparsed_check) {
        fmt::print(
          o,
          "{{test_type: {}, test_json: {}}}",
          unparsed_check.test_type,
          unparsed_check.test_json);
        return o;
    }
};

struct self_test_result
  : serde::
      envelope<self_test_result, serde::version<1>, serde::compat_version<0>> {
    double p50{0};
    double p90{0};
    double p99{0};
    double p999{0};
    double max{0};
    uint64_t rps{0};
    uint64_t bps{0};
    uint32_t timeouts{0};
    uuid_t test_id;
    ss::sstring name;
    ss::sstring info;
    ss::sstring test_type;
    uint64_t
      start_time{}; // lowres_clock::time_point is not serializable in serde
    uint64_t end_time{};
    ss::lowres_clock::duration duration{};
    std::optional<ss::sstring> warning;
    std::optional<ss::sstring> error;

    friend std::ostream&
    operator<<(std::ostream& o, const self_test_result& r) {
        fmt::print(
          o,
          "{{p50: {} p90: {} p99: {} p999: {} max: {} rps: {} bps: {} "
          "timeouts: {} test_id: {} name: {} info: {} type: {} start_time: {} "
          "end_time: {} duration: {}ms "
          "warning: {} error: {}}}",
          r.p50,
          r.p90,
          r.p99,
          r.p999,
          r.max,
          r.rps,
          r.bps,
          r.timeouts,
          r.test_id,
          r.name,
          r.info,
          r.test_type,
          r.start_time,
          r.end_time,
          std::chrono::duration_cast<std::chrono::milliseconds>(r.duration)
            .count(),
          r.warning ? *r.warning : "<no_value>",
          r.error ? *r.error : "<no_value>");
        return o;
    }

    auto serde_fields() {
        return std::tie(
          p50,
          p90,
          p99,
          p999,
          max,
          rps,
          bps,
          timeouts,
          test_id,
          name,
          info,
          test_type,
          duration,
          warning,
          error,
          start_time,
          end_time);
    }
};

struct empty_request
  : serde::
      envelope<empty_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    auto serde_fields() { return std::tie(); }
};

struct start_test_request
  : serde::envelope<
      start_test_request,
      serde::version<2>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    uuid_t id;
    std::vector<diskcheck_opts> dtos;
    std::vector<netcheck_opts> ntos;
    std::vector<unparsed_check> unparsed_checks;
    std::vector<cloudcheck_opts> ctos;

    auto serde_fields() {
        return std::tie(id, dtos, ntos, unparsed_checks, ctos);
    }

    friend std::ostream&
    operator<<(std::ostream& o, const start_test_request& r) {
        std::stringstream ss;
        for (const auto& v : r.dtos) {
            fmt::print(ss, "diskcheck_opts: {}", v);
        }
        for (const auto& v : r.ntos) {
            fmt::print(ss, "netcheck_opts: {}", v);
        }
        for (const auto& v : r.ctos) {
            fmt::print(ss, "cloudcheck_opts: {}", v);
        }
        for (const auto& v : r.unparsed_checks) {
            fmt::print(ss, "unparsed_check: {}", v);
        }
        fmt::print(o, "{{id: {} {}}}", r.id, ss.str());
        return o;
    }
};

struct get_status_response
  : serde::envelope<
      get_status_response,
      serde::version<1>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    uuid_t id{};
    self_test_status status{};
    std::vector<self_test_result> results;
    self_test_stage stage{};

    auto serde_fields() { return std::tie(id, status, results, stage); }

    friend std::ostream&
    operator<<(std::ostream& o, const get_status_response& r) {
        fmt::print(
          o,
          "{{id: {} status: {} stage: {} test_results: {}}}",
          r.id,
          r.status,
          r.stage,
          r.results);
        return o;
    }
};

struct netcheck_request
  : serde::
      envelope<netcheck_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    model::node_id source;
    iobuf buf;
    auto serde_fields() { return std::tie(source, buf); }
    friend std::ostream&
    operator<<(std::ostream& o, const netcheck_request& r) {
        fmt::print(o, "{{source: {} buf: {}}}", r.source, r.buf.size_bytes());
        return o;
    }
};

struct netcheck_response
  : serde::
      envelope<netcheck_response, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    size_t bytes_read{0};

    auto serde_fields() { return std::tie(bytes_read); }

    friend std::ostream&
    operator<<(std::ostream& o, const netcheck_response& r) {
        fmt::print(o, "{{bytes_read: {}}}", r.bytes_read);
        return o;
    }
};

/// Creates a netcheck_request with the buffer initialized to random data. The
/// buffer will be split into fragments of 8192 bytes each.
ss::future<cluster::netcheck_request>
make_netcheck_request(model::node_id src, size_t sz);

// Parses the raw json out of the start_test_request::unparsed_checks vector
// into self-test options for the various tests, utilizing `opt_t::from_json`.
// In the case that the controller node is of a redpanda version lower than
// the current node, some self-test checks may have been left in
// the "unparsed_checks" vector in the request when the server first processes
// the self test request. We will attempt to parse the test json in the
// self_test_backend of the follower node instead, if we recognize it.
void parse_self_test_checks(start_test_request& r);

} // namespace cluster
