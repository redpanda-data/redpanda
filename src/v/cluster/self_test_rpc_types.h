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

#include "cluster/errc.h"
#include "config/node_config.h"
#include "json/document.h"
#include "model/metadata.h"
#include "random/generators.h"
#include "serde/serde.h"
#include "utils/uuid.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/scheduling.hh>

namespace cluster {

enum class self_test_status : int8_t { idle = 0, running, unreachable };

ss::sstring self_test_status_as_string(self_test_status sts);

std::ostream& operator<<(std::ostream& o, self_test_status sts);

struct diskcheck_opts
  : serde::
      envelope<diskcheck_opts, serde::version<0>, serde::compat_version<0>> {
    /// Descriptive name given to test run
    ss::sstring name{"512K sequential r/w disk test"};
    /// Where files this benchmark will read/write to exist
    std::filesystem::path dir{config::node().disk_benchmark_path()};
    /// Open the file with O_DSYNC flag option
    bool dsync{true};
    /// Set to true to disable the write portion of the benchmark
    bool skip_write{false};
    /// Set to true to disable the read portion of the benchmark
    bool skip_read{false};
    /// Total size of all benchmark files to exist on disk
    uint64_t data_size{10ULL << 30}; // 1GiB
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
          "request_size: {} parallelism: {} duration: {}}}",
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

/// TODO: Replace fields with actual fields from real self-tests
struct netcheck_opts
  : serde::
      envelope<netcheck_opts, serde::version<0>, serde::compat_version<0>> {
    /// Descriptive name given to test run
    ss::sstring name;
    /// Scheduling group that the benchmark will operate under
    ss::scheduling_group sg;

    ss::lowres_clock::duration duration;

    auto serde_fields() { return std::tie(name, duration); }

    static netcheck_opts from_json(const json::Value& obj) {
        static const auto default_duration = 5;
        return cluster::netcheck_opts{
          .name = obj.HasMember("name") ? obj["name"].GetString() : "",
          .duration = std::chrono::seconds(
            obj.HasMember("network_test_execution_time")
              ? obj["network_test_execution_time"].GetInt()
              : default_duration)};
    }

    friend std::ostream&
    operator<<(std::ostream& o, const netcheck_opts& opts) {
        fmt::print(
          o,
          "{{name: {} test_duration: {}}}",
          opts.name,
          opts.duration.count());
        return o;
    }
};

struct self_test_result
  : serde::
      envelope<self_test_result, serde::version<0>, serde::compat_version<0>> {
    double p50;
    double p90;
    double p99;
    double p999;
    double max;
    uint64_t rps;
    uint64_t bps;
    uint32_t timeouts;
    uuid_t test_id;
    ss::sstring name;
    ss::sstring info;
    ss::lowres_clock::duration duration;
    std::optional<ss::sstring> warning;
    std::optional<ss::sstring> error;

    friend std::ostream&
    operator<<(std::ostream& o, const self_test_result& r) {
        fmt::print(
          o,
          "{{p50: {} p90: {} p99: {} p999: {} max: {} rps: {} bps: {} "
          "timeouts: {} test_id: {} name: {} info: {} duration: {}ms warning: "
          "{} error: {}}}",
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
          std::chrono::duration_cast<std::chrono::milliseconds>(r.duration)
            .count(),
          r.warning ? *r.warning : "<no_value>",
          r.error ? *r.error : "<no_value>");
        return o;
    }
};

struct empty_request
  : serde::
      envelope<empty_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
};

struct start_test_request
  : serde::envelope<
      start_test_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    uuid_t id;
    std::vector<diskcheck_opts> dtos;
    std::vector<netcheck_opts> ntos;

    friend std::ostream&
    operator<<(std::ostream& o, const start_test_request& r) {
        std::stringstream ss;
        for (auto& v : r.dtos) {
            fmt::print(ss, "diskcheck_opts: {}", v);
        }
        for (auto& v : r.ntos) {
            fmt::print(ss, "netcheck_opts: {}", v);
        }
        fmt::print(o, "{{id: {} {}}}", r.id, ss.str());
        return o;
    }
};

struct get_status_response
  : serde::envelope<
      get_status_response,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    uuid_t id{};
    self_test_status status{};
    std::vector<self_test_result> results;

    friend std::ostream&
    operator<<(std::ostream& o, const get_status_response& r) {
        fmt::print(
          o,
          "{{id: {} status:{} test_results: {}}}",
          r.id,
          r.status,
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

} // namespace cluster
