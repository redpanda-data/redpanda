/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_io/io_result.h"
#include "cloud_storage_clients/types.h"
#include "lsm/io/persistence.h"
#include "model/fundamental.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_provider.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>

#include <exception>
#include <optional>
#include <string_view>

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace lsm::io {

[[noreturn]] void throw_as_lsm_ex(std::exception_ptr ex, ss::sstring msg);
retry_chain_node make_cloud_rtc(ss::abort_source& as);

/// Returns true if the download succeeded and false if the object didn't exist.
/// Throws otherwise.
bool check_cloud_result(cloud_io::download_result);

/// Throws an appropriate exception for the given result.
void check_cloud_result(cloud_io::upload_result);
cloud_storage_clients::object_key join_cloud_key(
  const cloud_storage_clients::object_key& prefix, std::string_view suffix);

ss::future<> upload_file(
  cloud_io::remote& remote,
  ss::abort_source& as,
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& cloud_key,
  const std::filesystem::path& local_path,
  size_t written);

struct one_time_stream_provider : public stream_provider {
    explicit one_time_stream_provider(ss::input_stream<char> s);
    ss::input_stream<char> take_stream() override;
    ss::future<> close() override;
    std::optional<ss::input_stream<char>> _st;
};

/// Shared base for data_persistence implementations that store SST files
/// in cloud storage. Subclasses may differ in how they stage data locally
/// (e.g. staging directory vs cloud cache).
class cloud_data_persistence_base : public data_persistence {
public:
    cloud_data_persistence_base(
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key prefix);

    ss::future<> remove_file(internal::file_handle h) override;

    ss::coroutine::experimental::generator<internal::file_handle>
    list_files() override;

    ss::future<> close() override;

protected:
    /// Called before deleting from cloud.
    virtual ss::future<> remove_file_locally(std::string_view filename) = 0;

    cloud_storage_clients::object_key cloud_key(std::string_view name);

    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    cloud_storage_clients::object_key _prefix;
    ss::abort_source _as;
    ss::gate _gate;
};

} // namespace lsm::io
