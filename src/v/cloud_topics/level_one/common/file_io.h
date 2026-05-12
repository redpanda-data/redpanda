/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_io/cache_service.h"
#include "cloud_io/remote.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/file_io_probe.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/common/single_flight.h"
#include "model/fundamental.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/gate.hh>

#include <optional>

namespace cloud_topics::l1 {

// The IO implementation that hides caching and other complexities of
// interacting with persistent storage of L1 objects.
//
// For writing this persists the file to the local disk, then writes it
// to object storage.
//
// Reads are cached locally on disk in the cloud cache before being
// returned.
class file_io : public io {
public:
    /// `probe` is nullable so tests can opt out.
    file_io(
      std::filesystem::path staging_dir,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_io::cache* cache,
      file_io_probe* probe = nullptr);

    ss::future<> stop();

    /// Cloud-cache disk key for an (oid, position, size) extent.
    static std::filesystem::path cache_key(const object_extent& extent);

    ss::future<std::expected<std::unique_ptr<staging_file>, errc>>
    create_tmp_file() override;

    ss::future<std::expected<void, errc>>
    put_object(object_id, staging_file*, ss::abort_source*) override;

    ss::future<std::expected<ss::input_stream<char>, errc>> read_object(
      object_extent, ss::abort_source*, cloud_io::group_id g) override;

    ss::future<std::expected<void, errc>>
    delete_objects(chunked_vector<object_id>, ss::abort_source*) override;

    ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, errc>>
    create_multipart_upload(
      object_id, size_t part_size, ss::abort_source*) override;

private:
    ss::future<uint64_t> save_to_cache(
      ss::input_stream<char>,
      cloud_io::space_reservation_guard*,
      std::filesystem::path,
      uint64_t content_length);

    /// Reserve cache space, run the S3 GET, and stream the bytes into
    /// the cloud cache under `cache_key`. Returns nullopt on success,
    /// or the mapped errc on reservation / download failure.
    ss::future<single_flight::outcome> do_download_to_cache(
      const object_extent& extent,
      const std::filesystem::path& cache_key,
      retry_chain_node& root,
      ss::abort_source& as,
      cloud_io::group_id gid);

    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    std::filesystem::path _staging_dir;
    cloud_io::cache* _cache;

    ss::gate _gate;

    // Per-shard single-flight coordinator. Helps avoid redundant cloud
    // storage I/O when two readers miss the cloud cache on the same
    // extent.
    single_flight _single_flight;

    // Non-owning.
    file_io_probe* _probe;
};

} // namespace cloud_topics::l1
