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
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"

#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>

#include <optional>

namespace cloud_topics::l1 {

// The IO implementation that hides caching and other complexities of
// interacting with persistent storage of L1 objects.
//
// For writing this persists the file to the local disk, then writes it
// to object storage.
//
// Reads are cached locally on disk in the cloud cache before being returned.
class file_io : public io {
    friend class file_io_test_fixture;

public:
    /// `probe` is an externally-owned per-shard probe. Nullable:
    /// secondary per-shard file_io instances (read-replica refreshers,
    /// etc.) pass nullptr and their IO activity is not counted on the
    /// shared probe.
    file_io(
      std::filesystem::path staging_dir,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_io::cache* cache,
      file_io_probe* probe = nullptr);

    /// Drain in-flight reads. Must be co_awaited before destruction so
    /// the read_object defer-cleanup never touches a destroyed map.
    ss::future<> stop();

    /// Cloud-cache disk key for an (oid, position, size) extent. Shared
    /// between `read_object` and tests so the format stays in lockstep.
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

    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    std::filesystem::path _staging_dir;
    cloud_io::cache* _cache;

    // Gates all read_object calls so destruction can wait for any
    // suspended fibers whose defer-cleanup would otherwise touch a
    // destroyed `_inflight_downloads`.
    ss::gate _gate;

    // If two reads on the same shard miss the cloud cache on the same
    // extent, only one triggers a download. Subsequent reads merge
    // into the in-flight download via the shared promise.
    // Promise resolves to nullopt on success (merged reads can expect
    // a warm cache); otherwise it carries the errc to propagate as if
    // the download came from each merged read's own fiber.
    // Loosely mirrors the L0 read_merge pattern.
    chunked_hash_map<
      std::filesystem::path,
      ss::shared_promise<std::optional<errc>>>
      _inflight_downloads;

    // Non-owning. Null for secondary per-shard file_io instances whose
    // IO activity is intentionally not counted on the shared probe.
    file_io_probe* _probe;
};

} // namespace cloud_topics::l1
