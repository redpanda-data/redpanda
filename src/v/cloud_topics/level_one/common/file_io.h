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

#include "cloud_io/remote.h"
#include "cloud_storage/cache_service.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "model/fundamental.h"

namespace experimental::cloud_topics::l1 {

// The IO implementation that hides caching and other complexities of
// interacting with persistent storage of L1 objects.
//
// For writing this persists the file to the local disk, then writes it
// to object storage.
//
// Reads are cached locally on disk in the cloud cache before being returned.
class file_io : public io {
public:
    file_io(
      std::filesystem::path staging_dir,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage::cache* cache);
    ss::future<std::expected<std::unique_ptr<staging_file>, errc>>
    create_tmp_file() override;

    ss::future<std::expected<void, errc>>
    put_object(object_id, staging_file*, ss::abort_source*) override;

    ss::future<std::expected<ss::input_stream<char>, errc>>
    read_object(object_extent, ss::abort_source*) override;

    ss::future<std::expected<void, errc>>
    delete_objects(chunked_vector<object_id>, ss::abort_source*) override;

private:
    ss::future<uint64_t> save_to_cache(
      ss::input_stream<char>,
      cloud_storage::space_reservation_guard*,
      std::filesystem::path,
      uint64_t content_length);

    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    std::filesystem::path _staging_dir;
    cloud_storage::cache* _cache;
};

} // namespace experimental::cloud_topics::l1
