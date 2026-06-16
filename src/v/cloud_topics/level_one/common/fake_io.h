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

#include "absl/container/btree_map.h"
#include "bytes/iobuf.h"
#include "cloud_topics/level_one/common/abstract_io.h"

#include <vector>

namespace cloud_topics::l1 {

// The IO implementation that is entirely in-memory, used for testing.
class fake_io : public io {
public:
    fake_io();
    ss::future<std::expected<std::unique_ptr<staging_file>, errc>>
    create_tmp_file() override;

    ss::future<std::expected<void, errc>>
    put_object(object_id, staging_file*, ss::abort_source*) override;

    ss::future<std::expected<ss::input_stream<char>, errc>> read_object(
      object_extent,
      ss::abort_source*,
      cloud_io::group_id g,
      bool skip_cache) override;

    ss::future<std::expected<void, errc>>
    delete_objects(chunked_vector<object_id>, ss::abort_source*) override;

    ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, errc>>
    create_multipart_upload(
      object_id, size_t part_size, ss::abort_source*) override;

    // Get a full object that has been put directly from storage
    std::optional<iobuf> get_object(object_id id);

    // Put an object directly into storage, bypassing staging
    void put_object(object_id id, iobuf data);

    // Directly remove an object from storage
    void remove_object(object_id id);

    // Return a list of the object IDs that haven't been removed.
    chunked_vector<object_id> list_objects() const;

    /// A recorded read_object call, so tests can assert how open_object chunks
    /// a read (chunk size / chunk alignment) and whether it bypasses the cache.
    struct read_object_call {
        size_t position;
        size_t size;
        bool skip_cache;
        bool imported;
    };
    const std::vector<read_object_call>& read_object_calls() const {
        return _read_object_calls;
    }

private:
    absl::btree_map<object_id, iobuf> _storage;
    std::vector<read_object_call> _read_object_calls;
};

} // namespace cloud_topics::l1
