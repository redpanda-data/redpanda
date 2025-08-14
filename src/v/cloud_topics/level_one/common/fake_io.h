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

namespace experimental::cloud_topics::l1 {

// The IO implementation that is entirely in-memory, used for testing.
class fake_io : public io {
public:
    fake_io();
    ss::future<std::expected<std::unique_ptr<staging_file>, errc>>
    create_tmp_file() override;

    ss::future<std::expected<void, errc>>
    put_object(object_id, staging_file*, ss::abort_source*) override;

    ss::future<std::expected<ss::input_stream<char>, errc>>
    read_object(object_extent, ss::abort_source*) override;

    ss::future<std::expected<void, errc>>
    delete_objects(chunked_vector<object_id>, ss::abort_source*) override;

    // Get a full object that has been put directly from storage
    std::optional<iobuf> get_object(object_id id);

    // Put an object directly into storage, bypassing staging
    void put_object(object_id id, iobuf data);

    // Directly remove an object from storage
    void remove_object(object_id id);

private:
    absl::btree_map<object_id, iobuf> _storage;
};

} // namespace experimental::cloud_topics::l1
