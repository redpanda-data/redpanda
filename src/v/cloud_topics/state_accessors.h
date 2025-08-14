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

namespace experimental::cloud_topics {

class data_plane_api;

// Encapsulates the required bits to access topic state from cloud topics,
// with minimal dependencies. This allows it to be passed around through
// different layers without introducing circular dependencies.
//
class state_accessors {
public:
    explicit state_accessors(data_plane_api* data_plane)
      : data_plane(data_plane) {}

    data_plane_api* get_data_plane() { return data_plane; }

private:
    data_plane_api* data_plane;
};
} // namespace experimental::cloud_topics
