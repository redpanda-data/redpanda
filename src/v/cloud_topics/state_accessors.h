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

#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"

namespace cluster {
class metadata_cache;
}

namespace cloud_topics {

class data_plane_api;
class l1_reader_cache;

namespace l1 {
class metastore;
class io;
} // namespace l1

namespace read_replica {
class metadata_provider;
class snapshot_provider;
} // namespace read_replica

// Encapsulates the required bits to access topic state from cloud topics,
// with minimal dependencies. This allows it to be passed around through
// different layers without introducing circular dependencies.
//
class state_accessors {
public:
    explicit state_accessors(
      data_plane_api* data_plane,
      l1::metastore* metastore,
      l1::io* io,
      cluster::metadata_cache* metadata_cache,
      level_one_reader_probe* l1_reader_probe,
      l1_reader_cache* l1_reader_cache_,
      read_replica::metadata_provider* rr_metadata_provider,
      read_replica::snapshot_provider* rr_snapshot_provider)
      : data_plane(data_plane)
      , l1_metastore(metastore)
      , l1_io(io)
      , metadata_cache(metadata_cache)
      , l1_reader_probe(l1_reader_probe)
      , l1_reader_cache_(l1_reader_cache_)
      , rr_metadata_provider_(rr_metadata_provider)
      , rr_snapshot_provider_(rr_snapshot_provider) {}

    data_plane_api* get_data_plane() { return data_plane; }
    l1::metastore* get_l1_metastore() { return l1_metastore; }
    l1::io* get_l1_io() { return l1_io; }
    level_one_reader_probe* get_l1_reader_probe() { return l1_reader_probe; }
    cluster::metadata_cache* get_metadata_cache() { return metadata_cache; }
    l1_reader_cache* get_l1_reader_cache() { return l1_reader_cache_; }
    read_replica::metadata_provider* get_rr_metadata_provider() {
        return rr_metadata_provider_;
    }
    read_replica::snapshot_provider* get_rr_snapshot_provider() {
        return rr_snapshot_provider_;
    }

private:
    data_plane_api* data_plane;
    l1::metastore* l1_metastore;
    l1::io* l1_io;
    cluster::metadata_cache* metadata_cache;
    level_one_reader_probe* l1_reader_probe;
    l1_reader_cache* l1_reader_cache_;
    read_replica::metadata_provider* rr_metadata_provider_;
    read_replica::snapshot_provider* rr_snapshot_provider_;
};
} // namespace cloud_topics
