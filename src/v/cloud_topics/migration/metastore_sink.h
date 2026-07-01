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

#include "cluster/archival/migration_metastore.h"
#include "model/fundamental.h"

#include <optional>

namespace cluster {
class metadata_cache;
}
namespace cloud_topics::l1 {
class metastore;
}

namespace cloud_topics {

/// cloud_topics implementation of archival::migration_metastore: translates the
/// archiver-side imported-segment descriptors into L1 metastore ops on the
/// injected metastore client. This is the bridge that lets the migration mirror
/// run in the archiver (cluster) while writing to the L1 metastore
/// (cloud_topics) without inverting the module dependency. It resolves the
/// archiver's ntp to the L1 topic_id_partition via the metadata cache.
class migration_metastore_sink final : public archival::migration_metastore {
public:
    migration_metastore_sink(l1::metastore* ms, cluster::metadata_cache* md)
      : _ms(*ms)
      , _md(md) {}

    // Stateless wrapper; nothing to tear down (provided so it can be hosted in
    // an ss::sharded<>).
    ss::future<> stop() { return ss::now(); }

    ss::future<errc> append_imported(
      const model::ntp&, chunked_vector<imported_segment>) override;

    ss::future<std::optional<offsets>> get_offsets(const model::ntp&) override;

    ss::future<errc> mark_complete(const model::ntp&) override;

private:
    // Resolve the archiver's ntp to the L1 metastore's topic_id_partition (the
    // cloud-topic id lives in the topic config, not the ntp). nullopt if the
    // topic config or its cloud-topic id is unknown.
    std::optional<model::topic_id_partition> resolve(const model::ntp&) const;

    l1::metastore& _ms;
    cluster::metadata_cache* _md;
};

} // namespace cloud_topics
