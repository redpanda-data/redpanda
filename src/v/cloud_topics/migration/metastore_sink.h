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

namespace cloud_topics::l1 {
class metastore;
}

namespace cloud_topics {

/// cloud_topics implementation of archival::migration_metastore: translates the
/// archiver-side imported-segment descriptors into L1 metastore ops on the
/// injected metastore client. This is the bridge that lets the migration mirror
/// run in the archiver (cluster) while writing to the L1 metastore
/// (cloud_topics) without inverting the module dependency.
class migration_metastore_sink final : public archival::migration_metastore {
public:
    explicit migration_metastore_sink(l1::metastore* ms)
      : _ms(*ms) {}

    ss::future<errc> append_imported(chunked_vector<imported_segment>) override;

    ss::future<std::optional<offsets>>
    get_offsets(const model::topic_id_partition&) override;

    ss::future<errc> mark_complete(const model::topic_id_partition&) override;

private:
    l1::metastore& _ms;
};

} // namespace cloud_topics
