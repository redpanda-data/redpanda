/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/topic_metadata_item.h"

namespace cluster {

bool topic_metadata_item::is_topic_replicable() const {
    return metadata.is_topic_replicable();
}

assignments_set& topic_metadata_item::get_assignments() {
    return metadata.get_assignments();
}

const assignments_set& topic_metadata_item::get_assignments() const {
    return metadata.get_assignments();
}
model::revision_id topic_metadata_item::get_revision() const {
    return metadata.get_revision();
}
std::optional<model::initial_revision_id>
topic_metadata_item::get_remote_revision() const {
    return metadata.get_remote_revision();
}
const model::topic& topic_metadata_item::get_source_topic() const {
    return metadata.get_source_topic();
}

const topic_configuration& topic_metadata_item::get_configuration() const {
    return metadata.get_configuration();
}
topic_configuration& topic_metadata_item::get_configuration() {
    return metadata.get_configuration();
}

} // namespace cluster
