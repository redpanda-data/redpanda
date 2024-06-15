// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cloud_storage/remote_path_provider.h"

#include "cloud_storage/remote_label.h"
#include "cloud_storage/topic_path_utils.h"

namespace cloud_storage {

remote_path_provider::remote_path_provider(std::optional<remote_label> label)
  : label_(label) {}

ss::sstring remote_path_provider::topic_manifest_prefix(
  const model::topic_namespace& topic) const {
    if (label_.has_value()) {
        return labeled_topic_manifest_prefix(*label_, topic);
    }
    return prefixed_topic_manifest_prefix(topic);
}

ss::sstring remote_path_provider::topic_manifest_path(
  const model::topic_namespace& topic, model::initial_revision_id rev) const {
    if (label_.has_value()) {
        return labeled_topic_manifest_path(*label_, topic, rev);
    }
    return prefixed_topic_manifest_bin_path(topic);
}

} // namespace cloud_storage
