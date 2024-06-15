// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cloud_storage/remote_label.h"
#include "model/metadata.h"

#include <optional>

namespace cloud_storage {

class remote_path_provider {
public:
    explicit remote_path_provider(std::optional<remote_label> label);

    ss::sstring
    topic_manifest_prefix(const model::topic_namespace& topic) const;
    ss::sstring topic_manifest_path(
      const model::topic_namespace& topic, model::initial_revision_id) const;

private:
    std::optional<remote_label> label_;
};

} // namespace cloud_storage
