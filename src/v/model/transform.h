/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/metadata.h"
#include "seastarx.h"
#include "serde/envelope.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>

#include <cstdint>

namespace model {
/**
 * An ID for a transform, these get allocated globally, to allow for users to
 * re-use names of transforms.
 */
using transform_id = named_type<int64_t, struct transform_id_tag>;
/**
 * The name of a transform, which is the user defined.
 *
 * Generally you should only use names when surfacing information to a user,
 * otherwise the ID should be used, especially if the information is persisted.
 */
using transform_name = named_type<ss::sstring, struct transform_name_tag>;

/**
 * Metadata for a WebAssembly powered data transforms.
 */
struct transform_metadata
  : serde::envelope<
      transform_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    // The user specified name of the transform.
    transform_name name;
    // The input topic being transformed.
    model::topic_namespace input_topic;
    // Right now we force there is only one, but we're mostly setup for
    // multiple. These are also validated to be unique, but we use a vector
    // here to preserve the user specified order (which is important as it
    // will be how the ABI boundary specifies which output topic to write too).
    std::vector<model::topic_namespace> output_topics;
    // The user specified environment variable configuration.
    absl::flat_hash_map<ss::sstring, ss::sstring> environment;
    // Each transform revision has a UUID, which is the key for the wasm_binary
    // topic and can be used to uniquely identify this revision.
    uuid_t uuid;
    // The offset of the committed WASM source in the wasm_binary topic.
    model::offset source_ptr;

    friend bool operator==(const transform_metadata&, const transform_metadata&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const transform_metadata&);

    auto serde_fields() {
        return std::tie(
          name, input_topic, output_topics, environment, uuid, source_ptr);
    }
};
} // namespace model
