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

#include "base/format_to.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/types.h"

#include <expected>

namespace cloud_topics {

/*
 * Utilities for working with the object storage paths.
 */
class object_path_factory {
public:
    /*
     * Generate the path of a level-zero object.
     */
    static cloud_storage_clients::object_key level_zero_path(object_id id);

    /*
     * Level-zero object data root directory. Contains a trailing "/".
     */
    static cloud_storage_clients::object_key level_zero_data_dir();

    /*
     * Extract the epoch from an L0 object key.
     */
    static std::expected<cluster_epoch, std::string>
      level_zero_path_to_epoch(std::string_view);

    static std::expected<object_id::prefix_t, std::string>
      level_zero_path_to_prefix(std::string_view);
};

struct prefix_range_inclusive {
    using T = object_id::prefix_t;
    static_assert(std::is_unsigned_v<T>);
    static constexpr T t_max = object_id::prefix_max;
    T min;
    T max;
    prefix_range_inclusive(T min, T max);
    bool contains(T v) const;
    bool operator==(const prefix_range_inclusive& other) const;
    fmt::iterator format_to(fmt::iterator it) const;
};

} // namespace cloud_topics
