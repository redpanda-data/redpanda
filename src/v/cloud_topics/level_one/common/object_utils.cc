/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/object_utils.h"

#include "ssx/sformat.h"

namespace cloud_topics::l1 {

constexpr auto level_one_data_dir_str = "level_one/data/";

cloud_storage_clients::object_key
object_path_factory::level_one_path(object_id id) {
    return cloud_storage_clients::object_key(
      ssx::sformat("{}v0_{}", level_one_data_dir_str, id));
}

} // namespace cloud_topics::l1
