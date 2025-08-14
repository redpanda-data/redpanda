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

namespace experimental::cloud_topics::l1 {

cloud_storage_clients::object_key
object_path_factory::level_one_path(object_id id) {
    return cloud_storage_clients::object_key(ssx::sformat("l1_v0_{}", id));
}

} // namespace experimental::cloud_topics::l1
