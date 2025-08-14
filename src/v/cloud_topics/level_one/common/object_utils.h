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

#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_one/common/object_id.h"

namespace experimental::cloud_topics::l1 {

/*
 * Utilities for working with the object storage paths.
 */
class object_path_factory {
public:
    /*
     * Generate the path of a level-one object.
     */
    static cloud_storage_clients::object_key level_one_path(object_id);
};

} // namespace experimental::cloud_topics::l1
