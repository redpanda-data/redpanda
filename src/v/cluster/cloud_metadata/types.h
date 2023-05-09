/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "utils/named_type.h"

namespace cluster::cloud_metadata {

using cluster_metadata_id
  = named_type<int64_t, struct cluster_metadata_id_struct>;

} // namespace cluster::cloud_metadata
