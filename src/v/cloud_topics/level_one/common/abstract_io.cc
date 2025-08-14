/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/abstract_io.h"

namespace experimental::cloud_topics::l1 {

ss::future<ss::input_stream<char>> io::read_file(staging_file* file) {
    return file->input_stream();
}

} // namespace experimental::cloud_topics::l1
