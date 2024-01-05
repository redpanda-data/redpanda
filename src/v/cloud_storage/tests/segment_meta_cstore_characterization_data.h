#pragma once

/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include <cstdint>
#include <span>

struct segment_meta_cstore_datapoint {
    std::span<const uint8_t> uncompressed;
    std::span<const uint8_t> compressed;
};
auto get_segment_meta_cstore_characterization_data()
  -> segment_meta_cstore_datapoint;
