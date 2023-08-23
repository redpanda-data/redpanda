/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "utils/delta_for.h"

#include <cstdint>
#include <span>

struct deltafor_datapoint {
    using dfor_enc = deltafor_encoder<
      int64_t,
      details::delta_xor,
      true,
      details::delta_xor{}>;
    using dfor_dec = deltafor_decoder<int64_t, details::delta_xor>;
    std::span<const int64_t, details::FOR_buffer_depth> reference_data;
    std::span<const uint8_t> encoded_data;
};

auto get_characterization_data() -> std::span<const deltafor_datapoint>;
