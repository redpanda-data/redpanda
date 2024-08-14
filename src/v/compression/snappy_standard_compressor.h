/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"

namespace compression {

struct snappy_standard_compressor {
    static iobuf compress(const iobuf&);
    static iobuf uncompress(const iobuf&);

    /*
     * uncompress data from the input iobuf and append to the output iobuf. the
     * input data is expected to have the output size bytes.
     */
    static void uncompress_append(const iobuf&, iobuf&, size_t output_size);

    static size_t get_uncompressed_length(const iobuf&);
};

} // namespace compression
