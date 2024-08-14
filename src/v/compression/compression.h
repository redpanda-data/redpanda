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
#include "model/compression.h"
namespace compression {

using type = model::compression;
// a very simple compressor. Exposes virtually no knobs and uses
// the defaults for all compressors. In the future, we can make these
// a virtual interface so we can instantiate them
struct compressor {
    static iobuf compress(const iobuf&, type);
    static iobuf uncompress(const iobuf&, type);
};

// A simple opinionated stream compressor.
//
// Will use stream compression when available, to defer to compressor.
struct stream_compressor {
    static ss::future<iobuf> compress(iobuf, type);
    static ss::future<iobuf> uncompress(iobuf, type);
};

} // namespace compression
