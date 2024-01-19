/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/logging/tests/utils.h"

#include "bytes/streambuf.h"
#include "json/istreamwrapper.h"

#include <istream>

namespace transform::logging::testing {

json::Document parse_json(iobuf resp) {
    iobuf_istreambuf ibuf{resp};
    std::istream stream{&ibuf};
    json::Document doc;
    json::IStreamWrapper wrapper(stream);
    doc.ParseStream(wrapper);
    return doc;
}

} // namespace transform::logging::testing
