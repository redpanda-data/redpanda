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
#include "random/generators.h"

#include <istream>
#include <random>

namespace transform::logging::testing {

json::Document parse_json(iobuf resp) {
    iobuf_istreambuf ibuf{resp};
    std::istream stream{&ibuf};
    json::Document doc;
    json::IStreamWrapper wrapper(stream);
    doc.ParseStream(wrapper);
    return doc;
}

std::string get_message_body(iobuf msg) {
    auto doc = parse_json(std::move(msg));
    return {doc["body"]["stringValue"].GetString()};
}

model::transform_name random_transform_name(size_t len) {
    return model::transform_name{random_generators::gen_alphanum_string(len)};
}

iobuf random_length_iobuf(size_t data_max) {
    assert(data_max > 0);
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(1, data_max);

    auto data = random_generators::gen_alphanum_string(dist(rng));
    iobuf b;
    b.append(data.data(), data.size());
    return b;
}

} // namespace transform::logging::testing
