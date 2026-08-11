// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/test/protobuf_utils.h"

#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/test/store_fixture.h"

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

namespace pandaproxy::schema_registry::test_utils {

ss::sstring make_proto_schema(const pps::subject& sub, int n_fields) {
    ss::sstring body = ss::format(
      "syntax = \"proto3\";\nmessage MyType{} {{\n", sub);
    for (int32_t i = 1; i <= n_fields; ++i) {
        body += ss::format("\tint32 i{} = {};\n", i, i);
    }
    body += "}\n";
    return body;
}

std::string sanitize(
  std::string_view raw_proto, pps::normalize norm, pps::output_format format) {
    store_fixture s;
    iobuf buf = pps::make_canonical_protobuf_schema(
                  s.store(),
                  pps::subject_schema{
                    pps::subject{"foo"},
                    pps::schema_definition{
                      raw_proto, pps::schema_type::protobuf, {}}},
                  norm,
                  format)
                  .get()
                  .def()
                  .raw()();
    iobuf_parser parser{std::move(buf)};
    return parser.read_string(parser.bytes_left());
}

std::string normalize(std::string_view raw_proto) {
    return sanitize(raw_proto, pps::normalize::yes);
}

} // namespace pandaproxy::schema_registry::test_utils
