// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/table_io.h"

#include "bytes/iobuf_parser.h"
#include "iceberg/table_metadata_json.h"
#include "json/chunked_buffer.h"

namespace iceberg {

ss::future<checked<table_metadata, metadata_io::errc>>
table_io::download_table_meta(const table_metadata_path& path) {
    return download_object<table_metadata>(
      path(), "iceberg::table_metadata", [](iobuf b) {
          iobuf_parser p(std::move(b));
          auto sz = p.bytes_left();
          json::Document parsed;
          parsed.Parse(p.read_string(sz));
          return parse_table_meta(parsed);
      });
}
ss::future<checked<size_t, metadata_io::errc>> table_io::upload_table_meta(
  const table_metadata_path& path, const table_metadata& m) {
    return upload_object<table_metadata>(
      path(), m, "iceberg::table_metadata", [](const table_metadata& m) {
          json::chunked_buffer b;
          iceberg::json_writer w(b);
          json::rjson_serialize(w, m);
          return std::move(b).as_iobuf();
      });
}

ss::future<checked<int, metadata_io::errc>>
table_io::download_version_hint(const version_hint_path& path) {
    return download_object<int>(path(), "iceberg::version_hint", [](iobuf b) {
        iobuf_parser p(std::move(b));
        auto sz = p.bytes_left();
        auto version_str = p.read_string(sz);
        return std::stoi(version_str);
    });
}

ss::future<checked<size_t, metadata_io::errc>>
table_io::upload_version_hint(const version_hint_path& path, int version) {
    return upload_object<int>(
      path(), version, "iceberg::version_hint", [](int v) {
          return iobuf::from(fmt::to_string(v));
      });
}

ss::future<checked<bool, metadata_io::errc>>
table_io::version_hint_exists(const version_hint_path& path) {
    return object_exists(path, "iceberg::version_hint");
}

} // namespace iceberg
