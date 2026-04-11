/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/debug_writer.h"

#include "cloud_topics/level_one/metastore/lsm/debug_serde.h"

namespace cloud_topics::l1 {

std::expected<chunked_vector<write_batch_row>, debug_writer::error>
debug_writer::build_rows(
  const proto::admin::metastore::write_rows_request& req) {
    chunked_vector<write_batch_row> rows;
    rows.reserve(req.get_writes().size() + req.get_deletes().size());

    for (const auto& w : req.get_writes()) {
        auto key_res = debug_encode_key(w.get_key());
        if (!key_res) {
            auto ec = errc{key_res.error().e};
            return std::unexpected(
              std::move(key_res.error())
                .wrap(std::move(ec), "write key: {}", w.get_key()));
        }
        auto val_res = debug_encode_value(w.get_value());
        if (!val_res) {
            auto ec = errc{val_res.error().e};
            return std::unexpected(
              std::move(val_res.error())
                .wrap(std::move(ec), "write key: {}", w.get_key()));
        }
        rows.push_back(
          write_batch_row{
            .key = std::move(*key_res),
            .value = std::move(*val_res),
          });
    }

    for (const auto& d : req.get_deletes()) {
        auto key_res = debug_encode_key(d);
        if (!key_res) {
            auto ec = errc{key_res.error().e};
            return std::unexpected(
              std::move(key_res.error())
                .wrap(std::move(ec), "delete key: {}", d));
        }
        rows.push_back(
          write_batch_row{
            .key = std::move(*key_res),
            .value = iobuf{},
          });
    }

    return rows;
}

} // namespace cloud_topics::l1
