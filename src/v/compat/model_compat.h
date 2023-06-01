/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include "compat/check.h"
#include "compat/json.h"
#include "compat/model_generator.h"
#include "model/metadata.h"
#include "model/record.h"

namespace compat {

GEN_COMPAT_CHECK(
  model::partition_metadata,
  {
      json_write(id);
      json_write(replicas);
      json_write(leader_node);
  },
  {
      json_read(id);
      json_read(replicas);
      json_read(leader_node);
  })

GEN_COMPAT_CHECK(
  model::topic_metadata,
  {
      json_write(tp_ns);
      json_write(partitions);
  },
  {
      json_read(tp_ns);
      json_read(partitions);
  });

GEN_COMPAT_CHECK(
  model::record_batch_header,
  {
      json_write(header_crc);
      json_write(size_bytes);
      json_write(type);
      json_write(base_offset);
      json_write(crc);
      json_write(attrs);
      json_write(last_offset_delta);
      json_write(first_timestamp);
      json_write(max_timestamp);
      json_write(producer_id);
      json_write(producer_epoch);
      json_write(base_sequence);
      json_write(record_count);
  },
  {
      json_read(header_crc);
      json_read(size_bytes);
      json_read(type);
      json_read(base_offset);
      json_read(crc);
      json_read(attrs);
      json_read(last_offset_delta);
      json_read(first_timestamp);
      json_read(max_timestamp);
      json_read(producer_id);
      json_read(producer_epoch);
      json_read(base_sequence);
      json_read(record_count);
  });

template<>
struct compat_check<model::record_batch> {
    static constexpr std::string_view name = "model::record_batch";

    static std::vector<model::record_batch> create_test_cases() {
        return generate_instances<model::record_batch>();
    }

    static void
    to_json(model::record_batch b, json::Writer<json::StringBuffer>& wr) {
        json::write_member(wr, "header", b.header());
        json::write_member(wr, "data", b.data().copy());
    }

    static model::record_batch from_json(json::Value& v) {
        model::record_batch_header header;
        iobuf data;
        json::read_member(v, "header", header);
        json::read_member(v, "data", data);

        return {header, std::move(data)};
    }

    static std::vector<compat_binary> to_binary(model::record_batch obj) {
        return {compat_binary::serde(obj.copy())};
    }

    static void check(model::record_batch obj, compat_binary test) {
        verify_serde_only(obj.copy(), std::move(test));
    }
};

} // namespace compat
