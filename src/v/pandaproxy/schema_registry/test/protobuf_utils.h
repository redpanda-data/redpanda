// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

namespace pandaproxy::schema_registry::test_utils {
struct simple_sharded_store {
    explicit simple_sharded_store()
      : store{} {
        store.start(pps::is_mutable::yes, ss::default_smp_service_group())
          .get();
    }
    ~simple_sharded_store() { store.stop().get(); }
    simple_sharded_store(const simple_sharded_store&) = delete;
    simple_sharded_store(simple_sharded_store&&) = delete;
    simple_sharded_store& operator=(const simple_sharded_store&) = delete;
    simple_sharded_store& operator=(simple_sharded_store&&) = delete;

    pps::schema_id
    insert(const pps::subject_schema& schema, pps::schema_version version) {
        const auto id = next_id++;
        store
          .upsert(
            pps::seq_marker{
              .seq = std::nullopt,
              .node = std::nullopt,
              .version = version,
              .key_type = pps::seq_marker_key_type::schema},
            schema.share(),
            id,
            version,
            pps::is_deleted::no)
          .get();
        return id;
    }

    pps::schema_id next_id{1};
    pps::sharded_store store;
};

ss::sstring make_proto_schema(const pps::subject& sub, int n_fields);

std::string sanitize(
  std::string_view raw_proto,
  pps::normalize norm = pps::normalize::no,
  pps::output_format format = pps::output_format::none);

std::string normalize(std::string_view raw_proto);

} // namespace pandaproxy::schema_registry::test_utils
