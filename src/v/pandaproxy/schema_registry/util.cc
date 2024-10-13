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

#include "pandaproxy/schema_registry/util.h"

namespace pandaproxy::schema_registry {

ss::future<collected_schema> collect_schema(
  schema_getter& store,
  collected_schema collected,
  ss::sstring name,
  canonical_schema schema) {
    for (const auto& ref : schema.def().refs()) {
        if (!collected.contains(ref.name)) {
            auto ss = co_await store.get_subject_schema(
              ref.sub, ref.version, include_deleted::no);
            collected = co_await collect_schema(
              store, std::move(collected), ref.name, std::move(ss.schema));
        }
    }
    collected.insert(std::move(name), std::move(schema).def());

    co_return std::move(collected);
}
} // namespace pandaproxy::schema_registry
