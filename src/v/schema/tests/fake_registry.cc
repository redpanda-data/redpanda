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

#include "schema/tests/fake_registry.h"

#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/log.hh>

namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,cert-err58-cpp)
static ss::logger dummy_logger("schema_test_logger");

namespace ppsr = pandaproxy::schema_registry;

bool same_schema(
  const ppsr::subject_schema& unparsed, const ppsr::subject_schema& canonical) {
    // Wrong, but works good enough for our simple testing.
    return unparsed.def().raw()() == canonical.def().raw()();
}

} // namespace

ss::future<ppsr::stored_schema> schema::fake_store::get_subject_schema(
  ppsr::context_subject sub,
  std::optional<ppsr::schema_version> version,
  ppsr::include_deleted) {
    std::optional<ppsr::stored_schema> found;
    for (const auto& s : schemas) {
        if (s.schema.sub() != sub) {
            continue;
        }
        if (version && *version != s.version) {
            continue;
        }
        if (found && found->version > s.version) {
            continue;
        }
        found.emplace(s.share());
    }
    if (!found) {
        throw as_exception(not_found(sub));
    }
    co_return std::move(found).value();
}

ss::future<ppsr::schema_definition>
schema::fake_store::get_schema_definition(ppsr::context_schema_id id) {
    for (const auto& s : schemas) {
        if (s.id == id) {
            co_return s.schema.def().share();
        }
    }
    throw std::runtime_error("unknown schema id");
}

ss::future<std::optional<ppsr::schema_definition>>
schema::fake_store::maybe_get_schema_definition(ppsr::context_schema_id id) {
    for (const auto& s : schemas) {
        if (s.id == id) {
            co_return s.schema.def().share();
        }
    }
    co_return std::nullopt;
}

void schema::fake_registry::maybe_throw_injected_failure() const {
    if (_injected_failure) {
        std::rethrow_exception(_injected_failure);
    }
}

ss::future<ppsr::schema_definition>
schema::fake_registry::get_schema_definition(ppsr::context_schema_id id) const {
    maybe_throw_injected_failure();
    return _store.get_schema_definition(id);
}
ss::future<ppsr::stored_schema> schema::fake_registry::get_subject_schema(
  ppsr::context_subject sub,
  std::optional<ppsr::schema_version> version) const {
    maybe_throw_injected_failure();
    return _store.get_subject_schema(sub, version, ppsr::include_deleted::no);
}
ss::future<ppsr::schema_getter*> schema::fake_registry::getter() const {
    maybe_throw_injected_failure();
    co_return &_store;
}
ss::future<ppsr::context_schema_id>
schema::fake_registry::create_schema(ppsr::subject_schema unparsed) {
    maybe_throw_injected_failure();
    // This is wrong, but simple for our testing.
    for (const auto& s : _store.schemas) {
        if (
          same_schema(unparsed, s.schema) && s.schema.sub() == unparsed.sub()) {
            co_return s.id;
        }
    }
    auto id = ppsr::schema_id(int32_t(_store.schemas.size() + 1));
    auto version = ppsr::schema_version(0);
    for (const auto& s : _store.schemas) {
        if (same_schema(unparsed, s.schema)) {
            id = s.id;
        }
        if (s.schema.sub() == unparsed.sub()) {
            version = std::max(version, s.version + 1);
        }
    }
    // TODO: validate references too
    _store.schemas.push_back({
      .schema = std::move(unparsed),
      .version = version,
      .id = id,
      .deleted = ppsr::is_deleted::no,
    });
    co_return _store.schemas.back().id;
}
const std::vector<ppsr::stored_schema>& schema::fake_registry::get_all() {
    maybe_throw_injected_failure();
    return _store.schemas;
}
