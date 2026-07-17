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

#include "container/chunked_hash_map.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

#include <algorithm>
#include <ranges>
#include <utility>

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
        if (s.context_id() == id) {
            co_return s.schema.def().share();
        }
    }
    throw std::runtime_error("unknown schema id");
}

ss::future<std::optional<ppsr::schema_definition>>
schema::fake_store::maybe_get_schema_definition(ppsr::context_schema_id id) {
    for (const auto& s : schemas) {
        if (s.context_id() == id) {
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
ss::future<chunked_vector<ppsr::subject_version_deleted>>
schema::fake_registry::list_subject_versions(
  ss::noncopyable_function<bool(const ppsr::context_subject&)> filter,
  ppsr::include_deleted inc_del) const {
    maybe_throw_injected_failure();
    chunked_vector<ppsr::subject_version_deleted> out;
    for (const auto& s : _store.schemas) {
        if (!filter(s.schema.sub())) {
            continue;
        }
        if (!inc_del && s.deleted) {
            continue;
        }
        out.push_back({s.schema.sub(), s.version, s.deleted});
    }
    co_return out;
}

ss::future<bool> schema::fake_registry::has_subjects(
  ppsr::context ctx, ppsr::include_deleted inc_del) const {
    maybe_throw_injected_failure();
    co_return std::ranges::any_of(_store.schemas, [&](const auto& s) {
        return s.schema.sub().ctx == ctx && (inc_del || !s.deleted);
    });
}

ss::future<chunked_vector<ppsr::context_subject>>
schema::fake_registry::get_subjects(ppsr::include_deleted inc_del) const {
    maybe_throw_injected_failure();
    co_return _store.schemas | std::views::filter([inc_del](const auto& s) {
        return inc_del || !s.deleted;
    }) | std::views::transform([](const auto& s) { return s.schema.sub(); })
      | std::ranges::to<chunked_hash_set<ppsr::context_subject>>()
      | std::ranges::to<chunked_vector<ppsr::context_subject>>();
}

ss::future<chunked_vector<ppsr::context>>
schema::fake_registry::list_contexts() const {
    maybe_throw_injected_failure();
    // `_contexts` holds only non-default contexts (default is always present
    // and never gets a CONTEXT record), so seed with the default and copy the
    // rest.
    auto out = chunked_vector{ppsr::default_context};
    std::ranges::copy(_contexts, std::back_inserter(out));
    co_return out;
}

ss::future<ppsr::context_schema_id>
schema::fake_registry::create_schema(ppsr::subject_schema unparsed) {
    maybe_throw_injected_failure();
    if (unparsed.sub().ctx != ppsr::default_context) {
        _contexts.insert(unparsed.sub().ctx);
    }
    // This is wrong, but simple for our testing.
    for (const auto& s : _store.schemas) {
        if (
          same_schema(unparsed, s.schema) && s.schema.sub() == unparsed.sub()) {
            co_return s.context_id();
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
    co_return _store.schemas.back().context_id();
}

ss::future<ppsr::context_schema_id>
schema::fake_registry::import_schema(ppsr::stored_schema imported) {
    maybe_throw_injected_failure();
    if (imported.schema.sub().ctx != ppsr::default_context) {
        _contexts.insert(imported.schema.sub().ctx);
    }

    for (const auto& s : _store.schemas) {
        if (
          s.context_id() == imported.context_id()
          && s.schema.def() != imported.schema.def()) {
            throw as_exception(
              ppsr::overwrite_schema_with_id_not_permitted(imported.id));
        }
    }

    for (auto& s : _store.schemas) {
        if (
          s.schema.sub() == imported.schema.sub()
          && s.version == imported.version) {
            if (s.id != imported.id) {
                throw as_exception(
                  ppsr::error_info{
                    ppsr::error_code::subject_version_schema_id_already_exists,
                    fmt::format(
                      "Subject {} version {} is already registered with "
                      "schema id {}, not imported schema id {}",
                      imported.schema.sub(),
                      imported.version(),
                      s.id(),
                      imported.id())});
            }
            s = imported.share();
            co_return s.context_id();
        }
    }

    _store.schemas.push_back(imported.share());
    co_return imported.context_id();
}

ss::future<bool> schema::fake_registry::soft_delete_schema(
  ppsr::context_subject sub, ppsr::schema_version version) {
    maybe_throw_injected_failure();
    for (auto& s : _store.schemas) {
        if (s.schema.sub() == sub && s.version == version) {
            auto was_deleted = std::exchange(s.deleted, ppsr::is_deleted::yes);
            co_return was_deleted == ppsr::is_deleted::no;
        }
    }
    throw as_exception(ppsr::not_found(sub, version));
}

ss::future<chunked_vector<ppsr::schema_version>>
schema::fake_registry::permanent_delete_schema(
  ppsr::context_subject sub, std::optional<ppsr::schema_version> version) {
    maybe_throw_injected_failure();
    chunked_vector<ppsr::schema_version> deleted;
    std::erase_if(_store.schemas, [&](const ppsr::stored_schema& schema) {
        const auto matches
          = schema.schema.sub() == sub
            && (!version.has_value() || schema.version == *version);
        if (matches) {
            deleted.push_back(schema.version);
        }
        return matches;
    });
    if (deleted.empty()) {
        throw as_exception(
          version.has_value() ? ppsr::not_found(sub, *version)
                              : ppsr::not_found(sub));
    }
    co_return deleted;
}

ss::future<bool>
schema::fake_registry::write_mode(ppsr::context_subject sub, ppsr::mode mode) {
    maybe_throw_injected_failure();
    auto it = _modes.find(sub);
    if (it == _modes.end()) {
        _modes.emplace(std::move(sub), mode);
        co_return true;
    }
    if (it->second == mode) {
        co_return false;
    }
    it->second = mode;
    co_return true;
}

ss::future<bool> schema::fake_registry::delete_mode(ppsr::context_subject sub) {
    maybe_throw_injected_failure();
    co_return _modes.erase(sub) > 0;
}

ss::future<bool> schema::fake_registry::write_config(
  ppsr::context_subject sub, ppsr::compatibility_level compat) {
    maybe_throw_injected_failure();
    auto it = _configs.find(sub);
    if (it == _configs.end()) {
        _configs.emplace(std::move(sub), compat);
        co_return true;
    }
    if (it->second == compat) {
        co_return false;
    }
    it->second = compat;
    co_return true;
}

ss::future<bool>
schema::fake_registry::delete_config(ppsr::context_subject sub) {
    maybe_throw_injected_failure();
    co_return _configs.erase(sub) > 0;
}

ss::future<> schema::fake_registry::delete_context(ppsr::context ctx) {
    maybe_throw_injected_failure();
    if (ctx == ppsr::default_context) {
        throw as_exception(ppsr::cannot_delete_default_context());
    }
    // Mirror the real store: an unmaterialized context is not found.
    if (!_contexts.contains(ctx)) {
        throw as_exception(
          ppsr::error_info{
            ppsr::error_code::subject_not_found,
            fmt::format("Context '{}' not found", ctx())});
    }
    // Mirror the real store: a context can only be tombstoned once empty,
    // counting soft-deleted subjects (include_deleted::yes).
    const bool not_empty = std::ranges::any_of(
      _store.schemas, [&](const auto& s) { return s.schema.sub().ctx == ctx; });
    if (not_empty) {
        throw as_exception(ppsr::context_not_empty(ctx));
    }
    _contexts.erase(ctx);
    co_return;
}

const std::vector<ppsr::stored_schema>& schema::fake_registry::get_all() {
    maybe_throw_injected_failure();
    return _store.schemas;
}
