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
#pragma once

#include "container/chunked_hash_map.h"
#include "pandaproxy/schema_registry/schema_getter.h"
#include "schema/registry.h"

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <map>

namespace schema {

struct fake_store : public pandaproxy::schema_registry::schema_getter {
public:
    ss::future<pandaproxy::schema_registry::stored_schema> get_subject_schema(
      pandaproxy::schema_registry::context_subject sub,
      std::optional<pandaproxy::schema_registry::schema_version> version,
      pandaproxy::schema_registry::include_deleted inc_dec) final;
    ss::future<pandaproxy::schema_registry::schema_definition>
    get_schema_definition(
      pandaproxy::schema_registry::context_schema_id id) final;
    ss::future<std::optional<pandaproxy::schema_registry::schema_definition>>
    maybe_get_schema_definition(
      pandaproxy::schema_registry::context_schema_id id) final;

    std::vector<pandaproxy::schema_registry::stored_schema> schemas;
};

// This is a fake schema registry for testing. Schemas are maintained in local
// memory only, not replicated or persisted to stable storage.
class fake_registry : public schema::registry {
public:
    bool is_enabled() const override { return true; };

    ss::future<> ensure_internal_topic() override { return ss::now(); }

    ss::future<pandaproxy::schema_registry::schema_getter*>
    getter() const override;
    ss::future<pandaproxy::schema_registry::schema_getter*>
    synced_getter() const override {
        return getter();
    }

    ss::future<ss::lowres_clock::time_point>
    sync(ss::lowres_clock::duration) override {
        return ss::make_ready_future<ss::lowres_clock::time_point>(
          ss::lowres_clock::now());
    }

    ss::future<pandaproxy::schema_registry::schema_definition>
    get_schema_definition(
      pandaproxy::schema_registry::context_schema_id id) const override;

    ss::future<pandaproxy::schema_registry::stored_schema> get_subject_schema(
      pandaproxy::schema_registry::context_subject sub,
      std::optional<pandaproxy::schema_registry::schema_version> version)
      const override;

    ss::future<
      chunked_vector<pandaproxy::schema_registry::subject_version_deleted>>
    list_subject_versions(
      ss::noncopyable_function<
        bool(const pandaproxy::schema_registry::context_subject&)> filter,
      pandaproxy::schema_registry::include_deleted inc_del) const override;

    ss::future<bool> has_subjects(
      pandaproxy::schema_registry::context ctx,
      pandaproxy::schema_registry::include_deleted inc_del) const override;

    ss::future<chunked_vector<pandaproxy::schema_registry::context_subject>>
    get_subjects(
      pandaproxy::schema_registry::include_deleted inc_del) const override;

    ss::future<chunked_vector<pandaproxy::schema_registry::context>>
    list_contexts() const override;

    ss::future<pandaproxy::schema_registry::context_schema_id> create_schema(
      pandaproxy::schema_registry::subject_schema unparsed) override;

    ss::future<pandaproxy::schema_registry::context_schema_id>
    import_schema(pandaproxy::schema_registry::stored_schema imported) override;

    ss::future<bool> soft_delete_schema(
      pandaproxy::schema_registry::context_subject sub,
      pandaproxy::schema_registry::schema_version version) override;

    ss::future<chunked_vector<pandaproxy::schema_registry::schema_version>>
    permanent_delete_schema(
      pandaproxy::schema_registry::context_subject sub,
      std::optional<pandaproxy::schema_registry::schema_version> version)
      override;

    ss::future<bool> write_mode(
      pandaproxy::schema_registry::context_subject sub,
      pandaproxy::schema_registry::mode mode) override;

    ss::future<bool>
    delete_mode(pandaproxy::schema_registry::context_subject sub) override;

    ss::future<bool> write_config(
      pandaproxy::schema_registry::context_subject sub,
      pandaproxy::schema_registry::compatibility_level compat) override;

    ss::future<bool>
    delete_config(pandaproxy::schema_registry::context_subject sub) override;

    ss::future<>
    delete_context(pandaproxy::schema_registry::context ctx) override;

    const std::vector<pandaproxy::schema_registry::stored_schema>& get_all();

    /// Test accessors for the mode/config overrides written to the registry, so
    /// a test can assert what mode/config replication applied.
    const std::map<
      pandaproxy::schema_registry::context_subject,
      pandaproxy::schema_registry::mode>&
    modes() const {
        return _modes;
    }
    const std::map<
      pandaproxy::schema_registry::context_subject,
      pandaproxy::schema_registry::compatibility_level>&
    configs() const {
        return _configs;
    }

    void set_inject_failures(const std::exception_ptr& injected) {
        _injected_failure = injected;
    }

private:
    void maybe_throw_injected_failure() const;

    std::exception_ptr _injected_failure;
    mutable fake_store _store;
    // Non-default contexts materialized by an import/create, mirroring the real
    // store's lazy CONTEXT record: a context stays listed after its subjects
    // are purged until delete_context tombstones it.
    chunked_hash_set<pandaproxy::schema_registry::context> _contexts;
    std::map<
      pandaproxy::schema_registry::context_subject,
      pandaproxy::schema_registry::mode>
      _modes;
    std::map<
      pandaproxy::schema_registry::context_subject,
      pandaproxy::schema_registry::compatibility_level>
      _configs;
};
} // namespace schema
