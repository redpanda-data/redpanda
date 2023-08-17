/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "wasm/schema_registry.h"

#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/sharded_store.h"

#include <seastar/core/sharded.hh>

#include <memory>
#include <stdexcept>

namespace wasm {

namespace {

namespace ppsr = pandaproxy::schema_registry;

class schema_registry_impl : public schema_registry {
public:
    schema_registry_impl(
      ppsr::sharded_store* store, ss::sharded<ppsr::seq_writer>* writer)
      : _store(store)
      , _writer(writer) {}

    bool is_enabled() const override { return true; };

    ss::future<ppsr::canonical_schema_definition>
    get_schema_definition(ppsr::schema_id id) const override {
        return _store->get_schema_definition(id);
    }
    ss::future<ppsr::subject_schema> get_subject_schema(
      ppsr::subject sub,
      std::optional<ppsr::schema_version> version) const override {
        return _store->get_subject_schema(
          sub, version, ppsr::include_deleted::no);
    }
    ss::future<ppsr::schema_id>
    create_schema(ppsr::unparsed_schema schema) override {
        co_await _writer->local().read_sync();
        auto parsed = co_await _store->make_canonical_schema(schema);
        co_return co_await _writer->local().write_subject_version(
          {.schema = std::move(parsed)});
    }

private:
    ppsr::sharded_store* _store;
    ss::sharded<ppsr::seq_writer>* _writer;
};

class disabled_schema_registry : public schema_registry {
public:
    bool is_enabled() const override { return false; };

    ss::future<ppsr::canonical_schema_definition>
    get_schema_definition(ppsr::schema_id) const override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<ppsr::subject_schema> get_subject_schema(
      ppsr::subject, std::optional<ppsr::schema_version>) const override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<ppsr::schema_id> create_schema(ppsr::unparsed_schema) override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
};
} // namespace

std::unique_ptr<schema_registry> schema_registry::make_default(ppsr::api* sr) {
    if (!sr) {
        return std::make_unique<disabled_schema_registry>();
    }
    return std::make_unique<schema_registry_impl>(
      sr->_store.get(), &sr->_sequencer);
}
} // namespace wasm
