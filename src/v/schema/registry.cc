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

#include "schema/registry.h"

#include "pandaproxy/schema_registry/api.h"
#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/service.h"
#include "pandaproxy/schema_registry/sharded_store.h"

#include <seastar/core/sharded.hh>

#include <memory>
#include <stdexcept>

namespace schema {

namespace {

namespace ppsr = pandaproxy::schema_registry;

class schema_registry_impl : public registry {
public:
    explicit schema_registry_impl(ss::sharded<ppsr::service>* service)
      : _service(service) {}

    bool is_enabled() const override { return true; };

    ss::future<ppsr::canonical_schema_definition>
    get_schema_definition(ppsr::schema_id id) const override {
        auto [reader, _] = co_await service();
        co_return co_await reader->get_schema_definition(id);
    }
    ss::future<ppsr::subject_schema> get_subject_schema(
      ppsr::subject sub,
      std::optional<ppsr::schema_version> version) const override {
        auto [reader, _] = co_await service();
        co_return co_await reader->get_subject_schema(
          sub, version, ppsr::include_deleted::no);
    }
    ss::future<ppsr::schema_id>
    create_schema(ppsr::unparsed_schema schema) override {
        auto [reader, writer] = co_await service();
        co_await writer->read_sync();
        auto parsed = co_await reader->make_canonical_schema(std::move(schema));
        co_return co_await writer->write_subject_version(
          {.schema = std::move(parsed)});
    }

private:
    ss::future<std::pair<ppsr::sharded_store*, ppsr::seq_writer*>>
    service() const {
        auto& service = _service->local();
        co_await service.ensure_started();
        co_return std::make_pair(&service.schema_store(), &service.writer());
    }

    ss::sharded<ppsr::service>* _service;
};

class disabled_schema_registry : public registry {
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

std::unique_ptr<registry> registry::make_default(ppsr::api* sr) {
    if (!sr) {
        return std::make_unique<disabled_schema_registry>();
    }
    return std::make_unique<schema_registry_impl>(&sr->_service);
}
} // namespace schema
