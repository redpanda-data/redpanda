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
#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/json.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/service.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"

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

    ss::future<ppsr::schema_getter*> getter() const override {
        auto [reader, _] = co_await service();
        co_return reader;
    }

    ss::future<ppsr::schema_getter*> synced_getter() const override {
        auto [reader, writer] = co_await service();
        co_await writer->read_sync();
        _last_sync_time = ss::lowres_clock::now();
        co_return reader;
    }

    ss::future<ss::lowres_clock::time_point>
    sync(ss::lowres_clock::duration max_age) override {
        auto now = ss::lowres_clock::now();

        if (
          now - _last_sync_time > max_age
          || max_age == ss::lowres_clock::duration{}) {
            auto [reader, writer] = co_await service();
            co_await writer->read_sync();
            _last_sync_time = now;
        }

        co_return _last_sync_time;
    }

    ss::future<ppsr::schema_definition>
    get_schema_definition(ppsr::context_schema_id id) const override {
        auto [reader, _] = co_await service();
        co_return co_await reader->get_schema_definition(id);
    }
    ss::future<ppsr::stored_schema> get_subject_schema(
      ppsr::context_subject sub,
      std::optional<ppsr::schema_version> version) const override {
        auto [reader, _] = co_await service();
        co_return co_await reader->get_subject_schema(
          sub, version, ppsr::include_deleted::no);
    }
    ss::future<chunked_vector<ppsr::subject_version>> list_subject_versions(
      std::function<bool(const ppsr::context_subject&)> filter,
      ppsr::include_deleted inc_del) const override {
        auto [reader, _] = co_await service();
        co_return co_await reader->list_subject_versions(
          std::move(filter), inc_del);
    }

    ss::future<ppsr::context_schema_id>
    create_schema(ppsr::subject_schema schema) override {
        auto ctx = schema.sub().ctx;
        auto [reader, writer] = co_await service();
        co_await writer->read_sync();
        _last_sync_time = ss::lowres_clock::now();
        auto parsed = co_await reader->make_canonical_schema(std::move(schema));
        auto result = co_await writer->write_subject_version(
          {.schema = std::move(parsed)});
        co_return ppsr::context_schema_id{std::move(ctx), result.id};
    }

    ss::future<ppsr::context_schema_id>
    import_schema(ppsr::stored_schema schema) override {
        auto ctx = schema.schema.sub().ctx;
        auto [reader, writer] = co_await service();
        co_await writer->read_sync();
        auto parsed = co_await reader->make_canonical_schema(
          std::move(schema.schema),
          ppsr::normalize::no,
          /*consider_always_normalize_config=*/false);
        co_await reader->validate_schema(parsed.share());
        schema.schema = std::move(parsed);
        auto result = co_await writer->write_subject_version_imported(
          std::move(schema));
        _last_sync_time = ss::lowres_clock::now();
        co_return ppsr::context_schema_id{std::move(ctx), result.id};
    }

    ss::future<bool> soft_delete_schema(
      ppsr::context_subject sub, ppsr::schema_version version) override {
        auto [_, writer] = co_await service();
        auto result = co_await writer->delete_subject_version(
          std::move(sub), version, ppsr::write_source::schema_registry_sync);
        _last_sync_time = ss::lowres_clock::now();
        co_return result;
    }

    ss::future<chunked_vector<ppsr::schema_version>> permanent_delete_schema(
      ppsr::context_subject sub,
      std::optional<ppsr::schema_version> version) override {
        auto [_, writer] = co_await service();
        auto result = co_await writer->delete_subject_permanent(
          std::move(sub), version, ppsr::write_source::schema_registry_sync);
        _last_sync_time = ss::lowres_clock::now();
        co_return result;
    }

    ss::future<bool>
    write_mode(ppsr::context_subject sub, ppsr::mode m) override {
        auto [_, writer] = co_await service();
        auto result = co_await writer->write_mode(
          std::move(sub),
          m,
          ppsr::force::yes,
          ppsr::write_source::schema_registry_sync);
        _last_sync_time = ss::lowres_clock::now();
        co_return result;
    }

    ss::future<bool> delete_mode(ppsr::context_subject sub) override {
        auto [_, writer] = co_await service();
        auto result = co_await writer->delete_mode(
          std::move(sub), ppsr::write_source::schema_registry_sync);
        _last_sync_time = ss::lowres_clock::now();
        co_return result;
    }

    ss::future<bool> write_config(
      ppsr::context_subject sub, ppsr::compatibility_level compat) override {
        auto [_, writer] = co_await service();
        auto result = co_await writer->write_config(
          std::move(sub), compat, ppsr::write_source::schema_registry_sync);
        _last_sync_time = ss::lowres_clock::now();
        co_return result;
    }

    ss::future<bool> delete_config(ppsr::context_subject sub) override {
        auto [_, writer] = co_await service();
        auto result = co_await writer->delete_config(
          std::move(sub), ppsr::write_source::schema_registry_sync);
        _last_sync_time = ss::lowres_clock::now();
        co_return result;
    }

private:
    ss::future<std::pair<ppsr::sharded_store*, ppsr::seq_writer*>>
    service() const {
        auto& service = _service->local();
        co_await service.ensure_started();
        co_return std::make_pair(&service.schema_store(), &service.writer());
    }

    ss::sharded<ppsr::service>* _service;
    mutable ss::lowres_clock::time_point _last_sync_time{};
};

class disabled_schema_registry : public registry {
public:
    bool is_enabled() const override { return false; };

    ss::future<ppsr::schema_getter*> getter() const override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<ppsr::schema_getter*> synced_getter() const override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<ss::lowres_clock::time_point>
    sync(ss::lowres_clock::duration) override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<ppsr::schema_definition>
    get_schema_definition(ppsr::context_schema_id) const override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<ppsr::stored_schema> get_subject_schema(
      ppsr::context_subject,
      std::optional<ppsr::schema_version>) const override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<chunked_vector<ppsr::subject_version>> list_subject_versions(
      std::function<bool(const ppsr::context_subject&)>,
      ppsr::include_deleted) const override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<ppsr::context_schema_id>
    create_schema(ppsr::subject_schema) override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<ppsr::context_schema_id>
    import_schema(ppsr::stored_schema) override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<bool>
    soft_delete_schema(ppsr::context_subject, ppsr::schema_version) override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<chunked_vector<ppsr::schema_version>> permanent_delete_schema(
      ppsr::context_subject, std::optional<ppsr::schema_version>) override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<bool> write_mode(ppsr::context_subject, ppsr::mode) override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<bool> delete_mode(ppsr::context_subject) override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<bool>
    write_config(ppsr::context_subject, ppsr::compatibility_level) override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
    ss::future<bool> delete_config(ppsr::context_subject) override {
        throw std::logic_error(
          "invalid attempted usage of a disabled schema registry");
    }
};
} // namespace

ss::future<std::optional<ppsr::valid_schema>>
registry::get_valid_schema(ppsr::context_schema_id schema_id) const {
    auto reader = co_await getter();
    auto schema_def_opt = co_await reader->maybe_get_schema_definition(
      schema_id);
    if (!schema_def_opt.has_value()) {
        // Assume that we expect to have the schema. If it's not there, one
        // possibility is that the reader needs to catch up, so do that and try
        // again.
        reader = co_await synced_getter();
        schema_def_opt = co_await reader->maybe_get_schema_definition(
          schema_id);
        if (!schema_def_opt.has_value()) {
            co_return std::nullopt;
        }
    }
    auto ctx_sub = ppsr::context_subject{
      ppsr::default_context, ppsr::subject("r")};
    switch (schema_def_opt->type()) {
    case ppsr::schema_type::json: {
        co_return co_await ppsr::make_json_schema_definition(
          *reader, {std::move(ctx_sub), std::move(*schema_def_opt)});
    }
    case ppsr::schema_type::avro: {
        co_return co_await ppsr::make_avro_schema_definition(
          *reader, {std::move(ctx_sub), std::move(*schema_def_opt)});
    }
    case ppsr::schema_type::protobuf: {
        co_return co_await ppsr::make_protobuf_schema_definition(
          *reader, {std::move(ctx_sub), std::move(*schema_def_opt)});
    }
    }
}

std::unique_ptr<registry> registry::make_default(ppsr::api* sr) {
    if (!sr) {
        return std::make_unique<disabled_schema_registry>();
    }
    return std::make_unique<schema_registry_impl>(&sr->_service);
}
} // namespace schema
