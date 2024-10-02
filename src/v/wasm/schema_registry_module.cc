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

#include "schema_registry_module.h"

#include "base/vassert.h"
#include "ffi.h"
#include "logger.h"
#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/named_type.h"

namespace wasm {

namespace {
using serialized_schema_type = named_type<int64_t, struct schema_id_tag>;
constexpr serialized_schema_type avro = serialized_schema_type(0);
constexpr serialized_schema_type protobuf = serialized_schema_type(1);
constexpr serialized_schema_type json = serialized_schema_type(2);

serialized_schema_type
serialize_schema_type(pandaproxy::schema_registry::schema_type st) {
    switch (st) {
    case pandaproxy::schema_registry::schema_type::avro:
        return avro;
    case pandaproxy::schema_registry::schema_type::json:
        return json;
    case pandaproxy::schema_registry::schema_type::protobuf:
        return protobuf;
    }
    vassert(false, "unknown schema type: {}", st);
}

std::optional<pandaproxy::schema_registry::schema_type>
deserialize_schema_type(serialized_schema_type st) {
    switch (st()) {
    case avro():
        return pandaproxy::schema_registry::schema_type::avro;
    case json():
        return pandaproxy::schema_registry::schema_type::json;
    case protobuf():
        return pandaproxy::schema_registry::schema_type::protobuf;
    }
    return std::nullopt;
}

template<typename T>
void write_encoded_schema_def(
  const pandaproxy::schema_registry::canonical_schema_definition& def, T* w) {
    w->append(serialize_schema_type(def.type()));
    w->append_with_length(def.raw()());
    w->append(def.refs().size());
    for (const auto& ref : def.refs()) {
        w->append_with_length(ref.name);
        w->append_with_length(ref.sub());
        w->append(ref.version());
    }
}

pandaproxy::schema_registry::unparsed_schema_definition
read_encoded_schema_def(ffi::reader* r) {
    using namespace pandaproxy::schema_registry;
    auto serialized_type = serialized_schema_type(r->read_varint());
    auto type = deserialize_schema_type(serialized_type);
    if (!type.has_value()) {
        throw std::runtime_error(
          ss::format("unknown schema type: {}", serialized_type));
    }
    auto def = r->read_sized_string();
    auto rc = r->read_varint();
    unparsed_schema_definition::references refs;
    refs.reserve(rc);
    for (int i = 0; i < rc; ++i) {
        auto name = r->read_sized_string();
        auto sub = r->read_sized_string();
        auto v = int(r->read_varint());
        refs.emplace_back(name, subject(sub), schema_version(v));
    }
    return {def, *type, refs};
}

template<typename T>
void write_encoded_schema_subject(
  const pandaproxy::schema_registry::subject_schema& schema, T* w) {
    w->append(schema.id());
    w->append(schema.version());
    // not writing the subject because the client should already have it.
    write_encoded_schema_def(schema.schema.def(), w);
}

constexpr int32_t SUCCESS = 0;
constexpr int32_t SCHEMA_REGISTRY_NOT_ENABLED = -1;
constexpr int32_t SCHEMA_REGISTRY_ERROR = -2;

} // namespace

schema_registry_module::schema_registry_module(schema::registry* sr)
  : _sr(sr) {}

void schema_registry_module::check_abi_version_0() {}

ss::future<int32_t> schema_registry_module::get_schema_definition_len(
  pandaproxy::schema_registry::schema_id schema_id, uint32_t* size_out) {
    if (!_sr->is_enabled()) {
        co_return SCHEMA_REGISTRY_NOT_ENABLED;
    }
    try {
        auto schema = co_await _sr->get_schema_definition(schema_id);
        ffi::sizer sizer;
        write_encoded_schema_def(schema, &sizer);
        *size_out = sizer.total();
        co_return SUCCESS;
    } catch (...) {
        vlog(wasm_log.warn, "error fetching schema definition {}", schema_id);
        co_return SCHEMA_REGISTRY_ERROR;
    }
}

ss::future<int32_t> schema_registry_module::get_schema_definition(
  pandaproxy::schema_registry::schema_id schema_id, ffi::array<uint8_t> buf) {
    if (!_sr->is_enabled()) {
        co_return SCHEMA_REGISTRY_NOT_ENABLED;
    }
    try {
        auto schema = co_await _sr->get_schema_definition(schema_id);
        ffi::writer writer(buf);
        write_encoded_schema_def(schema, &writer);
        co_return writer.total();
    } catch (...) {
        vlog(wasm_log.warn, "error fetching schema definition {}", schema_id);
        co_return SCHEMA_REGISTRY_ERROR;
    }
}
ss::future<int32_t> schema_registry_module::get_subject_schema_len(
  pandaproxy::schema_registry::subject sub,
  pandaproxy::schema_registry::schema_version version,
  uint32_t* size_out) {
    if (!_sr->is_enabled()) {
        co_return SCHEMA_REGISTRY_NOT_ENABLED;
    }

    using namespace pandaproxy::schema_registry;
    try {
        std::optional<schema_version> v = version == invalid_schema_version
                                            ? std::nullopt
                                            : std::make_optional(version);
        auto schema = co_await _sr->get_subject_schema(sub, v);
        ffi::sizer sizer;
        write_encoded_schema_subject(schema, &sizer);
        *size_out = sizer.total();
        co_return SUCCESS;
    } catch (const std::exception& ex) {
        vlog(
          wasm_log.warn, "error fetching schema {}/{}: {}", sub, version, ex);
        co_return SCHEMA_REGISTRY_ERROR;
    }
}

ss::future<int32_t> schema_registry_module::get_subject_schema(
  pandaproxy::schema_registry::subject sub,
  pandaproxy::schema_registry::schema_version version,
  ffi::array<uint8_t> buf) {
    if (!_sr->is_enabled()) {
        co_return SCHEMA_REGISTRY_NOT_ENABLED;
    }
    using namespace pandaproxy::schema_registry;
    try {
        std::optional<schema_version> v = version == invalid_schema_version
                                            ? std::nullopt
                                            : std::make_optional(version);
        auto schema = co_await _sr->get_subject_schema(sub, v);
        ffi::writer writer(buf);
        write_encoded_schema_subject(schema, &writer);
        co_return writer.total();
    } catch (const std::exception& ex) {
        vlog(
          wasm_log.warn, "error fetching schema {}/{}: {}", sub, version, ex);
        co_return SCHEMA_REGISTRY_ERROR;
    }
}

ss::future<int32_t> schema_registry_module::create_subject_schema(
  pandaproxy::schema_registry::subject sub,
  ffi::array<uint8_t> buf,
  pandaproxy::schema_registry::schema_id* out_schema_id) {
    if (!_sr->is_enabled()) {
        co_return SCHEMA_REGISTRY_NOT_ENABLED;
    }

    ffi::reader r(buf);
    using namespace pandaproxy::schema_registry;
    try {
        *out_schema_id = co_await _sr->create_schema(
          unparsed_schema(sub, read_encoded_schema_def(&r)));
    } catch (const std::exception& ex) {
        vlog(wasm_log.warn, "error registering subject schema: {}", ex);
        co_return SCHEMA_REGISTRY_ERROR;
    }

    co_return SUCCESS;
}

} // namespace wasm
