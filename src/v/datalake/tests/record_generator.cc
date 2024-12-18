/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/tests/record_generator.h"

#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/registry.h"
#include "storage/record_batch_builder.h"
#include "utils/vint.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/variant_utils.hh>

#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/text_format.h>

namespace datalake::tests {

ss::future<checked<std::nullopt_t, record_generator::error>>
record_generator::register_avro_schema(
  std::string_view name, std::string_view schema) {
    using namespace pandaproxy::schema_registry;
    auto id = co_await ss::coroutine::as_future(
      _sr->create_schema(unparsed_schema{
        subject{"foo"},
        unparsed_schema_definition{schema, schema_type::avro}}));
    if (id.failed()) {
        co_return error{fmt::format(
          "Error creating schema {}: {}", name, id.get_exception())};
    }
    auto [_, added] = _id_by_name.emplace(name, id.get());
    if (!added) {
        co_return error{fmt::format("Failed to add schema {} to map", name)};
    }
    co_return std::nullopt;
}

ss::future<checked<std::nullopt_t, record_generator::error>>
record_generator::register_protobuf_schema(
  std::string_view name, std::string_view schema) {
    using namespace pandaproxy::schema_registry;
    auto id = co_await ss::coroutine::as_future(
      _sr->create_schema(unparsed_schema{
        subject{"foo"},
        unparsed_schema_definition{schema, schema_type::protobuf}}));
    if (id.failed()) {
        co_return error{fmt::format(
          "Error creating schema {}: {}", name, id.get_exception())};
    }
    auto [_, added] = _id_by_name.emplace(name, id.get());
    if (!added) {
        co_return error{fmt::format("Failed to add schema {} to map", name)};
    }
    co_return std::nullopt;
}

iobuf encode_protobuf_message_index(const std::vector<int32_t>& message_index) {
    iobuf ret;
    if (message_index.size() == 1 && message_index[0] == 0) {
        ret.append("\0", 1);
        return ret;
    }

    std::array<uint8_t, vint::max_length> bytes{0};
    size_t res_size = vint::serialize(message_index.size(), &bytes[0]);
    ret.append(&bytes[0], res_size);

    for (const auto& o : message_index) {
        size_t res_size = vint::serialize(o, &bytes[0]);
        ret.append(&bytes[0], res_size);
    }

    return ret;
}

ss::future<checked<std::nullopt_t, record_generator::error>>
record_generator::add_random_protobuf_record(
  storage::record_batch_builder& b,
  std::string_view name,
  const std::vector<int32_t>& message_index,
  std::optional<iobuf> key,
  testing::protobuf_generator_config config) {
    using namespace pandaproxy::schema_registry;
    auto it = _id_by_name.find(name);
    if (it == _id_by_name.end()) {
        co_return error{fmt::format("Schema {} is missing", name)};
    }
    auto schema_id = it->second;
    auto schema_def = co_await _sr->get_valid_schema(schema_id);
    if (!schema_def) {
        co_return error{
          fmt::format("Unable to find schema def for id: {}", schema_id)};
    }
    if (schema_def->type() != schema_type::protobuf) {
        co_return error{fmt::format(
          "Schema {} has wrong type: {}", name, schema_def->type())};
    }

    auto protobuf_def = schema_def
                          ->visit(ss::make_visitor(
                            [](const avro_schema_definition&)
                              -> std::optional<protobuf_schema_definition> {
                                return std::nullopt;
                            },
                            [](const protobuf_schema_definition& pb_def)
                              -> std::optional<protobuf_schema_definition> {
                                return {pb_def};
                            },
                            [](const json_schema_definition&)
                              -> std::optional<protobuf_schema_definition> {
                                return std::nullopt;
                            }))
                          .value();
    auto md_res = pandaproxy::schema_registry::descriptor(
      protobuf_def, message_index);
    if (md_res.has_error()) {
        co_return error{fmt::format(
          "Wasn't able to get descriptor for protobuf def with id: {}",
          schema_id)};
    }

    iobuf val;
    val.append("\0", 1);
    int32_t encoded_id = ss::cpu_to_be(schema_id());
    val.append((const uint8_t*)(&encoded_id), 4);

    testing::protobuf_generator pb_gen(config);
    auto msg = pb_gen.generate_protobuf_message(&md_res.value().get());

    val.append(encode_protobuf_message_index(message_index));
    val.append(iobuf::from(msg->SerializeAsString()));

    b.add_raw_kv(std::move(key), std::move(val));
    co_return std::nullopt;
}

ss::future<checked<std::nullopt_t, record_generator::error>>
record_generator::add_random_avro_record(
  storage::record_batch_builder& b,
  std::string_view name,
  std::optional<iobuf> key,
  testing::avro_generator_config config) {
    using namespace pandaproxy::schema_registry;
    auto it = _id_by_name.find(name);
    if (it == _id_by_name.end()) {
        co_return error{fmt::format("Schema {} is missing", name)};
    }
    auto schema_id = it->second;
    auto schema_def_res = co_await _sr->get_valid_schema(schema_id);
    if (!schema_def_res.has_value()) {
        co_return error{fmt::format("Schema {} not in store", schema_id)};
    }
    auto& schema_def = schema_def_res.value();
    if (schema_def.type() != schema_type::avro) {
        co_return error{
          fmt::format("Schema {} has wrong type: {}", name, schema_def.type())};
    }
    iobuf val;
    val.append("\0", 1);
    int32_t encoded_id = ss::cpu_to_be(schema_id());
    val.append((const uint8_t*)(&encoded_id), 4);

    avro::NodePtr node_ptr;
    struct visitor {
    public:
        explicit visitor(avro::NodePtr& ptr)
          : node_ptr(ptr) {}
        avro::NodePtr& node_ptr;
        void operator()(const avro_schema_definition& avro_def) {
            node_ptr = avro_def().root();
        }
        void operator()(const protobuf_schema_definition&) {}
        void operator()(const json_schema_definition&) {}
    };
    schema_def.visit(visitor(node_ptr));
    if (!node_ptr) {
        co_return error{
          fmt::format("Schema {} didn't resolve Avro node", name)};
    }
    testing::avro_generator gen(config);
    auto datum = gen.generate_datum(node_ptr);
    std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out);
    avro::encode(*e, datum);
    e->flush();
    auto snap = avro::snapshot(*out);
    iobuf data_buf;
    data_buf.append(snap->data(), snap->size());
    val.append(std::move(data_buf));

    b.add_raw_kv(std::move(key), std::move(val));
    co_return std::nullopt;
}

} // namespace datalake::tests
