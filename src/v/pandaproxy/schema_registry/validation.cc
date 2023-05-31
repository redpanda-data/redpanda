/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "pandaproxy/schema_registry/validation.h"

#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "cluster/controller.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/schema_id_cache.h"
#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/schema_registry/validation_metrics.h"
#include "storage/parser_utils.h"
#include "utils/vint.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/coroutine/exception.hh>

#include <absl/algorithm/container.h>

#include <iterator>
#include <optional>
#include <stdexcept>
#include <type_traits>

namespace pandaproxy::schema_registry {

namespace {

using field = schema_id_cache::field;

auto to_string_view(field f) {
    switch (f) {
    case field::key:
        return "key";
    case field::val:
        return "value";
    }
};

auto make_subject(
  subject_name_strategy sns,
  const model::topic& t,
  field f,
  std::string_view record_name) {
    switch (sns) {
    case subject_name_strategy::topic_name:
        return subject{ssx::sformat("{}-{}", t(), to_string_view(f))};
    case subject_name_strategy::record_name:
        return subject{record_name};
    case subject_name_strategy::topic_record_name:
        return subject{ssx::sformat("{}-{}", t(), record_name)};
    }
};

std::vector<int32_t> get_proto_offsets(iobuf_parser& p) {
    // The encoding is a length, followed by indexes into the file or message.
    // Each number is a zigzag encoded integer.
    std::vector<int32_t> offsets;
    auto [offset_count, bytes_read] = p.read_varlong();
    if (!bytes_read) {
        return {};
    }
    // Reject more offsets than bytes remaining; it's not possible
    if (static_cast<size_t>(offset_count) > p.bytes_left()) {
        return {};
    }
    offsets.resize(offset_count);
    for (auto& o : offsets) {
        std::tie(o, bytes_read) = p.read_varlong();
        if (!bytes_read) {
            return {};
        }
    }
    if (offsets.empty()) {
        // If the decoded length is 0, then assume the optimised encoding for
        // using the first message in the file
        offsets.push_back(0);
    }
    return offsets;
}

ss::future<std::optional<ss::sstring>> get_record_name(
  pandaproxy::schema_registry::sharded_store& store,
  field field,
  const model::topic& topic,
  subject_name_strategy sns,
  const canonical_schema_definition& schema,
  std::optional<std::vector<int32_t>>& offsets) {
    if (sns == subject_name_strategy::topic_name) {
        // Result is succesfully nothing
        co_return "";
    }

    switch (schema.type()) {
    case schema_type::avro: {
        auto s = co_await make_avro_schema_definition(
          store, {subject("r"), {schema.raw(), schema.type()}});
        co_return s().root()->name().fullname();
    } break;
    case schema_type::protobuf: {
        if (!offsets) {
            co_return std::nullopt;
        }
        auto s = co_await make_protobuf_schema_definition(
          store, {subject("r"), {schema.raw(), schema.type()}});
        auto r = s.name(*offsets);
        if (!r) {
            co_return std::nullopt;
        }
        co_return std::move(r).assume_value();
    } break;
    case schema_type::json:
        break;
    }
    co_return std::nullopt;
}

} // namespace

class schema_id_validator::impl {
public:
    using data_t = model::record_batch_reader::data_t;
    using foreign_data_t = model::record_batch_reader::foreign_data_t;
    using storage_t = model::record_batch_reader::storage_t;

    impl(
      const std::unique_ptr<api>& api,
      model::topic topic,
      const cluster::topic_properties& props)
      : _api{api}
      , _topic{std::move(topic)}
      , _record_key_schema_id_validation{props.record_key_schema_id_validation
                                           .value_or(
                                             props
                                               .record_key_schema_id_validation_compat
                                               .value_or(false))}
      , _record_key_subject_name_strategy{props.record_key_subject_name_strategy
                                            .value_or(
                                              props
                                                .record_key_subject_name_strategy_compat
                                                .value_or(
                                                  subject_name_strategy::
                                                    topic_name))}
      , _record_value_schema_id_validation{props
                                             .record_value_schema_id_validation
                                             .value_or(
                                               props
                                                 .record_value_schema_id_validation_compat
                                                 .value_or(false))}
      , _record_value_subject_name_strategy{
          props.record_value_subject_name_strategy.value_or(
            props.record_value_subject_name_strategy_compat.value_or(
              subject_name_strategy::topic_name))} {}

    auto validate_field(
      field field, model::topic topic, subject_name_strategy sns, iobuf buf)
      -> ss::future<bool> {
        iobuf_parser parser(std::move(buf));

        if (parser.bytes_left() < 5) {
            vlog(
              plog.debug,
              "validating: topic: {}, field: {}, not enough bytes: {}",
              topic(),
              to_string_view(field),
              parser.bytes_left());
            co_return false;
        }

        auto magic = parser.consume_type<int8_t>();
        if (magic != 0) {
            vlog(
              plog.debug,
              "validating: topic: {}, field: {}, invalid magic: {}",
              topic(),
              to_string_view(field),
              magic);
            co_return false;
        }

        auto id = schema_id{parser.consume_be_type<int32_t>()};

        // Optimistically check the cache in case just the id matches
        // This is true for Avro with TopicNameStrategy
        if (_api->_schema_id_cache.local().has(
              topic, field, sns, id, std::nullopt)) {
            vlog(
              plog.debug,
              "validating: topic: {}, field: {}, cache hit",
              topic(),
              to_string_view(field));
            _api->_schema_id_validation_probe.local().hit();
            co_return true;
        }

        // Determine the schema type
        std::optional<canonical_schema_definition> schema;
        try {
            schema.emplace(co_await _api->_store->get_schema_definition(id));
        } catch (const exception& ex) {
            vlog(
              plog.debug,
              "validating: topic: {}, field: {}, schema not found: {}",
              topic(),
              to_string_view(field),
              ex.message());
            co_return false;
        }

        std::optional<std::vector<int32_t>> proto_offsets;
        if (schema->type() == schema_type::protobuf) {
            auto offsets = get_proto_offsets(parser);
            if (offsets.empty()) {
                vlog(
                  plog.debug,
                  "validating: topic: {}, field: {}, invalid protobuf offsets",
                  topic(),
                  to_string_view(field));
                co_return false;
            }

            if (_api->_schema_id_cache.local().has(
                  topic, field, sns, id, offsets)) {
                vlog(
                  plog.debug,
                  "validating: topic: {}, field: {}, cache hit",
                  topic(),
                  to_string_view(field));
                _api->_schema_id_validation_probe.local().hit();
                co_return true;
            }

            proto_offsets.emplace(std::move(offsets));
        }

        auto record_name = co_await get_record_name(
          *_api->_store, field, topic, sns, *schema, proto_offsets);
        if (!record_name) {
            vlog(
              plog.debug,
              "validating: topic: {}, field: {}, unable to extract record_name",
              topic(),
              to_string_view(field));
            co_return false;
        }

        auto sub = make_subject(sns, topic, field, *record_name);

        auto has_id = co_await _api->_store->has_version(
          sub, id, include_deleted::yes);
        if (!has_id) {
            vlog(
              plog.debug,
              "validating: sub: {}, id: {}, has_id: {}",
              sub,
              id,
              has_id);
            co_return false;
        }

        _api->_schema_id_cache.local().put(
          topic, field, sns, id, std::move(proto_offsets));
        _api->_schema_id_validation_probe.local().miss();
        co_return true;
    };

    ss::future<bool> validate(const model::record_batch& batch) {
        if (
          !_record_key_schema_id_validation
          && !_record_value_schema_id_validation) {
            co_return true;
        }

        auto key_is_valid =
          [this](const model::record& record) -> ss::future<bool> {
            if (!_record_key_schema_id_validation) {
                return ss::make_ready_future<bool>(true);
            }
            return validate_field(
              field::key,
              _topic,
              _record_key_subject_name_strategy,
              record.key().copy());
        };

        auto value_is_valid =
          [this](const model::record& record) -> ss::future<bool> {
            if (!_record_value_schema_id_validation) {
                return ss::make_ready_future<bool>(true);
            }
            return validate_field(
              field::val,
              _topic,
              _record_value_subject_name_strategy,
              record.value().copy());
        };

        const model::record_batch& b = batch;
        std::optional<const model::record_batch> u;
        bool compressed = batch.compressed();
        if (compressed) {
            u.emplace(
              co_await storage::internal::decompress_batch(batch.copy()));
            _api->_schema_id_validation_probe.local().decompressed();
        }

        bool valid{true};
        co_await model::for_each_record(
          compressed ? u.value() : b,
          [&valid, k{std::move(key_is_valid)}, v{std::move(value_is_valid)}](
            const model::record& r) {
              return k(r)
                .then([&r, v{std::move(v)}](bool k) {
                    return k ? v(r) : ss::make_ready_future<bool>(false);
                })
                .then([&valid](bool v) { valid = v; });
          });
        co_return valid;
    }

    ss::future<bool> validate(const data_t& data) {
        for (const auto& b : data) {
            if (!co_await validate(b)) {
                co_return false;
            }
        }
        co_return true;
    }

    auto validate(const foreign_data_t& data) { return validate(*data.buffer); }

    ss::future<result> operator()(model::record_batch_reader&& rbr) {
        if (!_api) {
            // If Schema Registry is not enabled, the safe default is to reject
            co_return kafka::error_code::invalid_record;
        }
        if (!_api->_controller->get_feature_table().local().is_active(
              features::feature::schema_id_validation)) {
            co_return std::move(rbr);
        }

        auto impl = std::move(rbr).release();
        auto slice = co_await impl->do_load_slice(model::no_timeout);
        vassert(
          impl->is_end_of_stream(),
          "Attempt to validate schema id on a record_batch_reader with "
          "multiple slices");

        auto valid = co_await ss::visit(
          slice, [this](const auto& d) { return validate(d); });

        if (!valid) {
            // It's possible that the schema registry doesn't have a newly
            // written schema, update and retry.
            co_await _api->_sequencer.local().read_sync();
            valid = co_await ss::visit(
              slice, [this](const auto& d) { return validate(d); });
        }

        if (!valid) {
            vlog(
              plog.debug,
              "validating: _topic: {}, _record_key_schema_id_validation: {}, "
              "_record_key_subject_name_strategy: {}, "
              "_record_value_schema_id_validation: {}, "
              "_record_value_subject_name_strategy: {}",
              _topic,
              _record_key_schema_id_validation,
              _record_key_subject_name_strategy,
              _record_value_schema_id_validation,
              _record_value_subject_name_strategy);
            co_return kafka::error_code::invalid_record;
        }

        co_return model::make_memory_record_batch_reader(std::move(slice));
    }

private:
    const std::unique_ptr<api>& _api;
    model::topic _topic;
    bool _record_key_schema_id_validation;
    pandaproxy::schema_registry::subject_name_strategy
      _record_key_subject_name_strategy;
    bool _record_value_schema_id_validation;
    pandaproxy::schema_registry::subject_name_strategy
      _record_value_subject_name_strategy;
};

schema_id_validator::schema_id_validator(
  const std::unique_ptr<api>& api,
  const model::topic& topic,
  const cluster::topic_properties& props)
  : _impl{std::make_unique<impl>(api, topic, props)} {}

schema_id_validator::schema_id_validator(schema_id_validator&&) noexcept
  = default;
schema_id_validator::~schema_id_validator() noexcept = default;

bool should_validate_schema_id(const cluster::topic_properties& props) {
    return props.record_key_schema_id_validation.value_or(
             props.record_key_schema_id_validation_compat.value_or(false))
           || props.record_value_schema_id_validation.value_or(
             props.record_value_schema_id_validation_compat.value_or(false));
}

std::optional<schema_id_validator> maybe_make_schema_id_validator(
  const std::unique_ptr<api>& api,
  const model::topic& topic,
  const cluster::topic_properties& props) {
    return api != nullptr && should_validate_schema_id(props)
             ? std::make_optional<schema_id_validator>(api, topic, props)
             : std::nullopt;
}

ss::future<schema_id_validator::result>
schema_id_validator::operator()(model::record_batch_reader&& rbr) {
    return (*_impl)(std::move(rbr));
}

} // namespace pandaproxy::schema_registry
