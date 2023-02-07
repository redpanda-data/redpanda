/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/topic_manifest.h"

#include "bytes/iobuf_istreambuf.h"
#include "bytes/iobuf_ostreambuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "hashing/xx.h"
#include "json/encodings.h"
#include "json/istreamwrapper.h"
#include "json/ostreamwrapper.h"
#include "json/reader.h"
#include "json/types.h"
#include "json/writer.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>

#include <boost/lexical_cast.hpp>
#include <fmt/ostream.h>
#include <rapidjson/error/en.h>

#include <chrono>
#include <optional>
#include <stdexcept>
#include <string_view>

namespace cloud_storage {

struct topic_manifest_handler
  : public json::BaseReaderHandler<json::UTF8<>, topic_manifest_handler> {
    using key_string = ss::basic_sstring<char, uint32_t, 31>;
    bool StartObject() {
        switch (_state) {
        case state::expect_manifest_start:
            _state = state::expect_key;
            return true;
        case state::expect_key:
        case state::expect_value:
            return false;
        }
    }

    bool Key(const char* str, json::SizeType length, bool /*copy*/) {
        switch (_state) {
        case state::expect_key:
            _key = key_string(str, length);
            _state = state::expect_value;
            return true;
        case state::expect_manifest_start:
        case state::expect_value:
            return false;
        }
    }

    bool String(const char* str, json::SizeType length, bool /*copy*/) {
        std::string_view sv(str, length);
        switch (_state) {
        case state::expect_value:
            if ("namespace" == _key) {
                _namespace = model::ns(ss::sstring(sv));
            } else if ("topic" == _key) {
                _topic = model::topic(ss::sstring(sv));
            } else if ("compression" == _key) {
                compression_sv = ss::sstring(sv);
            } else if ("cleanup_policy_bitflags" == _key) {
                cleanup_policy_bitflags_sv = ss::sstring(sv);
            } else if ("compaction_strategy" == _key) {
                compaction_strategy_sv = ss::sstring(sv);
            } else if ("timestamp_type" == _key) {
                timestamp_type_sv = ss::sstring(sv);
            } else {
                return false;
            }
            _state = state::expect_key;
            return true;
        case state::expect_manifest_start:
        case state::expect_key:
            return false;
        }
    }

    bool Uint(unsigned u) { return Uint64(u); }

    bool Uint64(uint64_t u) {
        switch (_state) {
        case state::expect_value:
            if ("version" == _key) {
                _version = u;
            } else if ("partition_count" == _key) {
                _partition_count = u;
            } else if ("replication_factor" == _key) {
                _replication_factor = u;
            } else if ("revision_id" == _key) {
                _revision_id = model::initial_revision_id(u);
            } else if ("segment_size" == _key) {
                _properties.segment_size = u;
            } else if ("retention_bytes" == _key) {
                _properties.retention_bytes = tristate<size_t>(u);
            } else if ("retention_duration" == _key) {
                _properties.retention_duration
                  = tristate<std::chrono::milliseconds>(
                    std::chrono::milliseconds(u));
            } else {
                return false;
            }
            _state = state::expect_key;
            return true;
        case state::expect_manifest_start:
        case state::expect_key:
            return false;
        }
    }

    bool EndObject(json::SizeType /*size*/) {
        return _state == state::expect_key;
    }

    bool Null() {
        if (_state == state::expect_value) {
            _state = state::expect_key;
            return true;
        }
        return false;
    }

    bool Default() { return false; }

    enum class state {
        expect_manifest_start,
        expect_key,
        expect_value,
    } _state{state::expect_manifest_start};

    key_string _key;

    // required fields
    std::optional<int32_t> _version;
    std::optional<model::ns> _namespace{};
    std::optional<model::topic> _topic;
    std::optional<int32_t> _partition_count;
    std::optional<int16_t> _replication_factor;
    std::optional<model::initial_revision_id> _revision_id{};

    // optional fields
    manifest_topic_configuration::topic_properties _properties;
    std::optional<ss::sstring> compaction_strategy_sv;
    std::optional<ss::sstring> timestamp_type_sv;
    std::optional<ss::sstring> compression_sv;
    std::optional<ss::sstring> cleanup_policy_bitflags_sv;
};

topic_manifest::topic_manifest(
  const manifest_topic_configuration& cfg, model::initial_revision_id rev)
  : _topic_config(cfg)
  , _rev(rev) {}

topic_manifest::topic_manifest()
  : _topic_config(std::nullopt) {}

void topic_manifest::update(const topic_manifest_handler& handler) {
    if (handler._version != topic_manifest_version) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "topic manifest version {} is not supported",
          handler._version));
    }
    _rev = handler._revision_id.value();

    if (!handler._version) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Missing _version value in parsed topic manifest"));
    }
    if (!handler._namespace) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Missing _namespace value in parsed topic manifest"));
    }
    if (!handler._topic) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Missing _topic value in parsed topic manifest"));
    }
    if (!handler._partition_count) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Missing _partition_count value in parsed topic manifest"));
    }
    if (!handler._replication_factor) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Missing _replication_factor value in parsed topic manifest"));
    }
    if (!handler._revision_id) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Missing _revision_id value in parsed topic manifest"));
    }

    _topic_config = manifest_topic_configuration{
      .tp_ns = model::topic_namespace(
        handler._namespace.value(), handler._topic.value()),
      .partition_count = handler._partition_count.value(),
      .replication_factor = handler._replication_factor.value(),
      .properties = handler._properties};

    if (handler.compaction_strategy_sv) {
        try {
            _topic_config->properties.compaction_strategy
              = boost::lexical_cast<model::compaction_strategy>(
                handler.compaction_strategy_sv.value());
        } catch (const std::runtime_error& e) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to parse topic manifest {}: Invalid compaction_strategy: "
              "{}",
              get_manifest_path(),
              handler.compaction_strategy_sv.value()));
        }
    }
    if (handler.timestamp_type_sv) {
        try {
            _topic_config->properties.timestamp_type
              = boost::lexical_cast<model::timestamp_type>(
                handler.timestamp_type_sv.value());
        } catch (const std::runtime_error& e) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to parse topic manifest {}: Invalid timestamp_type "
              "value: {}",
              get_manifest_path(),
              handler.timestamp_type_sv.value()));
        }
    }
    if (handler.compression_sv) {
        try {
            _topic_config->properties.compression
              = boost::lexical_cast<model::compression>(
                handler.compression_sv.value());
        } catch (const std::runtime_error& e) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to parse topic manifest {}: Invalid compression value: "
              "{}",
              get_manifest_path(),
              handler.compression_sv.value()));
        }
    }
    if (handler.cleanup_policy_bitflags_sv) {
        try {
            _topic_config->properties.cleanup_policy_bitflags
              = boost::lexical_cast<model::cleanup_policy_bitflags>(
                handler.cleanup_policy_bitflags_sv.value());
        } catch (const std::runtime_error& e) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to parse topic manifest {}: Invalid "
              "cleanup_policy_bitflags value: {}",
              get_manifest_path(),
              handler.cleanup_policy_bitflags_sv.value()));
        }
    }
}

ss::future<> topic_manifest::update(ss::input_stream<char> is) {
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os);
    iobuf_istreambuf ibuf(result);
    std::istream stream(&ibuf);

    json::IStreamWrapper wrapper(stream);
    json::Reader reader;
    topic_manifest_handler handler;
    if (reader.Parse(wrapper, handler)) {
        vlog(cst_log.debug, "Parsed successfully!");
        topic_manifest::update(handler);
    } else {
        rapidjson::ParseErrorCode e = reader.GetParseErrorCode();
        size_t o = reader.GetErrorOffset();

        if (_topic_config) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to parse topic manifest {}: {} at offset {}",
              get_manifest_path(),
              rapidjson::GetParseError_En(e),
              o));
        } else {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to parse topic manifest: {} at offset {}",
              rapidjson::GetParseError_En(e),
              o));
        }
    }
    co_return;
}

ss::future<serialized_json_stream> topic_manifest::serialize() const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialize(os);
    if (!os.good()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "could not serialize topic manifest {}",
          get_manifest_path()));
    }
    size_t size_bytes = serialized.size_bytes();
    co_return serialized_json_stream{
      .stream = make_iobuf_input_stream(std::move(serialized)),
      .size_bytes = size_bytes};
}

void topic_manifest::serialize(std::ostream& out) const {
    json::OStreamWrapper wrapper(out);
    json::Writer<json::OStreamWrapper> w(wrapper);
    w.StartObject();
    w.Key("version");
    w.Int(static_cast<int>(topic_manifest_version));
    w.Key("namespace");
    w.String(_topic_config->tp_ns.ns());
    w.Key("topic");
    w.String(_topic_config->tp_ns.tp());
    w.Key("partition_count");
    w.Int(_topic_config->partition_count);
    w.Key("replication_factor");
    w.Int(_topic_config->replication_factor);
    w.Key("revision_id");
    w.Int(_rev());

    // optional values are encoded in the following manner:
    // - key set to null - optional is nullopt
    // - key is not null - optional has value
    w.Key("compression");
    if (_topic_config->properties.compression.has_value()) {
        w.String(boost::lexical_cast<std::string>(
          *_topic_config->properties.compression));
    } else {
        w.Null();
    }
    w.Key("cleanup_policy_bitflags");
    if (_topic_config->properties.cleanup_policy_bitflags.has_value()) {
        w.String(boost::lexical_cast<std::string>(
          *_topic_config->properties.cleanup_policy_bitflags));
    } else {
        w.Null();
    }
    w.Key("compaction_strategy");
    if (_topic_config->properties.compaction_strategy.has_value()) {
        w.String(boost::lexical_cast<std::string>(
          *_topic_config->properties.compaction_strategy));
    } else {
        w.Null();
    }
    w.Key("timestamp_type");
    if (_topic_config->properties.timestamp_type.has_value()) {
        w.String(boost::lexical_cast<std::string>(
          *_topic_config->properties.timestamp_type));
    } else {
        w.Null();
    }
    w.Key("segment_size");
    if (_topic_config->properties.segment_size.has_value()) {
        w.Int64(*_topic_config->properties.segment_size);
    } else {
        w.Null();
    }
    // NOTE: manifest_object_name is intentionaly ommitted

    // tristate values are encoded in the following manner:
    // - key not present - tristate is disabled
    // - key set to null - tristate is enabled but not set
    // - key is not null - tristate is enabled and set
    if (!_topic_config->properties.retention_bytes.is_disabled()) {
        w.Key("retention_bytes");
        if (_topic_config->properties.retention_bytes.has_optional_value()) {
            w.Int64(_topic_config->properties.retention_bytes.value());
        } else {
            w.Null();
        }
    }
    if (!_topic_config->properties.retention_duration.is_disabled()) {
        w.Key("retention_duration");
        if (_topic_config->properties.retention_duration.has_optional_value()) {
            w.Int64(
              _topic_config->properties.retention_duration.value().count());
        } else {
            w.Null();
        }
    }
    w.EndObject();
}

remote_manifest_path
topic_manifest::get_topic_manifest_path(model::ns ns, model::topic topic) {
    // The path is <prefix>/meta/<ns>/<topic>/topic_manifest.json
    constexpr uint32_t bitmask = 0xF0000000;
    auto path = fmt::format("{}/{}", ns(), topic());
    uint32_t hash = bitmask & xxhash_32(path.data(), path.size());
    return remote_manifest_path(
      fmt::format("{:08x}/meta/{}/topic_manifest.json", hash, path));
}

remote_manifest_path topic_manifest::get_manifest_path() const {
    // The path is <prefix>/meta/<ns>/<topic>/topic_manifest.json
    vassert(_topic_config, "Topic config is not set");
    return get_topic_manifest_path(
      _topic_config->tp_ns.ns, _topic_config->tp_ns.tp);
}
} // namespace cloud_storage
