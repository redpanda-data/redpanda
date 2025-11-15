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

#include "bytes/iostream.h"
#include "bytes/streambuf.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/types.h"
#include "json/encodings.h"
#include "json/istreamwrapper.h"
#include "json/reader.h"
#include "json/types.h"
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

namespace {
// topic manifest state is a serde-friendly representation of
// topic_manifest. it will allow to evolve the manifest without pushing
// fields to topic_properties, if the need arises
struct topic_manifest_state
  : public serde::envelope<
      topic_manifest_state,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster::topic_configuration cfg;
    // note: initial_revision will be used to initialize
    // cfg.properties.remote_topic_properties.initial_revision, but keep it
    // separate here to mirror the old behavior.

    model::initial_revision_id initial_revision;

    auto serde_fields() { return std::tie(cfg, initial_revision); }

    bool operator==(const topic_manifest_state&) const = default;
};

} // namespace

namespace cloud_storage {

/// JSON parsing handler for topic manifest written by legacy
/// versions of Redpanda. For backwards compatibility.
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
            if (_key == "namespace") {
                _namespace = model::ns(ss::sstring(sv));
            } else if (_key == "topic") {
                _topic = model::topic(ss::sstring(sv));
            } else if (_key == "compression") {
                compression_sv = ss::sstring(sv);
            } else if (_key == "cleanup_policy_bitflags") {
                cleanup_policy_bitflags_sv = ss::sstring(sv);
            } else if (_key == "compaction_strategy") {
                compaction_strategy_sv = ss::sstring(sv);
            } else if (_key == "timestamp_type") {
                timestamp_type_sv = ss::sstring(sv);
            } else if (_key == "virtual_cluster_id") {
                virtual_cluster_id_sv = ss::sstring(sv);
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

    bool Int(int i) { return Int64(i); }

    bool Int64(int64_t i) {
        if (i >= 0) {
            // Should only be called when negative, but doesn't hurt to just
            // defer to the unsigned variant.
            return Uint64(i);
        }
        switch (_state) {
        case state::expect_value:
            if (_key == "version") {
                _version = i;
            } else if (_key == "partition_count") {
                _partition_count = i;
            } else if (_key == "replication_factor") {
                _replication_factor = i;
            } else if (_key == "revision_id") {
                _revision_id = model::initial_revision_id{i};
            } else if (_key == "segment_size") {
                // NOTE: segment size and retention bytes are unsigned, but
                // older versions of Redpanda could serialize them as negative.
                // Just leave them empty.
                _properties.segment_size = std::nullopt;
            } else if (_key == "retention_bytes") {
                _properties.retention_bytes = tristate<size_t>{
                  disable_tristate};
            } else if (_key == "retention_duration") {
                // even though a negative number is valid for milliseconds,
                // interpret any negative value as a request for infinite
                // retention, that translates to a disabled tristate (like for
                // retention_bytes)
                _properties.retention_duration
                  = tristate<std::chrono::milliseconds>(disable_tristate);
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
            if (_key == "version") {
                _version = u;
            } else if (_key == "partition_count") {
                _partition_count = u;
            } else if (_key == "replication_factor") {
                _replication_factor = u;
            } else if (_key == "revision_id") {
                _revision_id = model::initial_revision_id(u);
            } else if (_key == "segment_size") {
                _properties.segment_size = u;
            } else if (_key == "retention_bytes") {
                _properties.retention_bytes = tristate<size_t>(u);
            } else if (_key == "retention_duration") {
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
            if (_key == "retention_bytes") {
                _properties.retention_bytes = tristate<size_t>{std::nullopt};
            } else if (_key == "retention_duration") {
                _properties.retention_duration
                  = tristate<std::chrono::milliseconds>{std::nullopt};
            }

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

    // NOTE: version is no longer explicitly tracked, now that we use the
    // versioned topic_manifest_state to serialize.
    std::optional<int32_t> _version;

    std::optional<model::ns> _namespace{};
    std::optional<model::topic> _topic;
    std::optional<int32_t> _partition_count;
    std::optional<int16_t> _replication_factor;
    std::optional<model::initial_revision_id> _revision_id{};

    // optional fields
    cluster::topic_properties _properties{};
    std::optional<ss::sstring> compaction_strategy_sv;
    std::optional<ss::sstring> timestamp_type_sv;
    std::optional<ss::sstring> compression_sv;
    std::optional<ss::sstring> cleanup_policy_bitflags_sv;
    std::optional<ss::sstring> virtual_cluster_id_sv;

    topic_manifest_handler() noexcept {
        // tristate decoding requires that the default starting value is
        // `disabled_tristate`
        _properties.retention_bytes = tristate<size_t>(disable_tristate),
        _properties.retention_duration = tristate<std::chrono::milliseconds>(
          disable_tristate);
    };
};

topic_manifest::topic_manifest(
  const cluster::topic_configuration& cfg, model::initial_revision_id rev)
  : _topic_config(cfg)
  , _rev(rev) {}

topic_manifest::topic_manifest()
  : _topic_config(std::nullopt) {}

void topic_manifest::do_update(const topic_manifest_handler& handler) {
    if (handler._version != topic_manifest::first_version) {
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

    _topic_config = cluster::topic_configuration{
      model::ns(handler._namespace.value()),
      model::topic(handler._topic.value()),
      handler._partition_count.value(),
      handler._replication_factor.value()};
    _topic_config.value().properties = handler._properties;

    if (handler.compaction_strategy_sv) {
        try {
            _topic_config.value().properties.compaction_strategy
              = boost::lexical_cast<model::compaction_strategy>(
                handler.compaction_strategy_sv.value());
        } catch (const std::runtime_error& e) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to parse topic manifest {}: Invalid compaction_strategy: "
              "{}",
              display_name(),
              handler.compaction_strategy_sv.value()));
        }
    }
    if (handler.timestamp_type_sv) {
        try {
            _topic_config.value().properties.timestamp_type
              = boost::lexical_cast<model::timestamp_type>(
                handler.timestamp_type_sv.value());
        } catch (const std::runtime_error& e) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to parse topic manifest {}: Invalid timestamp_type "
              "value: {}",
              display_name(),
              handler.timestamp_type_sv.value()));
        }
    }
    if (handler.compression_sv) {
        try {
            _topic_config.value().properties.compression
              = boost::lexical_cast<model::compression>(
                handler.compression_sv.value());
        } catch (const boost::bad_lexical_cast& e) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to parse topic manifest {}: Invalid compression value: "
              "{}",
              display_name(),
              handler.compression_sv.value()));
        }
    }
    if (handler.cleanup_policy_bitflags_sv) {
        try {
            _topic_config.value().properties.cleanup_policy_bitflags
              = boost::lexical_cast<model::cleanup_policy_bitflags>(
                handler.cleanup_policy_bitflags_sv.value());
        } catch (const std::runtime_error& e) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to parse topic manifest {}: Invalid "
              "cleanup_policy_bitflags value: {}",
              display_name(),
              handler.cleanup_policy_bitflags_sv.value()));
        }
    }

    if (handler.virtual_cluster_id_sv) {
        try {
            _topic_config.value().properties.mpx_virtual_cluster_id
              = boost::lexical_cast<model::vcluster_id>(
                handler.virtual_cluster_id_sv.value());
        } catch (const std::runtime_error& e) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to parse topic manifest {}: Invalid "
              "virtual_cluster_id_sv value: {}",
              display_name(),
              handler.virtual_cluster_id_sv.value()));
        }
    }
}

ss::future<>
topic_manifest::update(manifest_format format, ss::input_stream<char> is) {
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os);

    vlog(cst_log.debug, "Parsing topic manifest with format {}", format);

    switch (format) {
    case manifest_format::json: {
        /// Backwards compatibility with legacy topic manifest
        /// written by older versions of Redpanda.

        iobuf_istreambuf ibuf(result);
        std::istream stream(&ibuf);

        json::IStreamWrapper wrapper(stream);
        json::Reader reader;
        topic_manifest_handler handler;
        if (reader.Parse(wrapper, handler)) {
            vlog(cst_log.debug, "Parsed successfully!");
            topic_manifest::do_update(handler);
        } else {
            rapidjson::ParseErrorCode e = reader.GetParseErrorCode();
            size_t o = reader.GetErrorOffset();

            if (_topic_config) {
                throw std::runtime_error(fmt_with_ctx(
                  fmt::format,
                  "Failed to parse topic manifest {}: {} at offset {}",
                  display_name(),
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
        break;
    }
    case manifest_format::serde:
        // serde format is straightforward: the buffer is a
        // topic_manifest_state
        auto m_state = serde::from_iobuf<topic_manifest_state>(
          std::move(result));
        _topic_config = std::move(m_state.cfg);
        _rev = m_state.initial_revision;
        break;
    }

    co_return;
}

ss::future<iobuf> topic_manifest::serialize_buf() const {
    vassert(_topic_config.has_value(), "_topic_config is not initialized");
    // serialize in binary format
    return ss::make_ready_future<iobuf>(serde::to_iobuf(
      topic_manifest_state{
        .cfg = _topic_config.value(), .initial_revision = _rev}));
}

ss::sstring topic_manifest::display_name() const {
    // The path is <prefix>/meta/<ns>/<topic>/topic_manifest.json
    vassert(_topic_config, "Topic config is not set");
    return fmt::format("tp_ns: {}, rev: {}", _topic_config.value().tp_ns, _rev);
}

remote_manifest_path topic_manifest::get_manifest_path(
  const remote_path_provider& path_provider) const {
    vassert(_topic_config, "Topic config is not set");
    return remote_manifest_path{
      path_provider.topic_manifest_path(_topic_config.value().tp_ns, _rev)};
}

} // namespace cloud_storage
