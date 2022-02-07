/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/topic_manifest.h"

#include "bytes/iobuf_istreambuf.h"
#include "bytes/iobuf_ostreambuf.h"
#include "cloud_storage/types.h"
#include "hashing/xx.h"
#include "json/writer.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>

#include <boost/lexical_cast.hpp>
#include <fmt/ostream.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/rapidjson.h>

#include <chrono>

namespace cloud_storage {
topic_manifest::topic_manifest(
  const cluster::topic_configuration& cfg, model::initial_revision_id rev)
  : _topic_config(cfg)
  , _rev(rev) {}

topic_manifest::topic_manifest()
  : _topic_config(std::nullopt) {}

ss::future<> topic_manifest::update(ss::input_stream<char> is) {
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os);
    iobuf_istreambuf ibuf(result);
    std::istream stream(&ibuf);
    json::Document m;
    rapidjson::IStreamWrapper wrapper(stream);
    m.ParseStream(wrapper);
    update(m);
    co_return;
}

void topic_manifest::update(const json::Document& m) {
    auto ver = m["version"].GetInt();
    if (ver != static_cast<int>(topic_manifest_version::v1)) {
        throw std::runtime_error("topic manifest version not supported");
    }
    auto ns = model::ns(m["namespace"].GetString());
    auto tp = model::topic(m["topic"].GetString());
    int32_t partitions = m["partition_count"].GetInt();
    int16_t rf = m["replication_factor"].GetInt();
    int32_t rev = m["revision_id"].GetInt();
    cluster::topic_configuration conf(ns, tp, partitions, rf);
    // optional
    if (!m["compression"].IsNull()) {
        conf.properties.compression = boost::lexical_cast<model::compression>(
          m["compression"].GetString());
    }
    if (!m["cleanup_policy_bitflags"].IsNull()) {
        conf.properties.cleanup_policy_bitflags
          = boost::lexical_cast<model::cleanup_policy_bitflags>(
            m["cleanup_policy_bitflags"].GetString());
    }
    if (!m["compaction_strategy"].IsNull()) {
        conf.properties.compaction_strategy
          = boost::lexical_cast<model::compaction_strategy>(
            m["compaction_strategy"].GetString());
    }
    if (!m["timestamp_type"].IsNull()) {
        conf.properties.timestamp_type
          = boost::lexical_cast<model::timestamp_type>(
            m["timestamp_type"].GetString());
    }
    if (!m["segment_size"].IsNull()) {
        conf.properties.segment_size = m["segment_size"].GetInt64();
    }
    // tristate
    if (m.HasMember("retention_bytes")) {
        if (!m["retention_bytes"].IsNull()) {
            conf.properties.retention_bytes = tristate<size_t>(
              m["retention_bytes"].GetInt64());
        } else {
            conf.properties.retention_bytes = tristate<size_t>(std::nullopt);
        }
    }
    if (m.HasMember("retention_duration")) {
        if (!m["retention_duration"].IsNull()) {
            conf.properties.retention_duration
              = tristate<std::chrono::milliseconds>(
                std::chrono::milliseconds(m["retention_duration"].GetInt64()));
        } else {
            conf.properties.retention_duration
              = tristate<std::chrono::milliseconds>(std::nullopt);
        }
    }
    _topic_config = conf;
    _rev = model::initial_revision_id(rev);
}

serialized_json_stream topic_manifest::serialize() const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialize(os);
    size_t size_bytes = serialized.size_bytes();
    return {
      .stream = make_iobuf_input_stream(std::move(serialized)),
      .size_bytes = size_bytes};
}

void topic_manifest::serialize(std::ostream& out) const {
    rapidjson::OStreamWrapper wrapper(out);
    json::Writer<rapidjson::OStreamWrapper> w(wrapper);
    w.StartObject();
    w.Key("version");
    w.Int(static_cast<int>(manifest_version::v1));
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
        if (_topic_config->properties.retention_bytes.has_value()) {
            w.Int64(_topic_config->properties.retention_bytes.value());
        } else {
            w.Null();
        }
    }
    if (!_topic_config->properties.retention_duration.is_disabled()) {
        w.Key("retention_duration");
        if (_topic_config->properties.retention_duration.has_value()) {
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

std::vector<remote_manifest_path>
topic_manifest::get_partition_manifests() const {
    std::vector<remote_manifest_path> result;
    const int32_t npart = _topic_config->partition_count;
    result.reserve(npart);
    for (int32_t i = 0; i < npart; i++) {
        model::ntp ntp(
          _topic_config->tp_ns.ns(),
          _topic_config->tp_ns.tp(),
          model::partition_id(i));
        auto path = generate_partition_manifest_path(ntp, _rev);
        result.emplace_back(std::move(path));
    }
    return result;
}
} // namespace cloud_storage
