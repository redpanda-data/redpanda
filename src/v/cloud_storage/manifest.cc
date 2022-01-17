/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/manifest.h"

#include "bytes/iobuf.h"
#include "bytes/iobuf_istreambuf.h"
#include "bytes/iobuf_ostreambuf.h"
#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "hashing/xx.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "ssx/sformat.h"
#include "storage/fs_utils.h"
#include "storage/ntp_config.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <ctll/fixed_string.hpp>
#include <ctre/functions.hpp>
#include <fmt/ostream.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <variant>

namespace cloud_storage {

std::ostream& operator<<(std::ostream& s, const manifest_path_components& c) {
    fmt::print(
      s, "{{{}: {}-{}-{}-{}}}", c._origin, c._ns, c._topic, c._part, c._rev);
    return s;
}

static bool parse_partition_and_revision(
  std::string_view s, manifest_path_components& comp) {
    auto pos = s.find('_');
    if (pos == std::string_view::npos) {
        // Invalid segment file name
        return false;
    }
    uint64_t res = 0;
    // parse first component
    auto sv = s.substr(0, pos);
    auto e = std::from_chars(sv.data(), sv.data() + sv.size(), res);
    if (e.ec != std::errc()) {
        return false;
    }
    comp._part = model::partition_id(res);
    // parse second component
    sv = s.substr(pos + 1);
    e = std::from_chars(sv.data(), sv.data() + sv.size(), res);
    if (e.ec != std::errc()) {
        return false;
    }
    comp._rev = model::initial_revision_id(res);
    return true;
}

std::optional<manifest_path_components>
get_manifest_path_components(const std::filesystem::path& path) {
    // example: b0000000/meta/kafka/redpanda-test/4_2/manifest.json
    enum {
        ix_prefix,
        ix_meta,
        ix_namespace,
        ix_topic,
        ix_part_rev,
        ix_file_name,
        total_components
    };
    manifest_path_components res;
    res._origin = path;
    int ix = 0;
    for (const auto& c : path) {
        ss::sstring p = c.string();
        switch (ix++) {
        case ix_prefix:
            break;
        case ix_namespace:
            res._ns = model::ns(std::move(p));
            break;
        case ix_topic:
            res._topic = model::topic(std::move(p));
            break;
        case ix_part_rev:
            if (!parse_partition_and_revision(p, res)) {
                return std::nullopt;
            }
            break;
        case ix_file_name:
            if (p != "manifest.json") {
                return std::nullopt;
            }
            break;
        }
    }
    if (ix == total_components) {
        return res;
    }
    return std::nullopt;
}

std::optional<segment_name_components>
parse_segment_name(const segment_name& name) {
    auto parsed = storage::segment_path::parse_segment_filename(name);
    if (!parsed) {
        return std::nullopt;
    }
    return segment_name_components{
      .base_offset = parsed->base_offset,
      .term = parsed->term,
    };
}

remote_segment_path generate_remote_segment_path(
  const model::ntp& ntp,
  model::initial_revision_id rev_id,
  const segment_name& name,
  model::term_id archiver_term) {
    vassert(
      rev_id != model::initial_revision_id(),
      "ntp {}: ntp revision must be known for segment {}",
      ntp,
      name);

    auto path = ssx::sformat("{}_{}/{}", ntp.path(), rev_id(), name());
    uint32_t hash = xxhash_32(path.data(), path.size());
    if (archiver_term != model::term_id{}) {
        return remote_segment_path(
          fmt::format("{:08x}/{}.{}", hash, path, archiver_term()));
    } else {
        return remote_segment_path(fmt::format("{:08x}/{}", hash, path));
    }
}

segment_name generate_segment_name(model::offset o, model::term_id t) {
    return segment_name(ssx::sformat("{}-{}-v1.log", o(), t()));
}

manifest::manifest()
  : _ntp()
  , _rev()
  , _last_offset(0) {}

manifest::manifest(model::ntp ntp, model::initial_revision_id rev)
  : _ntp(std::move(ntp))
  , _rev(rev)
  , _last_offset(0) {}

// NOTE: the methods that generate remote paths use the xxhash function
// to randomize the prefix. S3 groups the objects into chunks based on
// these prefixes. It also applies rate limit to chunks so if all segments
// and manifests will have the same prefix we will be able to do around
// 3000-5000 req/sec. AWS doc mentions that having only two prefix
// characters should be enough for most workloads
// (https://aws.amazon.com/blogs/aws/amazon-s3-performance-tips-tricks-seattle-hiring-event/)
// We're using eight because it's free and because AWS S3 is not the only
// backend and other S3 API implementations might benefit from that.

static remote_manifest_path generate_partition_manifest_path(
  const model::ntp& ntp, model::initial_revision_id rev) {
    // NOTE: the idea here is to split all possible hash values into
    // 16 bins. Every bin should have lowest 28-bits set to 0.
    // As result, for segment names all prefixes are possible, but
    // for manifests, only 0x00000000, 0x10000000, ... 0xf0000000
    // are used. This will allow us to quickly find all manifests
    // that S3 bucket contains.
    constexpr uint32_t bitmask = 0xF0000000;
    auto path = ssx::sformat("{}_{}", ntp.path(), rev());
    uint32_t hash = bitmask & xxhash_32(path.data(), path.size());
    return remote_manifest_path(
      fmt::format("{:08x}/meta/{}_{}/manifest.json", hash, ntp.path(), rev()));
}

remote_manifest_path manifest::get_manifest_path() const {
    return generate_partition_manifest_path(_ntp, _rev);
}

const model::ntp& manifest::get_ntp() const { return _ntp; }

const model::offset manifest::get_last_offset() const { return _last_offset; }

model::initial_revision_id manifest::get_revision_id() const { return _rev; }

remote_segment_path manifest::generate_segment_path(
  const manifest::key& key, const segment_meta& meta) const {
    auto name = generate_segment_name(key.base_offset, key.term);
    return generate_remote_segment_path(
      _ntp, meta.ntp_revision, name, meta.archiver_term);
}

manifest::const_iterator manifest::begin() const { return _segments.begin(); }

manifest::const_iterator manifest::end() const { return _segments.end(); }

manifest::const_reverse_iterator manifest::rbegin() const {
    return _segments.rbegin();
}

manifest::const_reverse_iterator manifest::rend() const {
    return _segments.rend();
}

size_t manifest::size() const { return _segments.size(); }

bool manifest::contains(const manifest::key& key) const {
    return _segments.contains(key);
}

bool manifest::contains(const segment_name& name) const {
    auto maybe_key = parse_segment_name(name);
    if (!maybe_key) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "can't parse segment name \"{}\"", name));
    }
    key key = {.base_offset = maybe_key->base_offset, .term = maybe_key->term};
    return _segments.contains(key);
}

bool manifest::add(const manifest::key& key, const segment_meta& meta) {
    auto [it, ok] = _segments.insert(std::make_pair(key, meta));
    if (ok && it->second.ntp_revision == model::initial_revision_id{}) {
        it->second.ntp_revision = _rev;
    }
    _last_offset = std::max(meta.committed_offset, _last_offset);
    return ok;
}

bool manifest::add(const segment_name& name, const segment_meta& meta) {
    auto maybe_key = parse_segment_name(name);
    if (!maybe_key) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "can't parse segment name \"{}\"", name));
    }
    key key = {.base_offset = maybe_key->base_offset, .term = maybe_key->term};
    return add(key, meta);
}

const manifest::segment_meta* manifest::get(const manifest::key& key) const {
    auto it = _segments.find(key);
    if (it == _segments.end()) {
        return nullptr;
    }
    return &it->second;
}

const manifest::segment_meta* manifest::get(const segment_name& name) const {
    auto maybe_key = parse_segment_name(name);
    if (!maybe_key) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "can't parse segment name \"{}\"", name));
    }
    key key = {.base_offset = maybe_key->base_offset, .term = maybe_key->term};
    return get(key);
}

manifest::const_iterator manifest::find(model::offset o) const {
    auto it = _segments.lower_bound(
      {.base_offset = o, .term = model::term_id(0)});
    if (it == _segments.end() || it->first.base_offset != o) {
        return end();
    }
    return it;
}

std::insert_iterator<manifest::segment_map> manifest::get_insert_iterator() {
    return std::inserter(_segments, _segments.begin());
}

manifest manifest::difference(const manifest& remote_set) const {
    vassert(
      _ntp == remote_set._ntp && _rev == remote_set._rev,
      "Local manifest {}-{} and remote {}-{} doesn't match",
      _ntp,
      _rev,
      remote_set._ntp,
      remote_set._rev);
    manifest result(_ntp, _rev);
    std::set_difference(
      begin(),
      end(),
      remote_set.begin(),
      remote_set.end(),
      result.get_insert_iterator());
    return result;
}

ss::future<> manifest::update(ss::input_stream<char> is) {
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

void manifest::update(const json::Document& m) {
    auto ver = model::partition_id(m["version"].GetInt());
    if (ver != static_cast<int>(manifest_version::v1)) {
        throw std::runtime_error("manifest version not supported");
    }
    auto ns = model::ns(m["namespace"].GetString());
    auto tp = model::topic(m["topic"].GetString());
    auto pt = model::partition_id(m["partition"].GetInt());
    _rev = model::initial_revision_id(m["revision"].GetInt());
    _ntp = model::ntp(ns, tp, pt);
    _last_offset = model::offset(m["last_offset"].GetInt64());
    segment_map tmp;
    if (m.HasMember("segments")) {
        const auto& s = m["segments"].GetObject();
        for (auto it = s.MemberBegin(); it != s.MemberEnd(); it++) {
            auto name = segment_name{it->name.GetString()};
            auto parsed_key = parse_segment_name(name);
            if (!parsed_key) {
                throw std::runtime_error(fmt_with_ctx(
                  fmt::format, "can't parse segment name \"{}\"", name));
            }
            key key = {
              .base_offset = parsed_key->base_offset, .term = parsed_key->term};
            auto coffs = it->value["committed_offset"].GetInt64();
            auto boffs = it->value["base_offset"].GetInt64();
            auto size_bytes = it->value["size_bytes"].GetInt64();
            model::timestamp base_timestamp = model::timestamp::missing();
            if (it->value.HasMember("base_timestamp")) {
                base_timestamp = model::timestamp(
                  it->value["base_timestamp"].GetInt64());
            }
            model::timestamp max_timestamp = model::timestamp::missing();
            if (it->value.HasMember("max_timestamp")) {
                max_timestamp = model::timestamp(
                  it->value["max_timestamp"].GetInt64());
            }
            model::offset delta_offset = model::offset::min();
            if (it->value.HasMember("delta_offset")) {
                delta_offset = model::offset(
                  it->value["delta_offset"].GetInt64());
            }
            model::initial_revision_id ntp_revision = _rev;
            if (it->value.HasMember("ntp_revision")) {
                ntp_revision = model::initial_revision_id(
                  it->value["ntp_revision"].GetInt64());
            }
            model::term_id archiver_term;
            if (it->value.HasMember("archiver_term")) {
                archiver_term = model::term_id(
                  it->value["archiver_term"].GetInt64());
            }
            segment_meta meta{
              .is_compacted = it->value["is_compacted"].GetBool(),
              .size_bytes = static_cast<size_t>(size_bytes),
              .base_offset = model::offset(boffs),
              .committed_offset = model::offset(coffs),
              .base_timestamp = base_timestamp,
              .max_timestamp = max_timestamp,
              .delta_offset = delta_offset,
              .ntp_revision = ntp_revision,
              .archiver_term = archiver_term,
            };
            tmp.insert(std::make_pair(key, meta));
        }
    }
    std::swap(tmp, _segments);
}

serialized_json_stream manifest::serialize() const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialize(os);
    size_t size_bytes = serialized.size_bytes();
    return {
      .stream = make_iobuf_input_stream(std::move(serialized)),
      .size_bytes = size_bytes};
}

void manifest::serialize(std::ostream& out) const {
    rapidjson::OStreamWrapper wrapper(out);
    rapidjson::Writer<rapidjson::OStreamWrapper> w(wrapper);
    w.StartObject();
    w.Key("version");
    w.Int(static_cast<int>(manifest_version::v1));
    w.Key("namespace");
    w.String(_ntp.ns().c_str());
    w.Key("topic");
    w.String(_ntp.tp.topic().c_str());
    w.Key("partition");
    w.Int64(_ntp.tp.partition());
    w.Key("revision");
    w.Int64(_rev());
    w.Key("last_offset");
    w.Int64(_last_offset());
    if (!_segments.empty()) {
        w.Key("segments");
        w.StartObject();
        for (const auto& [key, meta] : _segments) {
            auto sn = generate_segment_name(key.base_offset, key.term);
            w.Key(sn());
            w.StartObject();
            w.Key("is_compacted");
            w.Bool(meta.is_compacted);
            w.Key("size_bytes");
            w.Int64(meta.size_bytes);
            w.Key("committed_offset");
            w.Int64(meta.committed_offset());
            w.Key("base_offset");
            w.Int64(meta.base_offset());
            if (meta.base_timestamp != model::timestamp::missing()) {
                w.Key("base_timestamp");
                w.Int64(meta.base_timestamp.value());
            }
            if (meta.max_timestamp != model::timestamp::missing()) {
                w.Key("max_timestamp");
                w.Int64(meta.max_timestamp.value());
            }
            if (meta.delta_offset != model::offset::min()) {
                w.Key("delta_offset");
                w.Int64(meta.delta_offset());
            }
            if (meta.ntp_revision != _rev) {
                vassert(
                  meta.ntp_revision != model::initial_revision_id(),
                  "ntp {}: missing ntp_revision for segment {} in the manifest",
                  _ntp,
                  sn);
                w.Key("ntp_revision");
                w.Int64(meta.ntp_revision());
            }
            if (meta.archiver_term != model::term_id::min()) {
                w.Key("archiver_term");
                w.Int64(meta.archiver_term());
            }
            w.EndObject();
        }
        w.EndObject();
    }
    w.EndObject();
}

bool manifest::delete_permanently(const manifest::key& key) {
    auto it = _segments.find(key);
    if (it != _segments.end()) {
        _segments.erase(it);
        return true;
    }
    return false;
}

std::ostream& operator<<(std::ostream& o, const manifest::key& k) {
    o << generate_segment_name(k.base_offset, k.term);
    return o;
}

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
    rapidjson::Writer<rapidjson::OStreamWrapper> w(wrapper);
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
