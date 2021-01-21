/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/manifest.h"

#include "archival/logger.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_istreambuf.h"
#include "bytes/iobuf_ostreambuf.h"
#include "hashing/murmur.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "storage/ntp_config.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

#include <boost/algorithm/string.hpp>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <array>

namespace archival {

manifest::manifest()
  : _ntp()
  , _rev() {}

manifest::manifest(model::ntp ntp, model::revision_id rev)
  : _ntp(std::move(ntp))
  , _rev(rev) {}

remote_manifest_path manifest::get_manifest_path() const {
    // NOTE: the idea here is to split all possible hash values into
    // 16 bins. Every bin should have lowest 28-bits set to 0.
    // As result, for segment names all prefixes are possible, but
    // for manifests, only 0x00000000, 0x10000000, ... 0xf0000000
    // are used. This will allow us to quickly find all manifests
    // that S3 bucket contains.
    constexpr uint32_t bitmask = 0xF0000000;
    auto path = fmt::format("{}_{}", _ntp.path(), _rev());
    uint32_t hash = bitmask & murmurhash3_x86_32(path.data(), path.size());
    return remote_manifest_path(fmt::format(
      "{:08x}/meta/{}_{}/manifest.json", hash, _ntp.path(), _rev()));
}

remote_segment_path
manifest::get_remote_segment_path(const segment_name& name) const {
    auto path = fmt::format("{}_{}/{}", _ntp.path(), _rev(), name());
    uint32_t hash = murmurhash3_x86_32(path.data(), path.size());
    return remote_segment_path(fmt::format("{:08x}/{}", hash, path));
}

const model::ntp& manifest::get_ntp() const { return _ntp; }

model::revision_id manifest::get_revision_id() const { return _rev; }

manifest::const_iterator manifest::begin() const { return _segments.begin(); }

manifest::const_iterator manifest::end() const { return _segments.end(); }

size_t manifest::size() const { return _segments.size(); }

bool manifest::contains(const segment_name& obj) const {
    return _segments.count(obj) != 0;
}

bool manifest::add(const segment_name& key, const segment_meta& meta) {
    auto [it, ok] = _segments.insert(std::make_pair(key, meta));
    return ok;
}

const manifest::segment_meta* manifest::get(const segment_name& key) const {
    auto it = _segments.find(key);
    if (it == _segments.end()) {
        return nullptr;
    }
    return &it->second;
}

std::insert_iterator<manifest::segment_map> manifest::get_insert_iterator() {
    return std::inserter(_segments, _segments.begin());
}

manifest manifest::difference(const manifest& remote_set) const {
    if (_ntp != remote_set._ntp && _rev != remote_set._rev) {
        throw std::logic_error(fmt_with_ctx(
          fmt::format,
          "{}-{} do not match {}-{}",
          _ntp,
          _rev,
          remote_set._ntp,
          remote_set._rev));
    }
    manifest result(_ntp, _rev);
    std::set_difference(
      begin(),
      end(),
      remote_set.begin(),
      remote_set.end(),
      result.get_insert_iterator());
    return result;
}

ss::future<> manifest::update(ss::input_stream<char>&& is) {
    using namespace rapidjson;
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os);
    iobuf_istreambuf ibuf(result);
    std::istream stream(&ibuf);
    Document m;
    IStreamWrapper wrapper(stream);
    m.ParseStream(wrapper);
    update(m);
    co_return;
}

void manifest::update(const rapidjson::Document& m) {
    using namespace rapidjson;
    auto ns = model::ns(m["namespace"].GetString());
    auto tp = model::topic(m["topic"].GetString());
    auto pt = model::partition_id(m["partition"].GetInt());
    _rev = model::revision_id(m["revision"].GetInt());
    _ntp = model::ntp(ns, tp, pt);
    segment_map tmp;
    if (m.HasMember("segments")) {
        const auto& s = m["segments"].GetObject();
        for (auto it = s.MemberBegin(); it != s.MemberEnd(); it++) {
            auto name = segment_name(it->name.GetString());
            auto coffs = it->value["committed_offset"].GetInt64();
            auto boffs = it->value["base_offset"].GetInt64();
            auto size_bytes = it->value["size_bytes"].GetInt64();
            segment_meta meta{
              .is_compacted = it->value["is_compacted"].GetBool(),
              .size_bytes = static_cast<size_t>(size_bytes),
              .base_offset = model::offset(boffs),
              .committed_offset = model::offset(coffs),
              .is_deleted_locally = it->value["deleted"].GetBool(),
            };
            tmp.insert(std::make_pair(name, meta));
        }
    }
    std::swap(tmp, _segments);
}

std::tuple<ss::input_stream<char>, size_t> manifest::serialize() const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialize(os);
    size_t size_bytes = serialized.size_bytes();
    return std::make_tuple(
      make_iobuf_input_stream(std::move(serialized)), size_bytes);
}

void manifest::serialize(std::ostream& out) const {
    using namespace rapidjson;
    OStreamWrapper wrapper(out);
    Writer<OStreamWrapper> w(wrapper);
    w.StartObject();
    w.Key("namespace");
    w.String(_ntp.ns().c_str());
    w.Key("topic");
    w.String(_ntp.tp.topic().c_str());
    w.Key("partition");
    w.Int64(_ntp.tp.partition());
    w.Key("revision");
    w.Int64(_rev());
    if (!_segments.empty()) {
        w.Key("segments");
        w.StartObject();
        for (const auto& [sn, meta] : _segments) {
            w.Key(sn().c_str());
            w.StartObject();
            w.Key("is_compacted");
            w.Bool(meta.is_compacted);
            w.Key("size_bytes");
            w.Int64(meta.size_bytes);
            w.Key("committed_offset");
            w.Int64(meta.committed_offset());
            w.Key("base_offset");
            w.Int64(meta.base_offset());
            w.Key("deleted");
            w.Bool(meta.is_deleted_locally);
            w.EndObject();
        }
        w.EndObject();
    }
    w.EndObject();
}

bool manifest::delete_permanently(const segment_name& name) {
    auto it = _segments.find(name);
    if (it != _segments.end()) {
        _segments.erase(it);
        return true;
    }
    return false;
}

bool manifest::mark_as_deleted(const segment_name& name) {
    auto it = _segments.find(name);
    if (it != _segments.end() && it->second.is_deleted_locally == false) {
        it->second.is_deleted_locally = true;
        return true;
    }
    return false;
}

} // namespace archival
