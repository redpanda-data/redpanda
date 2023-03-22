/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_storage/tx_range_manifest.h"

#include "bytes/iobuf.h"
#include "bytes/iobuf_istreambuf.h"
#include "bytes/iobuf_ostreambuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/types.h"
#include "json/istreamwrapper.h"
#include "model/record.h"
#include "utils/fragmented_vector.h"

#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>

namespace cloud_storage {

remote_manifest_path generate_remote_tx_path(const remote_segment_path& path) {
    return remote_manifest_path(fmt::format("{}.tx", path().native()));
}

tx_range_manifest::tx_range_manifest(
  remote_segment_path spath, fragmented_vector<model::tx_range> range)
  : _path(std::move(spath))
  , _ranges(std::move(range)) {}

tx_range_manifest::tx_range_manifest(remote_segment_path spath)
  : _path(std::move(spath)) {}

ss::future<> tx_range_manifest::update(ss::input_stream<char> is) {
    using namespace rapidjson;
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os).finally([&is, &os]() mutable {
        return is.close().finally([&os]() mutable { return os.close(); });
    });
    iobuf_istreambuf ibuf(result);
    std::istream stream(&ibuf);
    Document m;
    IStreamWrapper wrapper(stream);
    m.ParseStream(wrapper);
    update(m);
}

void tx_range_manifest::update(const rapidjson::Document& doc) {
    _ranges = fragmented_vector<model::tx_range>();
    auto version = doc["version"].GetInt();
    auto compat_version = doc["compat_version"].GetInt();
    if (
      compat_version
      > static_cast<int>(tx_range_manifest_version::current_version)) {
        throw std::runtime_error(fmt::sprintf(
          "Can't deserialize tx manifest, supported version {}, manifest "
          "version {}, compatible version {}",
          static_cast<int32_t>(tx_range_manifest_version::current_version),
          version,
          compat_version));
    }
    if (doc.HasMember("ranges")) {
        const auto& arr = doc["ranges"].GetArray();
        for (const auto& it : arr) {
            const auto& tx_range = it.GetObject();
            auto id = tx_range["pid.id"].GetInt64();
            auto epoch = tx_range["pid.epoch"].GetInt();
            auto first = model::offset{tx_range["first"].GetInt64()};
            auto last = model::offset{tx_range["last"].GetInt64()};
            model::producer_identity pid(id, static_cast<int16_t>(epoch));
            _ranges.push_back(model::tx_range{pid, first, last});
        }
    }
    _ranges.shrink_to_fit();
}

ss::future<serialized_json_stream> tx_range_manifest::serialize() const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialize(os);
    if (!os.good()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "could not serialize tx range manifest {}",
          get_manifest_path()));
    }
    size_t size_bytes = serialized.size_bytes();
    co_return serialized_json_stream{
      .stream = make_iobuf_input_stream(std::move(serialized)),
      .size_bytes = size_bytes};
}

remote_manifest_path tx_range_manifest::get_manifest_path() const {
    return generate_remote_tx_path(_path);
}

void tx_range_manifest::serialize(std::ostream& out) const {
    using namespace rapidjson;
    OStreamWrapper wrapper(out);
    Writer<OStreamWrapper> w(wrapper);
    w.StartObject();
    w.Key("version");
    w.Int(static_cast<int>(tx_range_manifest_version::current_version));
    w.Key("compat_version");
    w.Int(static_cast<int>(tx_range_manifest_version::compat_version));
    w.Key("ranges");
    w.StartArray();
    for (const auto& tx : _ranges) {
        w.StartObject();
        w.Key("pid.id");
        w.Int64(tx.pid.id);
        w.Key("pid.epoch");
        w.Int(tx.pid.epoch);
        w.Key("first");
        w.Int64(tx.first());
        w.Key("last");
        w.Int64(tx.last());
        w.EndObject();
    }
    w.EndArray();
    w.EndObject();
}
} // namespace cloud_storage
