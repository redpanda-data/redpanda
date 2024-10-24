/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/cloud_metadata/cluster_manifest.h"

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "bytes/streambuf.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "utils/uuid.h"

#include <boost/uuid/uuid_io.hpp>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>

namespace cluster::cloud_metadata {

ss::future<> cluster_metadata_manifest::update(ss::input_stream<char> is) {
    using namespace rapidjson;
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os).finally([&is, &os]() mutable {
        return is.close().finally([&os]() mutable { return os.close(); });
    });
    iobuf_istreambuf ibuf(result);
    std::istream stream(&ibuf);
    Document doc;
    IStreamWrapper wrapper(stream);
    doc.ParseStream(wrapper);
    load_from_json(doc);
}

void cluster_metadata_manifest::load_from_json(const rapidjson::Document& doc) {
    if (!doc.HasMember("version")) {
        throw std::runtime_error("missing 'version' field");
    }
    if (!doc.HasMember("compat_version")) {
        throw std::runtime_error("missing 'version' field");
    }
    auto version = doc["version"].GetInt();
    auto compat_version = doc["compat_version"].GetInt();
    if (
      compat_version
      > static_cast<int>(cluster_metadata_manifest::redpanda_serde_version)) {
        throw std::runtime_error(fmt::format(
          "Can't deserialize cluster manifest, supported version {}, manifest "
          "version {}, compatible version {}",
          static_cast<int>(cluster_metadata_manifest::redpanda_serde_version),
          version,
          compat_version));
    }
    if (doc.HasMember("cluster_uuid")) {
        std::string uuid_str(doc["cluster_uuid"].GetString());
        try {
            auto u = boost::lexical_cast<uuid_t::underlying_t>(uuid_str);
            std::vector<uint8_t> uuid_vec{u.begin(), u.end()};
            cluster_uuid = model::cluster_uuid(uuid_vec);
        } catch (...) {
            throw std::runtime_error(fmt::format(
              "Failed to deserialize 'cluster_uuid' field {}: {}",
              uuid_str,
              std::current_exception()));
        }
    }
    if (doc.HasMember("upload_time_since_epoch")) {
        upload_time_since_epoch = duration(
          doc["upload_time_since_epoch"].GetInt64());
    }
    if (doc.HasMember("metadata_id")) {
        metadata_id = cluster_metadata_id(doc["metadata_id"].GetInt64());
    }
    if (doc.HasMember("controller_snapshot_offset")) {
        controller_snapshot_offset = model::offset(
          doc["controller_snapshot_offset"].GetInt64());
    }
    if (doc.HasMember("controller_snapshot_path")) {
        controller_snapshot_path = doc["controller_snapshot_path"].GetString();
    }
    if (doc.HasMember("offsets_snapshots_by_partition")) {
        auto snapshot_paths_json
          = doc["offsets_snapshots_by_partition"].GetArray();
        offsets_snapshots_by_partition.reserve(snapshot_paths_json.Size());
        for (const auto& partition_snap_paths_json : snapshot_paths_json) {
            auto partition_array_json = partition_snap_paths_json.GetArray();
            std::vector<ss::sstring> paths_for_partition;
            paths_for_partition.reserve(partition_snap_paths_json.Size());
            for (const auto& path_json : partition_array_json) {
                paths_for_partition.emplace_back(path_json.GetString());
            }
            offsets_snapshots_by_partition.emplace_back(
              std::move(paths_for_partition));
        }
    }
}

ss::future<iobuf> cluster_metadata_manifest::serialize_buf() const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    to_json(os);
    if (!os.good()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "could not serialize tx range manifest {}",
          get_manifest_path()));
    }
    co_return serialized;
}

void cluster_metadata_manifest::to_json(std::ostream& out) const {
    using namespace rapidjson;
    OStreamWrapper wrapper(out);
    Writer<OStreamWrapper> w(wrapper);
    w.StartObject();
    w.Key("version");
    w.Int(static_cast<int>(cluster_metadata_manifest::redpanda_serde_version));
    w.Key("compat_version");
    w.Int(static_cast<int>(
      cluster_metadata_manifest::redpanda_serde_compat_version));
    w.Key("cluster_uuid");
    w.String(ss::sstring(cluster_uuid()));
    w.Key("upload_time_since_epoch");
    w.Int64(static_cast<int64_t>(upload_time_since_epoch.count()));
    w.Key("metadata_id");
    w.Int64(static_cast<int64_t>(metadata_id()));
    w.Key("controller_snapshot_offset");
    w.Int64(controller_snapshot_offset());
    w.Key("controller_snapshot_path");
    w.String(controller_snapshot_path);

    w.Key("offsets_snapshots_by_partition");
    w.StartArray();
    for (const auto& paths : offsets_snapshots_by_partition) {
        w.StartArray();
        for (const auto& p : paths) {
            w.String(p);
        }
        w.EndArray();
    }
    w.EndArray();
    w.EndObject();
}

cloud_storage::remote_manifest_path
cluster_metadata_manifest::get_manifest_path() const {
    return cluster_manifest_key(cluster_uuid, metadata_id);
}

std::ostream& operator<<(std::ostream& os, const cluster_metadata_manifest& m) {
    os << fmt::format(
      "{{upload_time_since_epoch: {}, cluster_uuid: {}, metadata_id: {}, "
      "controller_snapshot_offset: {}, controller_snapshot_path: '{}', "
      "offsets_snapshots_by_partition: '{}'}}",
      m.upload_time_since_epoch,
      m.cluster_uuid,
      m.metadata_id,
      m.controller_snapshot_offset,
      m.controller_snapshot_path,
      m.offsets_snapshots_by_partition);
    return os;
}

} // namespace cluster::cloud_metadata
