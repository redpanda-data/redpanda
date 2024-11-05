// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/iobuf.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/dl_version.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "utils/retry_chain_node.h"

namespace cloud_io {
class remote;
}

namespace experimental::cloud_topics::recovery {

class ntp_snapshot_prefix {
public:
    ntp_snapshot_prefix(
      model::ns ns,
      model::topic tp,
      model::partition_id partition_id,
      model::initial_revision_id rev_id)
      : _ns(std::move(ns))
      , _tp(std::move(tp))
      , _partition_id(partition_id)
      , _rev_id(rev_id) {};

public:
    explicit operator std::string() const {
        return fmt::format(
          "ct-recovery/v1/{}/{}/{}_{}/", _ns, _tp, _partition_id, _rev_id);
    }

    cloud_storage_clients::object_key object_key() const {
        return cloud_storage_clients::object_key(operator std::string());
    }

    bool operator==(const ntp_snapshot_prefix& other) const {
        return std::tie(_ns, _tp, _partition_id, _rev_id)
               == std::tie(
                 other._ns, other._tp, other._partition_id, other._rev_id);
    }

private:
    model::ns _ns;
    model::topic _tp;
    model::partition_id _partition_id;
    model::initial_revision_id _rev_id;
};

/// A unique snapshot identifier for a partition.
///
/// A higher version indicates a more recent snapshot.
class ntp_snapshot_id {
public:
    ntp_snapshot_id(
      model::ns ns,
      model::topic tp,
      model::partition_id partition_id,
      model::initial_revision_id rev_id,
      dl_version version)
      : _prefix(std::move(ns), std::move(tp), partition_id, rev_id)
      , _version(version) {};

    ntp_snapshot_id(ntp_snapshot_prefix prefix, dl_version version)
      : _prefix(std::move(prefix))
      , _version(version) {};

public:
    cloud_storage_clients::object_key object_key() const {
        // TODO(nv): We need to include cluster id / label in the key.
        //
        // Do we need to include raft term? What if partition log gets
        // truncated? Maybe not include it but increment min/next snapshot
        // version on recovery based on cloud storage contents.
        return cloud_storage_clients::object_key(
          fmt::format("{}{}.snap", std::string(_prefix), _version));
    }

    const ntp_snapshot_prefix& prefix() const { return _prefix; }
    const dl_version& version() const { return _version; }

    bool operator<(const ntp_snapshot_id& other) const {
        if (_prefix != other._prefix) {
            throw std::runtime_error(fmt::format(
              "Comparing snapshots from different partitions {} and {}",
              std::string(_prefix),
              std::string(other._prefix)));
        }

        return _version < other._version;
    }

    /// Parse an ntp_snapshot_id from a string given the prefix.
    /// Returns std::nullopt if the string does not match the expected format.
    /// The expected input format is:
    ///   <prefix>/<version>.snap
    static std::optional<ntp_snapshot_id>
      parse(ntp_snapshot_prefix, std::string_view);

private:
    ntp_snapshot_prefix _prefix;
    dl_version _version;
};

class snapshot_io {
public:
    explicit snapshot_io(cloud_io::remote*, cloud_storage_clients::bucket_name);

public:
    ss::future<> upload_snapshot(ntp_snapshot_id, iobuf, retry_chain_node&);
    ss::future<> delete_snapshots_before(ntp_snapshot_id, retry_chain_node&);
    ss::future<std::optional<ntp_snapshot_id>>
    find_latest_snapshot(ntp_snapshot_prefix, retry_chain_node&);
    ss::future<iobuf> download_snapshot(ntp_snapshot_id, retry_chain_node&);

private:
    cloud_io::remote* _cloud_io;
    cloud_storage_clients::bucket_name _bucket;
};

} // namespace experimental::cloud_topics::recovery
