// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cluster/topic_properties.h"
#include "model/metadata.h"
#include "storage/ntp_config.h"

namespace cluster {

// Structure holding topic configuration, optionals will be replaced by broker
// defaults
struct topic_configuration
  : serde::envelope<
      topic_configuration,
      serde::version<2>,
      serde::compat_version<0>> {
    topic_configuration(
      model::ns ns,
      model::topic topic,
      int32_t partition_count,
      int16_t replication_factor,
      bool is_migrated = false)
      : tp_ns(std::move(ns), std::move(topic))
      , partition_count(partition_count)
      , replication_factor(replication_factor)
      , is_migrated(is_migrated) {}

    topic_configuration() = default;

    storage::ntp_config make_ntp_config(
      const ss::sstring&,
      model::partition_id,
      model::revision_id,
      model::initial_revision_id) const;

    bool is_internal() const {
        return tp_ns.ns == model::kafka_internal_namespace
               || tp_ns == model::kafka_consumer_offsets_nt;
    }
    bool is_read_replica() const {
        return properties.read_replica && properties.read_replica.value();
    }
    bool is_recovery_enabled() const {
        return properties.recovery && properties.recovery.value();
    }
    bool has_remote_topic_namespace_override() const {
        return properties.remote_topic_namespace_override.has_value();
    }

    const model::topic_namespace& remote_tp_ns() const {
        if (has_remote_topic_namespace_override()) {
            return properties.remote_topic_namespace_override.value();
        }
        return tp_ns;
    }

    model::topic_namespace tp_ns;
    // using signed integer because Kafka protocol defines it as signed int
    int32_t partition_count;
    // using signed integer because Kafka protocol defines it as signed int
    int16_t replication_factor;
    // bypass migration restrictions
    bool is_migrated;

    topic_properties properties;

    auto serde_fields() {
        return std::tie(
          tp_ns, partition_count, replication_factor, properties, is_migrated);
    }

    void serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;

        tp_ns = read_nested<model::topic_namespace>(in, h._bytes_left_limit);
        partition_count = read_nested<int32_t>(in, h._bytes_left_limit);
        replication_factor = read_nested<int16_t>(in, h._bytes_left_limit);
        properties = read_nested<topic_properties>(in, h._bytes_left_limit);
        if (h._version >= 2) {
            is_migrated = read_nested<bool>(in, h._bytes_left_limit);
        } else {
            is_migrated = false;
        }

        if (h._version < 1) {
            // Legacy tiered storage topics do not delete data on
            // topic deletion.
            properties.remote_delete
              = storage::ntp_config::legacy_remote_delete;
        }
    }

    friend std::ostream& operator<<(std::ostream&, const topic_configuration&);

    friend bool
    operator==(const topic_configuration&, const topic_configuration&)
      = default;
};

using topic_configuration_vector = chunked_vector<topic_configuration>;

} // namespace cluster

namespace reflection {

template<>
struct adl<cluster::topic_configuration> {
    void to(iobuf&, cluster::topic_configuration&&);
    cluster::topic_configuration from(iobuf_parser&);
};

} // namespace reflection
