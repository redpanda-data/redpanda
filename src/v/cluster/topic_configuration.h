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
#include "serde/rw/envelope.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
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
    int32_t partition_count{0};
    // using signed integer because Kafka protocol defines it as signed int
    int16_t replication_factor{0};
    // bypass migration restrictions
    bool is_migrated{false};

    topic_properties properties;

    void serde_write(iobuf& out);
    void serde_read(iobuf_parser& in, const serde::header& h);

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
