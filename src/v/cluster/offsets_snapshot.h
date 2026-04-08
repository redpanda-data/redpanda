/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cluster/errc.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

namespace cluster {

struct group_offsets
  : public serde::
      envelope<group_offsets, serde::version<0>, serde::compat_version<0>> {
    struct partition_offset
      : public serde::envelope<
          partition_offset,
          serde::version<0>,
          serde::compat_version<0>> {
        partition_offset(model::partition_id p, kafka::offset o)
          : partition(p)
          , offset(o) {}
        partition_offset() = default;

        model::partition_id partition;
        kafka::offset offset;

        auto serde_fields() { return std::tie(partition, offset); }
        auto operator<=>(const partition_offset&) const = default;
    };

    struct topic_partitions
      : public serde::envelope<
          topic_partitions,
          serde::version<0>,
          serde::compat_version<0>> {
        topic_partitions(model::topic t, chunked_vector<partition_offset> ps)
          : topic(std::move(t))
          , partitions(std::move(ps)) {}
        topic_partitions() = default;

        model::topic topic;
        chunked_vector<partition_offset> partitions;

        topic_partitions copy() const { return {topic, partitions.copy()}; }

        auto serde_fields() { return std::tie(topic, partitions); }
        friend bool
        operator==(const topic_partitions&, const topic_partitions&) = default;
    };

    // The consumer group ID.
    ss::sstring group_id;

    // Data partitions and their committed offsets.
    chunked_vector<topic_partitions> offsets;

    auto serde_fields() { return std::tie(group_id, offsets); }

    group_offsets copy() const {
        return {
          .group_id = group_id,
          .offsets = {
            std::from_range,
            offsets | std::views::transform(&topic_partitions::copy)}};
    }

    friend bool
    operator==(const group_offsets&, const group_offsets&) = default;
};

struct group_offsets_snapshot
  : public serde::envelope<
      group_offsets_snapshot,
      serde::version<0>,
      serde::compat_version<0>> {
    // Partition ID of the offsets topic that managed these groups.
    model::partition_id offsets_topic_pid;

    // Consumer groups and their offsets.
    chunked_vector<group_offsets> groups;

    group_offsets_snapshot copy() const {
        return {
          .offsets_topic_pid = offsets_topic_pid,
          .groups = {
            std::from_range,
            groups | std::views::transform(&group_offsets::copy)}};
    }

    auto serde_fields() { return std::tie(offsets_topic_pid, groups); }

    friend bool operator==(
      const group_offsets_snapshot&, const group_offsets_snapshot&) = default;
};

struct get_group_offsets_request
  : public serde::envelope<
      get_group_offsets_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using self = get_group_offsets_request;

    model::partition_id co_partition;
    chunked_vector<kafka::group_id> groups;

    get_group_offsets_request() = default;
    get_group_offsets_request(
      model::partition_id co_partition, chunked_vector<kafka::group_id> groups)
      : co_partition(co_partition)
      , groups(std::move(groups)) {}

    /**
     * This type must be copyable to operate with leader_router
     */
    get_group_offsets_request(get_group_offsets_request&&) = default;
    get_group_offsets_request& operator=(get_group_offsets_request&&) = default;
    get_group_offsets_request(const get_group_offsets_request& other)
      : co_partition(other.co_partition)
      , groups(other.groups.copy()) {}
    get_group_offsets_request&
    operator=(const get_group_offsets_request& other) {
        return *this = auto(other);
    };

    auto serde_fields() { return std::tie(co_partition, groups); }
    friend bool operator==(const self&, const self&) = default;
};

struct get_group_offsets_reply
  : public serde::envelope<
      get_group_offsets_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using self = get_group_offsets_reply;

    cluster::errc ec;
    chunked_vector<group_offsets> group_offsets;

    get_group_offsets_reply() = default;
    get_group_offsets_reply(
      cluster::errc ec, chunked_vector<cluster::group_offsets> group_offsets)
      : ec(ec)
      , group_offsets(std::move(group_offsets)) {}

    /**
     * This type must be copyable to operate with leader_router
     */
    get_group_offsets_reply(get_group_offsets_reply&&) = default;
    get_group_offsets_reply& operator=(get_group_offsets_reply&&) = default;
    get_group_offsets_reply(const get_group_offsets_reply& other)
      : ec(other.ec)
      , group_offsets(
          std::from_range,
          other.group_offsets | std::views::transform(&group_offsets::copy)) {}
    get_group_offsets_reply& operator=(const get_group_offsets_reply& other) {
        return *this = auto(other);
    };

    auto serde_fields() { return std::tie(ec, group_offsets); }
    friend bool operator==(const self&, const self&) = default;
};

struct set_group_offsets_request
  : public serde::envelope<
      set_group_offsets_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using self = set_group_offsets_request;

    group_offsets_snapshot group_offsets;

    set_group_offsets_request() = default;
    explicit set_group_offsets_request(group_offsets_snapshot group_offsets)
      : group_offsets(std::move(group_offsets)) {}

    /**
     * This type must be copyable to operate with leader_router
     */
    set_group_offsets_request(set_group_offsets_request&&) = default;
    set_group_offsets_request& operator=(set_group_offsets_request&&) = default;
    set_group_offsets_request(const set_group_offsets_request& other)
      : group_offsets(other.group_offsets.copy()) {}
    set_group_offsets_request&
    operator=(const set_group_offsets_request& other) {
        return *this = auto(other);
    };

    auto serde_fields() { return std::tie(group_offsets); }
    friend bool operator==(const self&, const self&) = default;
};

struct set_group_offsets_reply
  : public serde::envelope<
      set_group_offsets_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using self = set_group_offsets_reply;

    cluster::errc ec;

    auto serde_fields() { return std::tie(ec); }
    friend bool operator==(const self&, const self&) = default;
};

} // namespace cluster
