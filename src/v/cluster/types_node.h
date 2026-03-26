/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/errc.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "serde/rw/chrono.h"
#include "serde/rw/envelope.h"

#include <fmt/format.h>

#include <chrono>
#include <cstdint>

namespace cluster {

struct decommission_node_request
  : serde::envelope<
      decommission_node_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::node_id id;

    friend bool operator==(
      const decommission_node_request&, const decommission_node_request&)
      = default;

    auto serde_fields() { return std::tie(id); }

    friend std::ostream&
    operator<<(std::ostream& o, const decommission_node_request& r) {
        fmt::print(o, "id {}", r.id);
        return o;
    }
};

struct decommission_node_reply
  : serde::envelope<
      decommission_node_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    errc error;

    friend bool
    operator==(const decommission_node_reply&, const decommission_node_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }

    friend std::ostream&
    operator<<(std::ostream& o, const decommission_node_reply& r) {
        fmt::print(o, "error {}", r.error);
        return o;
    }
};

struct recommission_node_request
  : serde::envelope<
      recommission_node_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::node_id id;

    friend bool operator==(
      const recommission_node_request&, const recommission_node_request&)
      = default;

    auto serde_fields() { return std::tie(id); }

    friend std::ostream&
    operator<<(std::ostream& o, const recommission_node_request& r) {
        fmt::print(o, "id {}", r.id);
        return o;
    }
};

struct recommission_node_reply
  : serde::envelope<
      recommission_node_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    errc error;

    friend bool
    operator==(const recommission_node_reply&, const recommission_node_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }

    friend std::ostream&
    operator<<(std::ostream& o, const recommission_node_reply& r) {
        fmt::print(o, "error {}", r.error);
        return o;
    }
};

struct finish_reallocation_request
  : serde::envelope<
      finish_reallocation_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::node_id id;

    friend bool operator==(
      const finish_reallocation_request&, const finish_reallocation_request&)
      = default;

    auto serde_fields() { return std::tie(id); }

    friend std::ostream&
    operator<<(std::ostream& o, const finish_reallocation_request& r) {
        fmt::print(o, "id {}", r.id);
        return o;
    }
};

struct finish_reallocation_reply
  : serde::envelope<
      finish_reallocation_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    errc error;

    friend bool operator==(
      const finish_reallocation_reply&, const finish_reallocation_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }

    friend std::ostream&
    operator<<(std::ostream& o, const finish_reallocation_reply& r) {
        fmt::print(o, "error {}", r.error);
        return o;
    }
};

struct set_maintenance_mode_request
  : serde::envelope<
      set_maintenance_mode_request,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    model::node_id id;
    bool enabled;

    friend bool operator==(
      const set_maintenance_mode_request&, const set_maintenance_mode_request&)
      = default;

    auto serde_fields() { return std::tie(id, enabled); }

    friend std::ostream&
    operator<<(std::ostream& o, const set_maintenance_mode_request& r) {
        fmt::print(o, "id {} enabled {}", r.id, r.enabled);
        return o;
    }
};

struct set_maintenance_mode_reply
  : serde::envelope<
      set_maintenance_mode_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    errc error;

    friend bool operator==(
      const set_maintenance_mode_reply&, const set_maintenance_mode_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }

    friend std::ostream&
    operator<<(std::ostream& o, const set_maintenance_mode_reply& r) {
        fmt::print(o, "error {}", r.error);
        return o;
    }
};

struct hello_request final
  : serde::
      envelope<hello_request, serde::version<0>, serde::compat_version<0>> {
    model::node_id peer;

    // milliseconds since epoch
    std::chrono::milliseconds start_time;

    friend bool operator==(const hello_request&, const hello_request&)
      = default;

    auto serde_fields() { return std::tie(peer, start_time); }

    friend std::ostream& operator<<(std::ostream&, const hello_request&);
};

struct hello_reply
  : serde::envelope<hello_reply, serde::version<0>, serde::compat_version<0>> {
    errc error;

    friend bool operator==(const hello_reply&, const hello_reply&) = default;

    friend std::ostream& operator<<(std::ostream&, const hello_reply&);

    auto serde_fields() { return std::tie(error); }
};

/**
 * Broker state transitions are coordinated centrally as opposite to
 * configuration which change is requested by the described node itself. Broker
 * state represents centrally managed node properties. The difference between
 * broker state and configuration is that the configuration change is made on
 * the node while state changes are managed by the cluster controller.
 */
class broker_state
  : public serde::
      envelope<broker_state, serde::version<1>, serde::compat_version<0>> {
public:
    model::membership_state get_membership_state() const {
        return _membership_state;
    }
    void set_membership_state(model::membership_state st) {
        _membership_state = st;
    }

    model::maintenance_state get_maintenance_state() const {
        return _maintenance_state;
    }
    void set_maintenance_state(model::maintenance_state st) {
        _maintenance_state = st;
    }

    friend bool operator==(const broker_state&, const broker_state&) = default;

    friend std::ostream& operator<<(std::ostream&, const broker_state&);

    auto serde_fields() {
        return std::tie(_membership_state, _maintenance_state);
    }

private:
    model::membership_state _membership_state = model::membership_state::active;
    model::maintenance_state _maintenance_state
      = model::maintenance_state::inactive;
};

/**
 * Node metadata describes a cluster node with its state and configuration
 */
struct node_metadata {
    model::broker broker;
    broker_state state;

    friend bool operator==(const node_metadata&, const node_metadata&)
      = default;
    friend std::ostream& operator<<(std::ostream&, const node_metadata&);
};

// Node update types, used for communication between members_manager and
// members_backend.
//
// NOTE: maintenance mode doesn't interact with the members_backend,
// instead interacting with each core via their respective drain_manager.
enum class node_update_type : int8_t {
    // A node has been added to the cluster.
    added,

    // A node has been decommissioned from the cluster.
    decommissioned,

    // A node has been recommissioned after an incomplete decommission.
    recommissioned,

    // All reallocations associated with a given node update have completed
    // (e.g. it's been fully decommissioned, indicating it can no longer be
    // recommissioned).
    reallocation_finished,

    // node has been removed from the cluster
    removed,

    // previous updates must be interrupted
    interrupted,
};

std::ostream& operator<<(std::ostream&, const node_update_type&);

} // namespace cluster
