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

#include "cluster/cluster_link/errc.h"
#include "cluster/errc.h"
#include "cluster_link/model/types.h"
#include "model/timeout_clock.h"
#include "serde/rw/envelope.h"

#include <chrono>

namespace cluster {

struct upsert_cluster_link_request
  : serde::envelope<
      upsert_cluster_link_request,
      serde::version<0>,
      serde::compat_version<0>> {
    ::cluster_link::model::metadata metadata;
    model::timeout_clock::duration timeout{};

    friend bool operator==(
      const upsert_cluster_link_request&, const upsert_cluster_link_request&)
      = default;
    auto serde_fields() { return std::tie(metadata, timeout); }
};

struct upsert_cluster_link_response
  : serde::envelope<
      upsert_cluster_link_response,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster::cluster_link::errc ec;

    friend bool operator==(
      const upsert_cluster_link_response&, const upsert_cluster_link_response&)
      = default;
    auto serde_fields() { return std::tie(ec); }
};

struct remove_cluster_link_request
  : serde::envelope<
      remove_cluster_link_request,
      serde::version<0>,
      serde::compat_version<0>> {
    ::cluster_link::model::delete_shadow_link_cmd cmd;
    model::timeout_clock::duration timeout{};

    friend bool operator==(
      const remove_cluster_link_request&, const remove_cluster_link_request&)
      = default;

    auto serde_fields() { return std::tie(cmd, timeout); }
};

struct remove_cluster_link_response
  : serde::envelope<
      remove_cluster_link_response,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster::cluster_link::errc ec;

    friend bool operator==(
      const remove_cluster_link_response&, const remove_cluster_link_response&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

struct add_mirror_topic_request
  : serde::envelope<
      add_mirror_topic_request,
      serde::version<0>,
      serde::compat_version<0>> {
    ::cluster_link::model::id_t link_id;
    ::cluster_link::model::add_mirror_topic_cmd cmd;
    model::timeout_clock::duration timeout{};

    friend bool
    operator==(const add_mirror_topic_request&, const add_mirror_topic_request&)
      = default;

    auto serde_fields() { return std::tie(link_id, cmd, timeout); }
};

struct add_mirror_topic_response
  : serde::envelope<
      add_mirror_topic_response,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster_link::errc ec{cluster_link::errc::success};

    friend bool operator==(
      const add_mirror_topic_response&, const add_mirror_topic_response&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

struct update_mirror_topic_status_request
  : serde::envelope<
      update_mirror_topic_status_request,
      serde::version<0>,
      serde::compat_version<0>> {
    ::cluster_link::model::id_t link_id;
    ::cluster_link::model::update_mirror_topic_status_cmd cmd;
    model::timeout_clock::duration timeout{};

    friend bool operator==(
      const update_mirror_topic_status_request&,
      const update_mirror_topic_status_request&)
      = default;

    auto serde_fields() { return std::tie(link_id, cmd, timeout); }
};

struct update_mirror_topic_status_response
  : serde::envelope<
      update_mirror_topic_status_response,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster_link::errc ec{cluster_link::errc::success};

    friend bool operator==(
      const update_mirror_topic_status_response&,
      const update_mirror_topic_status_response&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

struct update_mirror_topic_properties_request
  : serde::envelope<
      update_mirror_topic_properties_request,
      serde::version<0>,
      serde::compat_version<0>> {
    ::cluster_link::model::id_t link_id;
    ::cluster_link::model::update_mirror_topic_properties_cmd cmd;
    model::timeout_clock::duration timeout{};

    friend bool operator==(
      const update_mirror_topic_properties_request&,
      const update_mirror_topic_properties_request&)
      = default;

    auto serde_fields() { return std::tie(link_id, cmd, timeout); }
};

struct update_mirror_topic_properties_response
  : serde::envelope<
      update_mirror_topic_properties_response,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster_link::errc ec{cluster_link::errc::success};

    friend bool operator==(
      const update_mirror_topic_properties_response&,
      const update_mirror_topic_properties_response&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

struct update_cluster_link_configuration_request
  : serde::envelope<
      update_cluster_link_configuration_request,
      serde::version<0>,
      serde::compat_version<0>> {
    ::cluster_link::model::id_t link_id;
    ::cluster_link::model::update_cluster_link_configuration_cmd cmd;
    model::timeout_clock::duration timeout{};

    friend bool operator==(
      const update_cluster_link_configuration_request&,
      const update_cluster_link_configuration_request&)
      = default;

    auto serde_fields() { return std::tie(link_id, cmd, timeout); }
};

struct update_cluster_link_configuration_response
  : serde::envelope<
      update_cluster_link_configuration_response,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster_link::errc ec{cluster_link::errc::success};

    friend bool operator==(
      const update_cluster_link_configuration_response&,
      const update_cluster_link_configuration_response&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

struct delete_mirror_topic_request
  : serde::envelope<
      delete_mirror_topic_request,
      serde::version<0>,
      serde::compat_version<0>> {
    ::cluster_link::model::id_t link_id;
    ::cluster_link::model::delete_mirror_topic_cmd cmd;
    model::timeout_clock::duration timeout{};

    friend bool operator==(
      const delete_mirror_topic_request&, const delete_mirror_topic_request&)
      = default;

    auto serde_fields() { return std::tie(link_id, cmd, timeout); }
};

struct delete_mirror_topic_response
  : serde::envelope<
      delete_mirror_topic_response,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster_link::errc ec{cluster_link::errc::success};

    friend bool operator==(
      const delete_mirror_topic_response&, const delete_mirror_topic_response&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

// Request to get the current cluster epoch.
struct get_current_cluster_epoch_request
  : serde::envelope<
      get_current_cluster_epoch_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::timeout_clock::duration timeout{};

    friend bool operator==(
      const get_current_cluster_epoch_request&,
      const get_current_cluster_epoch_request&)
      = default;

    auto serde_fields() { return std::tie(timeout); }
};

// The reply to the get_current_cluster_epoch_request.
struct get_current_cluster_epoch_response
  : serde::envelope<
      get_current_cluster_epoch_response,
      serde::version<0>,
      serde::compat_version<0>> {
    errc ec{errc::success};
    int64_t epoch{-1};

    friend bool operator==(
      const get_current_cluster_epoch_response&,
      const get_current_cluster_epoch_response&)
      = default;

    auto serde_fields() { return std::tie(ec, epoch); }
};

} // namespace cluster
