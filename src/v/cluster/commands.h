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
#include "bytes/iobuf_parser.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "model/record.h"
#include "reflection/adl.h"
#include "reflection/async_adl.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "serde/serde.h"
#include "utils/named_type.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/when_all.hh>

namespace cluster {

using command_type = named_type<int8_t, struct command_type_tag>;

enum class serde_opts {
    // The command type requires both ADL and serde serialization.
    adl_and_serde = 0,

    // The command type only supports serde, e.g. because it will only be used
    // when all nodes are on a Redpanda version that supports serde.
    serde_only = 1,
};

// Controller state updates are represented in terms of commands. Each
// command represent different type of action that is going to be executed
// over state when applying replicated entry. The controller_command is a
// base type that underlies all commands used in the controller the
// type provides information required for dispatching updates in
// `raft::mux_state_machine` and automatic serialization/deserialization
// handling.
//
// Generic controller command, this type is base for all commands
//
// NOTE: some types are required to compile ADL for compatibility with data
// from older versions of Redpanda that only supported ADL. New types should be
// serde-only, and should not be sent to older versions of Redpanda (e.g. by
// gating with the feature manager).
template<
  typename K,
  typename V,
  int8_t tp,
  model::record_batch_type bt,
  serde_opts so = serde_opts::serde_only>
struct controller_command {
    static_assert(
      tp >= 0,
      "Command type must be greater than 0. Negative values are used to "
      "determine serialization type");

    using key_t = K;
    using value_t = V;

    static constexpr command_type type{tp};
    static constexpr model::record_batch_type batch_type{bt};
    static constexpr serde_opts serde_opts{so};

    controller_command(K k, V v)
      : key(std::move(k))
      , value(std::move(v)) {}

    key_t key; // we use key to leverage kafka key based compaction
    value_t value;
};

// topic commands
static constexpr int8_t create_topic_cmd_type = 0;
static constexpr int8_t delete_topic_cmd_type = 1;
static constexpr int8_t move_partition_replicas_cmd_type = 2;
static constexpr int8_t finish_moving_partition_replicas_cmd_type = 3;
static constexpr int8_t update_topic_properties_cmd_type = 4;
static constexpr int8_t create_partition_cmd_type = 5;
static constexpr int8_t create_non_replicable_topic_cmd_type = 6;
static constexpr int8_t cancel_moving_partition_replicas_cmd_type = 7;
static constexpr int8_t move_topic_replicas_cmd_type = 8;
static constexpr int8_t revert_cancel_partition_move_cmd_type = 9;

static constexpr int8_t create_user_cmd_type = 5;
static constexpr int8_t delete_user_cmd_type = 6;
static constexpr int8_t update_user_cmd_type = 7;
static constexpr int8_t create_acls_cmd_type = 8;
static constexpr int8_t delete_acls_cmd_type = 9;

// data policy commands
static constexpr int8_t create_data_policy_cmd_type = 0;
static constexpr int8_t delete_data_policy_cmd_type = 1;

// node management commands
static constexpr int8_t decommission_node_cmd_type = 0;
static constexpr int8_t recommission_node_cmd_type = 1;
static constexpr int8_t finish_reallocations_cmd_type = 2;
static constexpr int8_t maintenance_mode_cmd_type = 3;
static constexpr int8_t register_node_uuid_cmd_type = 4;
static constexpr int8_t add_node_cmd_type = 5;
static constexpr int8_t update_node_cmd_type = 6;
static constexpr int8_t remove_node_cmd_type = 7;

// cluster config commands
static constexpr int8_t cluster_config_delta_cmd_type = 0;
static constexpr int8_t cluster_config_status_cmd_type = 1;

// feature_manager command types
static constexpr int8_t feature_update_cmd_type = 0;
static constexpr int8_t feature_update_license_update_cmd_type = 1;

// cluster bootstrap commands
static constexpr int8_t bootstrap_cluster_cmd_type = 0;

using create_topic_cmd = controller_command<
  model::topic_namespace,
  topic_configuration_assignment,
  create_topic_cmd_type,
  model::record_batch_type::topic_management_cmd,
  serde_opts::adl_and_serde>;

using delete_topic_cmd = controller_command<
  model::topic_namespace,
  model::topic_namespace,
  delete_topic_cmd_type,
  model::record_batch_type::topic_management_cmd,
  serde_opts::adl_and_serde>;

using move_partition_replicas_cmd = controller_command<
  model::ntp,
  std::vector<model::broker_shard>,
  move_partition_replicas_cmd_type,
  model::record_batch_type::topic_management_cmd,
  serde_opts::adl_and_serde>;

using move_topic_replicas_cmd = controller_command<
  model::topic_namespace,
  std::vector<move_topic_replicas_data>,
  move_topic_replicas_cmd_type,
  model::record_batch_type::topic_management_cmd>;

using finish_moving_partition_replicas_cmd = controller_command<
  model::ntp,
  std::vector<model::broker_shard>,
  finish_moving_partition_replicas_cmd_type,
  model::record_batch_type::topic_management_cmd,
  serde_opts::adl_and_serde>;

using update_topic_properties_cmd = controller_command<
  model::topic_namespace,
  incremental_topic_updates,
  update_topic_properties_cmd_type,
  model::record_batch_type::topic_management_cmd,
  serde_opts::adl_and_serde>;

using create_partition_cmd = controller_command<
  model::topic_namespace,
  create_partitions_configuration_assignment,
  create_partition_cmd_type,
  model::record_batch_type::topic_management_cmd,
  serde_opts::adl_and_serde>;

using create_non_replicable_topic_cmd = controller_command<
  non_replicable_topic,
  int8_t, // unused
  create_non_replicable_topic_cmd_type,
  model::record_batch_type::topic_management_cmd,
  serde_opts::adl_and_serde>;

using cancel_moving_partition_replicas_cmd = controller_command<
  model::ntp,
  cancel_moving_partition_replicas_cmd_data,
  cancel_moving_partition_replicas_cmd_type,
  model::record_batch_type::topic_management_cmd,
  serde_opts::adl_and_serde>;

using revert_cancel_partition_move_cmd = controller_command<
  int8_t, // unused
  revert_cancel_partition_move_cmd_data,
  revert_cancel_partition_move_cmd_type,
  model::record_batch_type::topic_management_cmd,
  serde_opts::serde_only>;

using create_user_cmd = controller_command<
  security::credential_user,
  security::scram_credential,
  create_user_cmd_type,
  model::record_batch_type::user_management_cmd,
  serde_opts::adl_and_serde>;

using delete_user_cmd = controller_command<
  security::credential_user,
  int8_t, // unused
  delete_user_cmd_type,
  model::record_batch_type::user_management_cmd,
  serde_opts::adl_and_serde>;

using update_user_cmd = controller_command<
  security::credential_user,
  security::scram_credential,
  update_user_cmd_type,
  model::record_batch_type::user_management_cmd,
  serde_opts::adl_and_serde>;

using create_acls_cmd = controller_command<
  create_acls_cmd_data,
  int8_t, // unused
  create_acls_cmd_type,
  model::record_batch_type::acl_management_cmd,
  serde_opts::adl_and_serde>;

using delete_acls_cmd = controller_command<
  delete_acls_cmd_data,
  int8_t, // unused
  delete_acls_cmd_type,
  model::record_batch_type::acl_management_cmd,
  serde_opts::adl_and_serde>;

using create_data_policy_cmd = controller_command<
  model::topic_namespace,
  create_data_policy_cmd_data,
  create_data_policy_cmd_type,
  model::record_batch_type::data_policy_management_cmd,
  serde_opts::adl_and_serde>;

using delete_data_policy_cmd = controller_command<
  model::topic_namespace,
  std::optional<ss::sstring>,
  delete_data_policy_cmd_type,
  model::record_batch_type::data_policy_management_cmd,
  serde_opts::adl_and_serde>;

using decommission_node_cmd = controller_command<
  model::node_id,
  int8_t, // unused
  decommission_node_cmd_type,
  model::record_batch_type::node_management_cmd,
  serde_opts::adl_and_serde>;
using recommission_node_cmd = controller_command<
  model::node_id,
  int8_t, // unused
  recommission_node_cmd_type,
  model::record_batch_type::node_management_cmd,
  serde_opts::adl_and_serde>;
using finish_reallocations_cmd = controller_command<
  model::node_id,
  int8_t, // unused
  finish_reallocations_cmd_type,
  model::record_batch_type::node_management_cmd,
  serde_opts::adl_and_serde>;
using register_node_uuid_cmd = controller_command<
  model::node_uuid,
  std::optional<model::node_id>,
  register_node_uuid_cmd_type,
  model::record_batch_type::node_management_cmd>;

using maintenance_mode_cmd = controller_command<
  model::node_id,
  bool, // enabled or disabled
  maintenance_mode_cmd_type,
  model::record_batch_type::node_management_cmd,
  serde_opts::adl_and_serde>;

using add_node_cmd = controller_command<
  int8_t, // unused
  model::broker,
  add_node_cmd_type,
  model::record_batch_type::node_management_cmd>;

using update_node_cfg_cmd = controller_command<
  int8_t, // unused
  model::broker,
  update_node_cmd_type,
  model::record_batch_type::node_management_cmd>;

using remove_node_cmd = controller_command<
  model::node_id,
  int8_t, // unused,
  remove_node_cmd_type,
  model::record_batch_type::node_management_cmd>;

// Cluster configuration deltas
using cluster_config_delta_cmd = controller_command<
  config_version,
  cluster_config_delta_cmd_data,
  cluster_config_delta_cmd_type,
  model::record_batch_type::cluster_config_cmd,
  serde_opts::adl_and_serde>;

using cluster_config_status_cmd = controller_command<
  model::node_id,
  cluster_config_status_cmd_data,
  cluster_config_status_cmd_type,
  model::record_batch_type::cluster_config_cmd,
  serde_opts::adl_and_serde>;

using feature_update_cmd = controller_command<
  feature_update_cmd_data,
  int8_t, // unused
  feature_update_cmd_type,
  model::record_batch_type::feature_update,
  serde_opts::adl_and_serde>;

using feature_update_license_update_cmd = controller_command<
  feature_update_license_update_cmd_data,
  int8_t, // unused
  feature_update_license_update_cmd_type,
  model::record_batch_type::feature_update,
  serde_opts::serde_only>;

// Cluster bootstrap
using bootstrap_cluster_cmd = controller_command<
  int8_t, // unused, always 0
  bootstrap_cluster_cmd_data,
  bootstrap_cluster_cmd_type,
  model::record_batch_type::cluster_bootstrap_cmd>;

// typelist utils
template<typename T>
concept ControllerCommand = requires(T cmd) {
    typename T::key_t;
    typename T::value_t;
    { cmd.key } -> std::convertible_to<const typename T::key_t&>;
    { cmd.value } -> std::convertible_to<const typename T::value_t&>;
    { T::type } -> std::convertible_to<const command_type&>;
    { T::batch_type } -> std::convertible_to<const model::record_batch_type&>;
};

template<typename... Commands>
struct commands_type_list {};

template<typename... Commands>
requires(
  ControllerCommand<Commands>,
  ...) using make_commands_list = commands_type_list<Commands...>;

/// Commands are serialized as a batch with single record. Command key is
/// serialized as a record key. Key is independent from command type so it can
/// leverage the log compactions (i.e only last command for given key is enough
/// to determine its state). Command value contains command type information.
/// Record data contains first the command type and then value.
///
///                  +--------------+-------+
///                  | command_type | value |
///                  +--------------+-------+
///
template<typename Cmd>
requires ControllerCommand<Cmd> ss::future<model::record_batch>
serialize_cmd(Cmd cmd) {
    return ss::do_with(
      iobuf{},
      iobuf{},
      [cmd = std::move(cmd)](iobuf& key_buf, iobuf& value_buf) mutable {
          auto value_f
            = reflection::async_adl<command_type>{}
                .to(value_buf, Cmd::type)
                .then([&value_buf, v = std::move(cmd.value)]() mutable {
                    return reflection::adl<typename Cmd::value_t>{}.to(
                      value_buf, std::move(v));
                });
          auto key_f = reflection::async_adl<typename Cmd::key_t>{}.to(
            key_buf, std::move(cmd.key));
          return ss::when_all_succeed(std::move(key_f), std::move(value_f))
            .discard_result()
            .then([&key_buf, &value_buf]() mutable {
                simple_batch_builder builder(Cmd::batch_type, model::offset(0));
                builder.add_raw_kv(std::move(key_buf), std::move(value_buf));
                return std::move(builder).build();
            });
      });
}
// flag indicating tha the command was serialized using serde serialization
static constexpr int8_t serde_serialized_cmd_flag = -1;

template<typename Cmd>
requires ControllerCommand<Cmd> model::record_batch
serde_serialize_cmd(Cmd cmd) {
    iobuf key_buf;
    iobuf value_buf;
    /**
     * We leverage the fact that all reflection::adl serialized commands have
     * command type that is >= 0. To indicate the new format we will use single
     * byte set to -1. This will allow choosing appropriate deserializer.
     */
    using serde::write;
    write<int8_t>(value_buf, serde_serialized_cmd_flag);
    write<int8_t>(value_buf, Cmd::type);

    write(value_buf, std::move(cmd.value));
    write(key_buf, std::move(cmd.key));

    simple_batch_builder builder(Cmd::batch_type, model::offset(0));
    builder.add_raw_kv(std::move(key_buf), std::move(value_buf));
    return std::move(builder).build();
}

namespace internal {
template<typename Cmd>
struct deserializer {
    using key_t = typename Cmd::key_t;
    using value_t = typename Cmd::value_t;
    ss::future<Cmd>
    deserialize(iobuf_parser k_parser, iobuf_parser v_parser, bool use_serde) {
        if constexpr (Cmd::serde_opts == serde_opts::adl_and_serde) {
            if (unlikely(!use_serde)) {
                auto results = co_await ss::when_all_succeed(
                  reflection::async_adl<key_t>{}.from(k_parser),
                  reflection::async_adl<value_t>{}.from(v_parser));
                co_return Cmd(std::get<0>(results), std::get<1>(results));
            }
        }
        vassert(use_serde, "Requested to ADL serialize a serde-only type");
        co_return serde_deserialize(std::move(k_parser), std::move(v_parser));
    }

    Cmd serde_deserialize(iobuf_parser k_parser, iobuf_parser v_parser) {
        using serde::read;

        return Cmd(read<key_t>(k_parser), read<value_t>(v_parser));
    }
};

template<typename Cmd>
requires ControllerCommand<Cmd> std::optional<deserializer<Cmd>>
make_deserializer(command_type tp) {
    if (tp != Cmd::type) {
        return std::nullopt;
    }
    return deserializer<Cmd>{};
}
} // namespace internal

template<typename... Commands>
ss::future<std::variant<Commands...>>
deserialize(model::record_batch b, commands_type_list<Commands...>) {
    vassert(
      b.record_count() == 1,
      "Currently we expect single command in single batch");
    auto records = b.copy_records();
    iobuf_parser v_parser(records.begin()->release_value());
    iobuf_parser k_parser(records.begin()->release_key());
    const bool use_serde_serialization = reflection::adl<int8_t>{}.from(
                                           v_parser.peek(1))
                                         == serde_serialized_cmd_flag;
    if (likely(use_serde_serialization)) {
        // skip over indicator field
        v_parser.skip(1);
    }

    // chose deserializer
    auto cmd_type = reflection::adl<command_type>{}.from(v_parser);
    std::optional<std::variant<internal::deserializer<Commands>...>> ret;
    (void)((ret = internal::make_deserializer<Commands>(cmd_type), ret) || ...);

    return std::visit(
      [k_parser = std::move(k_parser),
       v_parser = std::move(v_parser),
       use_serde_serialization](auto& d) mutable {
          return d
            .deserialize(
              std::move(k_parser), std::move(v_parser), use_serde_serialization)
            .then([](auto cmd) {
                return std::variant<Commands...>(std::move(cmd));
            });
      },
      *ret);
}
} // namespace cluster
