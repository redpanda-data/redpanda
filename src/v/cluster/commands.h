/*
 * Copyright 2020 Vectorized, Inc.
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
#include "utils/named_type.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>

namespace cluster {

// cluster batch types
// we do not have to add it to well_known_batch_types as we will never use it to
// filter the batches out
static constexpr model::record_batch_type topic_batch_type
  = model::record_batch_type(6);

using command_type = named_type<int8_t, struct command_type_tag>;
// Controller state updates are represented in terms of commands. Each
// command represent different type of action that is going to be executed
// over state when applying replicated entry. The controller_command is a
// base type that underlies all commands used in the controller the
// type provides information required for dispatching updates in
// `raft::mux_state_machine` and automatic serialization/deserialization
// handling.
//
// Generic controller command, this type is base for all commands
template<typename K, typename V, int8_t tp, model::record_batch_type::type bt>
struct controller_command {
    using key_t = K;
    using value_t = V;

    static constexpr command_type type{tp};
    static constexpr model::record_batch_type batch_type{bt};

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

using create_topic_cmd = controller_command<
  model::topic_namespace,
  topic_configuration_assignment,
  create_topic_cmd_type,
  topic_batch_type()>;

using delete_topic_cmd = controller_command<
  model::topic_namespace,
  model::topic_namespace,
  delete_topic_cmd_type,
  topic_batch_type()>;

using move_partition_replicas_cmd = controller_command<
  model::ntp,
  std::vector<model::broker_shard>,
  move_partition_replicas_cmd_type,
  topic_batch_type()>;

// typelist utils
// clang-format off
CONCEPT(
template<typename T>
concept ControllerCommand = requires (T cmd) {
    typename T::key_t;
    typename T::value_t;
    { cmd.key } -> std::convertible_to<const typename T::key_t&>;
    { cmd.value } -> std::convertible_to<const typename T::value_t&>;
    { T::type } -> std::convertible_to<const command_type&>;
    { T::batch_type } -> std::convertible_to<const model::record_batch_type&>;
};
)
// clang-format on

template<typename... Commands>
struct commands_type_list {};

template<typename... Commands>
CONCEPT(requires(ControllerCommand<Commands>, ...))
using make_commands_list = commands_type_list<Commands...>;

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
CONCEPT(requires ControllerCommand<Cmd>)
ss::future<model::record_batch> serialize_cmd(Cmd cmd) {
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

namespace internal {
template<typename Cmd>
struct deserializer {
    ss::future<Cmd> deserialize(iobuf_parser k_parser, iobuf_parser v_parser) {
        using key_t = typename Cmd::key_t;
        using value_t = typename Cmd::value_t;
        return ss::do_with(
          std::move(k_parser),
          std::move(v_parser),
          [](iobuf_parser& k_parser, iobuf_parser& v_parser) {
              return ss::when_all(
                       reflection::async_adl<key_t>{}.from(k_parser),
                       reflection::async_adl<value_t>{}.from(v_parser))
                .then(
                  [](std::tuple<ss::future<key_t>, ss::future<value_t>> res) {
                      // futures are already completed here
                      return Cmd(
                        std::get<0>(res).get0(), std::get<1>(res).get0());
                  });
          });
    }
};

template<typename Cmd>
CONCEPT(requires ControllerCommand<Cmd>)
std::optional<deserializer<Cmd>> make_deserializer(command_type tp) {
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
    // chose deserializer
    auto cmd_type = reflection::adl<command_type>{}.from(v_parser);
    std::optional<std::variant<internal::deserializer<Commands>...>> ret;
    (void)((ret = internal::make_deserializer<Commands>(cmd_type), ret) || ...);

    return std::visit(
      [k_parser = std::move(k_parser),
       v_parser = std::move(v_parser)](auto& d) mutable {
          return d.deserialize(std::move(k_parser), std::move(v_parser))
            .then([](auto cmd) {
                return std::variant<Commands...>(std::move(cmd));
            });
      },
      *ret);
}
} // namespace cluster
