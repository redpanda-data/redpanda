/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "backend_decl.h"
#include "base/vassert.h"
#include "cluster/logger.h"
#include "cluster/sloth_mail/impl/types.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

#include <utility>

namespace cluster::sloth_mail::impl {
template<mail_config Config>
backend<Config>::backend(
  Config config,
  model::node_id self,
  members_table& members,
  size_t max_buffered_bytes_per_node,
  deadline_t::duration retry_in)
  : _config(config)
  , _self(self)
  , _members(members)
  , _max_buffered_bytes_per_node(max_buffered_bytes_per_node)
  , _retry_in(retry_in) {}

template<mail_config Config>
backend<Config>::~backend() noexcept {
    vassert(
      _gate.is_closed(),
      "SlothMail backend destroyed without stop() being called");
}

template<mail_config Config>
void backend<Config>::start() {
    _members_update_notification_id
      = _members.register_members_updated_notification(
        [this](model::node_id node_id, model::membership_state node_state) {
            if (node_state == model::membership_state::removed) {
                // node gone, bin its mail
                _destination_states.erase(node_id);
                vlog(
                  clusterlog.debug,
                  "SlothMail removed state for node {}",
                  node_id);
            }
        });
}

template<mail_config Config>
ss::future<> backend<Config>::stop() {
    vlog(clusterlog.debug, "SlothMail stopping backend");
    _destination_states.clear();
    _members.unregister_members_updated_notification(
      _members_update_notification_id);
    return _gate.close();
}

template<mail_config Config>
backend<Config>::destination_state::destination_state(
  backend& parent, model::node_id destination) {
    timer.set_callback(
      [&parent, destination] { parent.ship_queued(destination); });
};

template<mail_config Config>
backend<Config>::destination_state::~destination_state() {
    if (in_flight_fiber_as) {
        in_flight_fiber_as->request_abort();
    }
};

template<mail_config Config>
template<mail_kind Kind>
void backend<Config>::dispatch(
  model::node_id destination,
  Kind::key_t&& key,
  Kind::value_t&& value,
  deadline_t deadline) {
    if (!_members.contains(destination)) {
        vlog(
          clusterlog.debug,
          "SlothMail dropped mail for unknown node {}",
          destination);
        // no such member, drop the mail
        return;
    }

    auto [st_it, st_ins] = _destination_states.try_emplace(
      destination, *this, destination);
    auto& dest_state = st_it->second;

    using tagged_key_t = typename types::template tagged_key_of_kind<Kind>;
    using any_tagged_key_t = typename types::kv_map_t::key_type;
    any_tagged_key_t wrapped_key{
      std::in_place_type<tagged_key_t>, std::move(key)};
    auto [it, inserted] = dest_state.queue.try_emplace(
      std::move(wrapped_key),
      std::in_place_type<typename Kind::value_t>,
      std::move(value));
    if (inserted) {
        dest_state.cumulative_size_bytes += Kind::entry_size;
    } else {
        // use-after-move is fine, as try_emplace didn't move if not inserted
        Kind::merge(
          std::get<tagged_key_t>(wrapped_key).key,
          std::get<typename Kind::value_t>(it->second),
          value);
    }
    dest_state.deadline = std::min(dest_state.deadline, deadline);
    ship_or_rearm_if_needed(*st_it);
}

template<mail_config Config>
void backend<Config>::force_generate_dispatch_for_all_kinds() {
    ([this]<typename... Kind>(std::type_identity<std::variant<Kind...>>) {
        (this->dispatch<Kind>(
           model::node_id{},
           typename Kind::key_t{},
           typename Kind::value_t{},
           deadline_t::clock::now()),
         ...);
    })(std::type_identity<typename Config::supported_kinds>{});
}

template<mail_config Config>
void backend<Config>::ship_or_rearm_if_needed(
  destination_map_t::value_type& state) {
    auto& dest_state = state.second;
    auto now = deadline_t::clock::now();
    bool needs_shipping = dest_state.cumulative_size_bytes
                            >= _max_buffered_bytes_per_node
                          || dest_state.deadline <= now;
    vlog(
      clusterlog.trace,
      "SlothMail limits for node {}. Bytes current={} max={}, deadline in {}, "
      "needs_shipping={}",
      state.first,
      dest_state.cumulative_size_bytes,
      _max_buffered_bytes_per_node,
      dest_state.deadline - now,
      needs_shipping);
    if (needs_shipping) {
        ship_queued(state);
    } else {
        vlog(
          clusterlog.trace,
          "SlothMail rearming timer for node {} at {}",
          state.first,
          dest_state.deadline - now);
        dest_state.timer.rearm(dest_state.deadline);
    }
}

template<mail_config Config>
void backend<Config>::ship_queued(model::node_id destination) {
    auto it = _destination_states.find(destination);
    if (it == _destination_states.end()) {
        vlog(
          clusterlog.debug,
          "SlothMail not sending to node {}, as it is no longer a member",
          destination);
        return;
    }
    ship_queued(*it);
}

template<mail_config Config>
void backend<Config>::ship_queued(destination_map_t::value_type& dest_state) {
    if (dest_state.second.in_flight_fiber_as) {
        vlog(
          clusterlog.debug,
          "SlothMail not spawning another sending fiber for node {}",
          dest_state.first);
        return;
    }
    vlog(
      clusterlog.trace,
      "SlothMail shipping {} bytes to node {}",
      dest_state.second.cumulative_size_bytes,
      dest_state.first);
    auto batch = std::move(dest_state.second.queue);
    dest_state.second.cumulative_size_bytes = 0;
    dest_state.second.deadline = deadline_t::max();
    dest_state.second.timer.cancel();
    dest_state.second.in_flight_fiber_as
      = ss::make_lw_shared<ss::abort_source>();

    ssx::spawn_with_gate(
      _gate,
      [this,
       destination = dest_state.first,
       batch = std::move(batch),
       as_lwptr = dest_state.second.in_flight_fiber_as]() mutable {
          return ship_batch(destination, std::move(batch), std::move(as_lwptr));
      });
}

template<mail_config Config>
ss::future<> backend<Config>::ship_batch(
  model::node_id destination,
  types::kv_map_t batch,
  ss::lw_shared_ptr<ss::abort_source> as) {
    auto request = build_request(batch);
    while (!as->abort_requested() && !_gate.is_closed()) {
        auto reply = co_await _config.do_ship_mail(destination, request.copy());
        vlog(
          clusterlog.trace,
          "SlothMail shipped to node {} with ec {}",
          destination,
          reply.ec);
        if (reply.ec == errc::success) {
            if (as->abort_requested()) {
                // node has been removed while we were waiting for reply
                co_return;
            }
            auto it = _destination_states.find(destination);
            vassert(
              it != _destination_states.end(), "no state for {}", destination);
            it->second.in_flight_fiber_as = nullptr;
            if (!it->second.queue.empty()) {
                vlog(
                  clusterlog.debug,
                  "SlothMail rearming timer for node {} at {}",
                  destination,
                  it->second.deadline - deadline_t::clock::now());
                it->second.timer.rearm(it->second.deadline);
            }
            co_return;
        }
        if (as->abort_requested()) {
            // node has been removed while we were waiting for reply
            co_return;
        }
        vlog(
          clusterlog.debug,
          "SlothMail not delivered to node {}: {}; will retry in {}",
          destination,
          reply.ec,
          _retry_in);
        co_await ss::sleep_abortable(_retry_in, *as);
    }
}

template<mail_config Config>
backend<Config>::types::mail_request
backend<Config>::build_request(const types::kv_map_t& batch) {
    typename types::mail_request request;
    // change to ssx::async_transform if _max_buffered_bytes_per_node set high
    for (auto&& [tagged_key, value] : batch) {
        std::visit(
          [&request, value = std::move(value)](auto&& tagged_key) {
              using tagged_key_t = std::decay_t<decltype(tagged_key)>;
              using vector_t = tagged_key_t::vector_of_pairs_t;
              using value_t = tagged_key_t::kind::value_t;
              constexpr auto kind_id = tagged_key_t::kind::id;
              auto [it, ins] = request.data.try_emplace(
                kind_id, std::in_place_type<vector_t>);
              std::get<vector_t>(it->second)
                .push_back(
                  {.key = std::forward<decltype(tagged_key)>(tagged_key).key,
                   .value = std::move(std::get<value_t>(value))});
          },
          std::move(tagged_key));
    }
    return request;
}

} // namespace cluster::sloth_mail::impl
