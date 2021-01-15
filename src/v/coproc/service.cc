// Copyright 2020 Vectorized, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md

#include "service.h"

#include "coproc/logger.h"
#include "coproc/types.h"
#include "model/namespace.h"
#include "ssx/future-util.h"
#include "utils/functional.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/array_map.hh>
#include <seastar/core/loop.hh>

#include <boost/range/irange.hpp>

#include <iterator>

namespace coproc {

service::service(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<pacemaker>& pacemaker)
  : script_manager_service(sg, ssg)
  , _pacemaker(pacemaker) {}

bool contains_code(const std::vector<errc>& codes, errc code) {
    return std::any_of(codes.cbegin(), codes.cend(), xform::equal_to(code));
}

bool contains_all_codes(const std::vector<errc>& codes, errc code) {
    return std::all_of(codes.cbegin(), codes.cend(), xform::equal_to(code));
}

enable_response_code fold_enable_acks(const std::vector<errc>& codes) {
    /// If at least one shard returned with error, the effect of registering has
    /// produced undefined behavior, so returns 'internal_error'
    if (contains_code(codes, errc::internal_error)) {
        return enable_response_code::internal_error;
    }
    /// For identifiable errors, all shards should have agreed on the error
    if (contains_all_codes(codes, errc::topic_does_not_exist)) {
        return enable_response_code::topic_does_not_exist;
    }
    if (contains_all_codes(codes, errc::invalid_ingestion_policy)) {
        return enable_response_code::invalid_ingestion_policy;
    }
    if (contains_all_codes(codes, errc::materialized_topic)) {
        return enable_response_code::materialized_topic;
    }
    if (contains_all_codes(codes, errc::invalid_topic)) {
        return enable_response_code::invalid_topic;
    }
    /// The only other 'normal' circumstance is some shards reporting 'success'
    /// and others reporting 'topic_does_not_exist'
    vassert(
      std::all_of(
        codes.cbegin(),
        codes.cend(),
        [](errc code) {
            return code == errc::success || code == errc::topic_does_not_exist;
        }),
      "Undefined behavior detected within the copro pacemaker, mismatch of "
      "reported error codes");
    return enable_response_code::success;
}

std::pair<script_id, std::vector<enable_response_code>>
make_copro_error(script_id id, enable_response_code c) {
    return std::make_pair(id, std::vector<enable_response_code>{c});
}

std::vector<topic_namespace_policy>
enrich_topics(std::vector<enable_copros_request::data::topic_mode> topics) {
    std::vector<topic_namespace_policy> tns;
    tns.reserve(topics.size());
    std::transform(
      std::make_move_iterator(topics.begin()),
      std::make_move_iterator(topics.end()),
      std::back_inserter(tns),
      [](enable_copros_request::data::topic_mode&& tm) {
          return topic_namespace_policy{
            .tn = model::topic_namespace(
              model::kafka_namespace, std::move(tm.first)),
            .policy = tm.second};
      });
    return tns;
}

ss::future<enable_copros_reply::ack_id_pair>
service::enable_copro(enable_copros_request::data&& input) {
    using reply_type = enable_copros_reply::ack_id_pair;
    vlog(
      coproclog.info,
      "Request recieved to register coprocessor with id: {}",
      input.id);
    /// First verify that the request is valid
    if (input.topics.empty()) {
        vlog(
          coproclog.warn,
          "Rejected attempt to register coprocessor with id {} as it "
          "didn't pass any input topics",
          input.id);
        return ss::make_ready_future<reply_type>(
          make_copro_error(input.id, enable_response_code::internal_error));
    }
    /// Then verify that the script id isn't already registered
    return _pacemaker
      .map_reduce0(
        [id = input.id](pacemaker& pacemaker) {
            /// ... to do this, query each shard for the id
            return pacemaker.local_script_id_exists(id);
        },
        false,
        std::logical_or<>())
      .then([this, id = input.id, topics = std::move(input.topics)](
              bool script_exists) mutable {
          if (script_exists) {
              /// Protocol dictates that if the script id already exists,
              /// the topic acks reply will be a vector of 1 with the
              /// script_id_dne code as the sole element
              vlog(
                coproclog.warn,
                "Rejected attempt to register coprocessor with id {} as it "
                "has already been registered",
                id);
              return ss::make_ready_future<reply_type>(make_copro_error(
                id, enable_response_code::script_id_already_exists));
          }
          auto enriched_topics = enrich_topics(std::move(topics));
          return _pacemaker
            .map(
              [id, topics = std::move(enriched_topics)](pacemaker& pacemaker) {
                  /// Insert the script into the pacemaker, after ensuring a
                  /// double id insert is not possible
                  return pacemaker.add_source(id, topics);
              })
            .then([id](std::vector<std::vector<errc>> ack_codes) {
                vassert(!ack_codes.empty(), "acks.size() must be > 0");
                vassert(
                  !ack_codes[0].empty(), "acks vector must contain values");
                const bool all_equivalent = std::all_of(
                  ack_codes.cbegin(),
                  ack_codes.cend(),
                  [s = ack_codes[0].size()](const std::vector<errc>& v) {
                      return v.size() == s;
                  });
                vassert(all_equivalent, "Acks from all shards differ in size");
                /// Interpret the reply, aggregate the response from
                /// attempting to insert a topic across each shard, per
                /// topic.
                std::vector<enable_response_code> acks;
                for (std::size_t i :
                     boost::irange<std::size_t>(0, ack_codes[0].size())) {
                    std::vector<errc> cross_shard_acks;
                    for (std::size_t j :
                         boost::irange<std::size_t>(0, ack_codes.size())) {
                        cross_shard_acks.push_back(ack_codes[j][i]);
                    }
                    /// Ordering is preserved so the client can know which
                    /// acks correspond to what topics
                    acks.push_back(fold_enable_acks(cross_shard_acks));
                }
                return std::make_pair(id, std::move(acks));
            });
      });
}

ss::future<enable_copros_reply>
service::enable_copros(enable_copros_request&& req, rpc::streaming_context&) {
    return ss::do_with(
      std::move(req.inputs),
      [this](std::vector<enable_copros_request::data>& inputs) {
          return ss::with_scheduling_group(
            get_scheduling_group(), [this, &inputs]() {
                return ssx::async_transform(
                         inputs,
                         [this](enable_copros_request::data input) {
                             return enable_copro(std::move(input));
                         })
                  .then([](std::vector<enable_copros_reply::ack_id_pair> acks) {
                      return enable_copros_reply{.acks = std::move(acks)};
                  });
            });
      });
}

disable_response_code fold_disable_acks(const std::vector<errc>& acks) {
    const bool internal_error = std::any_of(
      acks.cbegin(), acks.cend(), [](errc ack) {
          return ack == errc::internal_error;
      });
    if (internal_error) {
        /// If any shard reported an error, this operation failed with error
        return disable_response_code::internal_error;
    }
    const bool not_removed = std::all_of(
      acks.cbegin(), acks.cend(), [](errc ack) {
          return ack == errc::script_id_does_not_exist;
      });
    if (not_removed) {
        /// If all shards reported that a script_id didn't exist, then it
        /// was never registered to begin with
        return disable_response_code::script_id_does_not_exist;
    }
    /// In oll other cases, return success
    return disable_response_code::success;
}

ss::future<disable_response_code> service::disable_copro(script_id id) {
    vlog(
      coproclog.info,
      "Request recieved to disable coprocessor with id: {}",
      id);
    return _pacemaker
      .map([id](pacemaker& pacemaker) { return pacemaker.remove_source(id); })
      .then(&fold_disable_acks);
}

ss::future<disable_copros_reply>
service::disable_copros(disable_copros_request&& req, rpc::streaming_context&) {
    return ss::do_with(std::move(req.ids), [this](std::vector<script_id>& ids) {
        return ss::with_scheduling_group(
          get_scheduling_group(), [this, &ids]() {
              return ssx::async_transform(
                       ids, [this](script_id id) { return disable_copro(id); })
                .then([](std::vector<disable_response_code> acks) {
                    return disable_copros_reply{.acks = std::move(acks)};
                });
          });
    });
}

} // namespace coproc
