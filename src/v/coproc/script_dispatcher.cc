/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/script_dispatcher.h"

#include "coproc/errc.h"
#include "coproc/logger.h"
#include "coproc/types.h"
#include "model/namespace.h"
#include "utils/functional.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>

namespace coproc::wasm {

static std::vector<topic_namespace_policy>
enrich_topics(std::vector<enable_copros_reply::topic_policy> itopics) {
    std::vector<topic_namespace_policy> tnp;
    tnp.reserve(itopics.size());
    std::transform(
      std::make_move_iterator(itopics.begin()),
      std::make_move_iterator(itopics.end()),
      std::back_inserter(tnp),
      [](enable_copros_reply::topic_policy&& tp) {
          return topic_namespace_policy{
            .tn = model::topic_namespace(
              model::kafka_namespace, std::move(tp.first)),
            .policy = tp.second};
      });
    return tnp;
}

static bool
contains_code(const std::vector<coproc::errc>& codes, coproc::errc code) {
    return std::any_of(codes.cbegin(), codes.cend(), xform::equal_to(code));
}

static bool
contains_all_codes(const std::vector<coproc::errc>& codes, coproc::errc code) {
    return std::all_of(codes.cbegin(), codes.cend(), xform::equal_to(code));
}

static bool fold_enable_codes(const std::vector<coproc::errc>& codes) {
    /// If at least one shard returned with error, the effect of registering
    /// has produced undefined behavior, so returns 'internal_error'
    if (contains_code(codes, coproc::errc::internal_error)) {
        vlog(
          coproclog.error,
          "Internal error encountered when internally regisering script");
        return true;
    }
    /// For identifiable errors, all shards should have agreed on the error
    if (contains_all_codes(codes, coproc::errc::topic_does_not_exist)) {
        return true;
    }
    /// If the following circumstances occur, that means there is a bug in
    /// the wasm engine
    if (
      contains_all_codes(codes, coproc::errc::invalid_ingestion_policy)
      || contains_all_codes(codes, coproc::errc::materialized_topic)
      || contains_all_codes(codes, coproc::errc::invalid_topic)) {
        vlog(
          coproclog.error,
          "wasm validator passed on datum for which didn't pass on "
          "redpanda");
        return true;
    }
    /// The only other 'normal' circumstance is some shards reporting
    /// 'success' and others reporting 'topic_does_not_exist'
    vassert(
      std::all_of(
        codes.cbegin(),
        codes.cend(),
        [](coproc::errc code) {
            return code == coproc::errc::success
                   || code == coproc::errc::topic_does_not_exist;
        }),
      "Undefined behavior detected within the copro pacemaker, mismatch of "
      "reported error codes");
    return false;
}

static bool should_immediately_deregister(
  const std::vector<std::vector<coproc::errc>>& codes) {
    vassert(!codes.empty(), "codes.size() must be > 0");
    vassert(!codes[0].empty(), "codes vector must contain values");
    const bool all_equivalent = std::all_of(
      codes.cbegin(),
      codes.cend(),
      [s = codes[0].size()](const std::vector<coproc::errc>& v) {
          return v.size() == s;
      });
    vassert(all_equivalent, "Codes from all shards differ in size");
    /// Interpret the reply, aggregate the response from
    /// attempting to insert a topic across each shard, per
    /// topic.
    std::vector<bool> results;
    for (std::size_t i : boost::irange<std::size_t>(0, codes[0].size())) {
        std::vector<coproc::errc> cross_shard_codes;
        for (std::size_t j : boost::irange<std::size_t>(0, codes.size())) {
            cross_shard_codes.push_back(codes[j][i]);
        }
        /// Ordering is preserved so the client can know which
        /// acks correspond to what topics
        results.push_back(fold_enable_codes(cross_shard_codes));
    }
    return std::any_of(results.cbegin(), results.cend(), xform::identity());
}

static disable_response_code
fold_disable_codes(const std::vector<coproc::errc>& codes) {
    const bool internal_error = std::any_of(
      codes.cbegin(), codes.cend(), [](coproc::errc code) {
          return code == coproc::errc::internal_error;
      });
    if (internal_error) {
        /// If any shard reported an error, this operation failed with error
        return disable_response_code::internal_error;
    }
    const bool not_removed = std::all_of(
      codes.cbegin(), codes.cend(), [](coproc::errc code) {
          return code == coproc::errc::script_id_does_not_exist;
      });
    if (not_removed) {
        /// If all shards reported that a script_id didn't exist, then it
        /// was never registered to begin with
        return disable_response_code::script_id_does_not_exist;
    }
    /// In oll other cases, return success
    return disable_response_code::success;
}

script_dispatcher::script_dispatcher(
  ss::sharded<pacemaker>& p, ss::abort_source& as)
  : _pacemaker(p)
  , _abort_source(as)
  , _transport(_pacemaker.local().resources().transport) {}

ss::future<std::vector<std::vector<coproc::errc>>>
script_dispatcher::add_sources(
  script_id id, std::vector<topic_namespace_policy> itopics) {
    return _pacemaker.map([id, itopics = std::move(itopics)](pacemaker& p) {
        return p.add_source(id, itopics);
    });
}

ss::future<> script_dispatcher::enable_coprocessors(enable_copros_request req) {
    auto client = co_await get_client();
    if (!client) {
        co_return;
    }
    auto reply = co_await client->enable_coprocessors(
      std::move(req), rpc::client_opts(model::no_timeout));
    if (!reply) {
        vlog(coproclog.error, "Could not complete enable_coprocessors request");
        co_return;
    }
    std::vector<script_id> deregisters;
    for (enable_copros_reply::data& r : reply.value().data.acks) {
        /// 1. Only continue on success
        script_id id = r.script_meta.id;
        vlog(coproclog.debug, "Request complete: {}", id);
        if (r.ack != enable_response_code::success) {
            vlog(
              coproclog.info,
              "wasm engine failed to register script, returned with code: "
              "{}",
              r.ack);
            continue;
        }

        /// 2. A request with no topics is malformed
        if (r.script_meta.input_topics.empty()) {
            vlog(
              coproclog.info,
              "wasm engine rejected malformed request to register script with "
              "id {}: topics list was empty",
              id);
            continue;
        }

        /// 3. Ensure script isn't already registered
        bool is_already_registered = co_await _pacemaker.map_reduce0(
          [id](pacemaker& p) { return p.local_script_id_exists(id); },
          false,
          std::logical_or<>());
        if (is_already_registered) {
            vlog(coproclog.info, "Script id already registered: {}", id);
            continue;
        }

        /// 4. Register the scripts with the pacemaker
        auto itopics = enrich_topics(std::move(r.script_meta.input_topics));
        auto results = co_await add_sources(id, std::move(itopics));

        /// 5. If there was a failure, we must deregisetr the coprocessor.
        /// There are only 2 possibilities for this scenario:
        ///
        /// a. wasm engine didn't properly validate something that didn't
        /// pass redpandas validator
        /// b. A script contains an input topic
        /// that doesn't yet exist.
        if (should_immediately_deregister(results)) {
            deregisters.push_back(id);
        } else {
            vlog(
              coproclog.info, "Successfully registered script with id: {}", id);
        }
    }
    /// This can be removed once we complete the feature to have copros that
    /// can register for input topics that don't yet exist
    if (!deregisters.empty()) {
        vlog(coproclog.error, "Immediately deregistering ids {}", deregisters);
        auto reply = co_await client->disable_coprocessors(
          disable_copros_request{.ids = std::move(deregisters)},
          rpc::client_opts(model::no_timeout));
        if (!reply) {
            vlog(
              coproclog.error,
              "Failed to deregister scripts for which had topics that didn't "
              "already exist");
        }
    }
}

ss::future<std::vector<coproc::errc>>
script_dispatcher::remove_sources(script_id id) {
    return _pacemaker.map([id](pacemaker& p) { return p.remove_source(id); });
}

ss::future<>
script_dispatcher::disable_coprocessors(disable_copros_request req) {
    auto client = co_await get_client();
    if (!client) {
        co_return;
    }
    auto reply = co_await client->disable_coprocessors(
      std::move(req), rpc::client_opts(model::no_timeout));
    if (!reply) {
        vlog(
          coproclog.error, "Could not complete disable_coprocessors request");
        co_return;
    }
    for (const auto& [id, code] : reply.value().data.acks) {
        if (code != disable_response_code::success) {
            vlog(
              coproclog.info,
              "wasm engine failed to deregister script {}, continuing "
              "anyway",
              id);
        }
        std::vector<coproc::errc> results = co_await remove_sources(id);
        auto final_code = fold_disable_codes(results);
        if (final_code != disable_response_code::success) {
            vlog(
              coproclog.error,
              "redpanda failed to deregister script id {} with error {}",
              id,
              final_code);
        } else {
            vlog(coproclog.info, "Successfully deregistered script: {}", id);
        }
    }
}

ss::future<> script_dispatcher::disable_all_coprocessors() {
    struct error_cnt {
        size_t n_success{0};
        size_t n_internal_error{0};
        size_t n_script_dnes{0};
    };
    auto client = co_await get_client();
    if (!client) {
        co_return;
    }
    auto reply = co_await client->disable_all_coprocessors(
      empty_request(), rpc::client_opts(model::no_timeout));
    if (!reply) {
        vlog(
          coproclog.error,
          "Could not complete disable_all_coprocessors request");
        co_return;
    }
    error_cnt cnt = std::accumulate(
      reply.value().data.acks.cbegin(),
      reply.value().data.acks.cend(),
      error_cnt(),
      [](error_cnt cnt, const disable_copros_reply::ack& ack) {
          if (ack.second == disable_response_code::success) {
              cnt.n_success += 1;
          } else if (
            ack.second == disable_response_code::script_id_does_not_exist) {
              cnt.n_script_dnes += 1;
          } else if (ack.second == disable_response_code::internal_error) {
              cnt.n_internal_error += 1;
          }
          return cnt;
      });
    vlog(
      coproclog.info,
      "Disable all coprocessors, {} disabled successfully, {} failed, {} "
      "requests to disable scripts that weren't registered",
      cnt.n_success,
      cnt.n_internal_error,
      cnt.n_script_dnes);
}

ss::future<std::optional<coproc::supervisor_client_protocol>>
script_dispatcher::get_client() {
    model::timeout_clock::duration dur = 1s;
    while (true) {
        if (_abort_source.abort_requested()) {
            co_return std::nullopt;
        }
        auto transport = co_await _transport.get_connected(model::no_timeout);
        if (!transport) {
            vlog(
              coproclog.error,
              "script_dispatcher failed to aquire a connection to the wasm "
              "engine (to deploy or remove a script), retrying... ");
            co_await ss::sleep(dur);
            dur = std::min(model::timeout_clock::duration(10s), dur * 2);
        } else {
            co_return supervisor_client_protocol(*transport.value());
        }
    }
}

} // namespace coproc::wasm
