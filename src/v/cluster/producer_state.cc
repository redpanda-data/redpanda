/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "producer_state.h"

#include "cluster/logger.h"
#include "cluster/producer_state_manager.h"
#include "vassert.h"

namespace cluster {

result_promise_t::future_type request::result() const {
    return _result.get_shared_future();
}

std::optional<request_ptr> requests::last_request() const {
    if (!_inflight_requests.empty()) {
        return _inflight_requests.back();
    } else if (!_finished_requests.empty()) {
        return _finished_requests.back();
    }
    return std::nullopt;
}

bool requests::is_valid_sequence(seq_t incoming) const {
    auto last_req = last_request();
    return
      // this is the first request with seq=0
      (!last_req && incoming == 0)
      // incoming request forms a sequence with last_request
      || (last_req && last_req.value()->_last_sequence + 1 == incoming)
      // sequence numbers got rolled over because they hit int32 max limit.
      || (last_req && last_req.value()->_last_sequence == std::numeric_limits<seq_t>::max() && incoming == 0);
}

result<request_ptr> requests::try_emplace(
  seq_t first, seq_t last, model::term_id current, bool reset_sequences) {
    if (reset_sequences) {
        // reset all the sequence tracking state, avoids any sequence
        // checks for sequence tracking.
        while (!_inflight_requests.empty()) {
            if (!_inflight_requests.front()->has_completed()) {
                _inflight_requests.front()->set_value(errc::timeout);
            }
            _inflight_requests.pop_front();
        }
        _finished_requests.clear();
    } else {
        // gc and fail any inflight requests from old terms
        // these are guaranteed to be failed because of sync() guarantees
        // prior to this request.
        while (!_inflight_requests.empty()
               && _inflight_requests.front()->_term < current) {
            if (!_inflight_requests.front()->has_completed()) {
                // Here we know for sure the term change, these in flight
                // requests are going to fail anyway, mark them so.
                _inflight_requests.front()->set_value(errc::timeout);
            }
            _inflight_requests.pop_front();
        }

        // check if an existing request matches
        auto match_it = std::find_if(
          _finished_requests.begin(),
          _finished_requests.end(),
          [first, last](const auto& request) {
              return request->_first_sequence == first
                     && request->_last_sequence == last;
          });

        if (match_it != _finished_requests.end()) {
            return *match_it;
        }

        match_it = std::find_if(
          _inflight_requests.begin(),
          _inflight_requests.end(),
          [first, last, current](const auto& request) {
              return request->_first_sequence == first
                     && request->_last_sequence == last
                     && request->_term == current;
          });

        if (match_it != _inflight_requests.end()) {
            return *match_it;
        }

        if (!is_valid_sequence(first)) {
            vlog(
              clusterlog.trace,
              "out of order sequence for request range [{}, {}], term: {}",
              first,
              last,
              current);
            return errc::sequence_out_of_order;
        }
    }

    // All invariants satisfied, enqueue the request.
    _inflight_requests.emplace_back(
      ss::make_lw_shared<request>(first, last, current, result_promise_t{}));

    return _inflight_requests.back();
}

bool requests::stm_apply(
  const model::batch_identity& bid, kafka::offset offset) {
    bool relink_producer = false;
    auto first = bid.first_seq;
    auto last = bid.last_seq;
    if (!_inflight_requests.empty()) {
        auto front = _inflight_requests.front();
        if (front->_first_sequence == first && front->_last_sequence == last) {
            // Promote the request from in_flight -> finished.
            _inflight_requests.pop_front();
        }
    } else {
        // on leaders, producer state is relinked with the manager as a part
        // of run_with_lock. On followers this happens as their stms catch up
        // with the committed changes. This branch is taken only when the stm is
        // applying changes from a different leader thus prompting a relink.
        relink_producer = true;
    }
    result_promise_t ready{};
    ready.set_value(kafka_result{.last_offset = offset});
    _finished_requests.emplace_back(ss::make_lw_shared<request>(
      bid.first_seq, bid.last_seq, model::term_id{-1}, std::move(ready)));

    while (_finished_requests.size() > requests_cached_max) {
        _finished_requests.pop_front();
    }
    return relink_producer;
}

void requests::shutdown() {
    for (auto& request : _inflight_requests) {
        if (!request->has_completed()) {
            request->_result.set_value(errc::shutting_down);
        }
    }
    _inflight_requests.clear();
    _finished_requests.clear();
}

std::ostream& operator<<(std::ostream& o, const requests& requests) {
    fmt::print(
      o,
      "{{ inflight: {}, finished: {} }}",
      requests._inflight_requests.size(),
      requests._finished_requests.size());
    return o;
}

std::ostream& operator<<(std::ostream& o, const producer_state& state) {
    fmt::print(
      o,
      "{{ id: {}, group: {}, requests: {}, "
      "ms_since_last_update: {}, evicted: {} }}",
      state._id,
      state._group,
      state._requests,
      state.ms_since_last_update(),
      state._evicted);
    return o;
}

ss::future<> producer_state::shutdown_input() {
    if (_evicted) {
        return ss::now();
    }
    _op_lock.broken();
    return _gate.close().then([this] { _requests.shutdown(); });
}

ss::future<> producer_state::evict() {
    if (_evicted) {
        return ss::now();
    }
    vlog(clusterlog.debug, "evicting producer: {}", *this);
    _evicted = true;
    vassert(_hook.is_linked(), "unexpected state, producer unlinked.");
    unlink_self();
    return shutdown_input().then_wrapped([this](auto result) {
        _post_eviction_hook();
        return result;
    });
}

void producer_state::register_self() {
    _parent.get().register_producer(*this);
    touch();
}

void producer_state::deregister_self() {
    _parent.get().deregister_producer(*this);
}

void producer_state::unlink_self() {
    if (_hook.is_linked()) {
        _hook.unlink();
        vlog(clusterlog.trace, "unlink self: {}", *this);
    }
}

void producer_state::link_self() {
    if (_ops_in_progress == 0) {
        _parent.get().link(*this);
        vlog(clusterlog.trace, "link self: {}", *this);
    }
    touch();
}

result<request_ptr> producer_state::try_emplace_request(
  const model::batch_identity& bid, model::term_id current_term, bool reset) {
    if (bid.first_seq > bid.last_seq) {
        // malformed batch
        return errc::invalid_request;
    }
    vlog(
      clusterlog.trace,
      "new request from producer: {}, first_seq: {}, last_seq: {}, term: {}, "
      "reset: {}, request_state: {}",
      *this,
      bid.first_seq,
      bid.last_seq,
      current_term,
      reset,
      _requests);
    return _requests.try_emplace(
      bid.first_seq, bid.last_seq, current_term, reset);
}

void producer_state::update(
  const model::batch_identity& bid, kafka::offset offset) {
    if (_gate.is_closed()) {
        return;
    }
    bool relink_producer = _requests.stm_apply(bid, offset);
    vlog(
      clusterlog.trace,
      "applied stm update for pid: {}, first: {}, last: {}, relink_producer: "
      "{}",
      *this,
      bid.first_seq,
      bid.last_seq,
      relink_producer);
    if (relink_producer) {
        // relink for LRU tracking on followers.
        // on leaders where this operation ran, it happens
        // in run_with_lock() that relinks the producer.
        unlink_self();
        link_self();
    }
}

} // namespace cluster
