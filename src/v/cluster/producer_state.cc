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

#include "base/vassert.h"
#include "cluster/logger.h"

namespace cluster {

std::ostream& operator<<(std::ostream& os, request_state state) {
    switch (state) {
    case request_state::initialized:
        return os << "initialized";
    case request_state::in_progress:
        return os << "in_progress";
    case request_state::completed:
        return os << "completed";
    }
}

result_promise_t::future_type request::result() const {
    return _result.get_shared_future();
}

void request::set_value(request_result_t::value_type value) {
    if (_state != request_state::completed) {
        _result.set_value(value);
        _state = request_state::completed;
    }
}

void request::set_error(request_result_t::error_type error) {
    // This is idempotent as different fibers can mark the result error
    // at different times in some edge cases.
    if (_state != request_state::completed) {
        _result.set_value(error);
        _state = request_state::completed;
        return;
    }
    vassert(
      _result.available() && result().get0().has_error(),
      "Invalid result state, expected to be available and errored out: {}",
      *this);
}

bool request::operator==(const request& other) const {
    bool compare = _first_sequence == other._first_sequence
                   && _last_sequence == other._last_sequence
                   && _state == other._state;

    // both are in progress or both finished
    if (compare && _result.available()) {
        // both requests have finished
        // compare the result from promise;
        compare = compare && _result.failed() == other._result.failed();
        if (compare && !_result.failed()) {
            // both requests succeeded. compare results.
            auto res = _result.get_shared_future().get0();
            auto res_other = other._result.get_shared_future().get0();
            compare = compare && res.has_error() == res_other.has_error();
            if (compare) {
                if (res.has_error()) {
                    // both have errored out, compare errors
                    compare = compare && res.error() == res_other.error();
                } else {
                    // both finished, compared result offsets.
                    compare = compare
                              && res.value().last_offset
                                   == res_other.value().last_offset;
                }
            }
        }
    }
    return compare;
}

bool requests::operator==(const requests& other) const {
    // check size match
    bool result
      = (_inflight_requests.size() == other._inflight_requests.size())
        && (_finished_requests.size() == other._finished_requests.size());
    if (!result) {
        return false;
    }

    auto match_inflight = std::equal(
      _inflight_requests.begin(),
      _inflight_requests.end(),
      other._inflight_requests.begin(),
      other._inflight_requests.end(),
      [](const request_ptr& left, const request_ptr& right) {
          return *left == *right;
      });

    auto match_finished = std::equal(
      _finished_requests.begin(),
      _finished_requests.end(),
      other._finished_requests.begin(),
      other._finished_requests.end(),
      [](const request_ptr& left, const request_ptr& right) {
          return *left == *right;
      });

    return match_inflight && match_finished;
}

std::optional<request_ptr> requests::last_request() const {
    if (!_inflight_requests.empty()) {
        return _inflight_requests.back();
    } else if (!_finished_requests.empty()) {
        return _finished_requests.back();
    }
    return std::nullopt;
}

void requests::reset(request_result_t::error_type error) {
    for (auto& request : _inflight_requests) {
        if (!request->has_completed()) {
            request->set_error(error);
        }
    }
    _inflight_requests.clear();
    _finished_requests.clear();
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
        reset(errc::timeout);
    } else {
        // gc and fail any inflight requests from old terms
        // these are guaranteed to be failed because of sync() guarantees
        // prior to this request.
        while (!_inflight_requests.empty()
               && _inflight_requests.front()->_term < current) {
            if (!_inflight_requests.front()->has_completed()) {
                // Here we know for sure the term change, these in flight
                // requests are going to fail anyway, mark them so.
                _inflight_requests.front()->set_error(errc::timeout);
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
    // if there are more than max cached requests inflight start dropping the
    // oldest of them
    while (_inflight_requests.size() > requests_cached_max
           && _inflight_requests.front()->has_completed()) {
        _inflight_requests.pop_front();
        // clear finished requests as the producer will not be interested in
        // them anymore
        if (!_finished_requests.empty()) {
            _finished_requests.clear();
        }
    }

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

void requests::shutdown() { reset(cluster::errc::shutting_down); }

producer_state::producer_state(
  ss::noncopyable_function<void()> post_eviction_hook,
  producer_state_snapshot snapshot) noexcept
  : _id(snapshot._id)
  , _group(snapshot._group)
  , _post_eviction_hook(std::move(post_eviction_hook)) {
    // Hydrate from snapshot.
    for (auto& req : snapshot._finished_requests) {
        result_promise_t ready{};
        ready.set_value(kafka_result{req._last_offset});
        _requests._finished_requests.push_back(ss::make_lw_shared<request>(
          req._first_sequence,
          req._last_sequence,
          model::term_id{-1},
          std::move(ready)));
    }
}

bool producer_state::operator==(const producer_state& other) const {
    return _id == other._id && _group == other._group
           && _requests == other._requests;
}

std::ostream& operator<<(std::ostream& o, const request& request) {
    fmt::print(
      o,
      "{{ first: {}, last: {}, term: {}, result_available: {}, state: {} }}",
      request._first_sequence,
      request._last_sequence,
      request._term,
      request._result.available(),
      request._state);
    return o;
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
      "ms_since_last_update: {} }}",
      state._id,
      state._group,
      state._requests,
      state.ms_since_last_update());
    return o;
}

void producer_state::shutdown_input() {
    _op_lock.broken();
    _requests.shutdown();
}

bool producer_state::can_evict() {
    // oplock is taken, do not allow producer state to be evicted
    if (!_op_lock.ready() || _evicted) {
        return false;
    }

    vlog(clusterlog.debug, "evicting producer: {}", *this);
    _evicted = true;
    shutdown_input();
    return true;
}

void producer_state::reset_with_new_epoch(model::producer_epoch new_epoch) {
    vassert(
      new_epoch > _id.get_epoch(),
      "Invalid epoch bump to {} for producer {}",
      new_epoch,
      *this);
    vlog(clusterlog.info, "[{}] Reseting epoch to {}", *this, new_epoch);
    _requests.reset(errc::timeout);
    _id = model::producer_identity(_id.id, new_epoch);
}

result<request_ptr> producer_state::try_emplace_request(
  const model::batch_identity& bid, model::term_id current_term, bool reset) {
    if (bid.first_seq > bid.last_seq) {
        // malformed batch
        return errc::invalid_request;
    }
    vlog(
      clusterlog.trace,
      "new request from producer: {}, batch meta: {}, term: {}, "
      "reset: {}, request_state: {}",
      *this,
      bid,
      current_term,
      reset,
      _requests);

    auto result = _requests.try_emplace(
      bid.first_seq, bid.last_seq, current_term, reset);

    if (unlikely(result.has_error())) {
        vlog(
          clusterlog.warn,
          "[{}] error {} processing request {}, term: {}, reset: {}",
          *this,
          result.error(),
          bid,
          current_term,
          reset);
    }
    return result;
}

bool producer_state::update(
  const model::batch_identity& bid, kafka::offset offset) {
    if (_evicted) {
        return false;
    }
    if (!bid.is_transactional && bid.pid.epoch > _id.epoch) {
        reset_with_new_epoch(model::producer_epoch{bid.pid.epoch});
    }
    bool relink_producer = _requests.stm_apply(bid, offset);
    vlog(
      clusterlog.trace,
      "applied stm update for pid: {}, batch meta: {}, relink_producer: {}",
      *this,
      bid,
      relink_producer);
    return relink_producer;
}

std::optional<seq_t> producer_state::last_sequence_number() const {
    auto maybe_ptr = _requests.last_request();
    if (!maybe_ptr) {
        return std::nullopt;
    }
    return maybe_ptr.value()->_last_sequence;
}

producer_state_snapshot
producer_state::snapshot(kafka::offset log_start_offset) const {
    producer_state_snapshot snapshot;
    snapshot._id = _id;
    snapshot._group = _group;
    snapshot._ms_since_last_update = ms_since_last_update();
    snapshot._finished_requests.reserve(_requests._finished_requests.size());
    for (auto& req : _requests._finished_requests) {
        vassert(
          req->has_completed(),
          "_finished_requests has unresolved promise: {}, range:[{}, {}]",
          *this,
          req->_first_sequence,
          req->_last_sequence);
        auto kafka_offset
          = req->_result.get_shared_future().get().value().last_offset;
        // offsets older than log start are no longer interesting.
        if (kafka_offset >= log_start_offset) {
            snapshot._finished_requests.push_back(
              producer_state_snapshot::finished_request{
                ._first_sequence = req->_first_sequence,
                ._last_sequence = req->_last_sequence,
                ._last_offset = kafka_offset});
        }
    }
    return snapshot;
}

} // namespace cluster
