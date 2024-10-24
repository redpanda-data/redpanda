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

namespace cluster::tx {

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
      _result.available() && result().get().has_error(),
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
            auto res = _result.get_shared_future().get();
            auto res_other = other._result.get_shared_future().get();
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
        gc_requests_from_older_terms(current);
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
            return cluster::errc::sequence_out_of_order;
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

void requests::stm_apply(
  const model::batch_identity& bid, model::term_id term, kafka::offset offset) {
    auto first = bid.first_seq;
    auto last = bid.last_seq;
    if (!_inflight_requests.empty()) {
        auto front = _inflight_requests.front();
        if (front->_first_sequence == first && front->_last_sequence == last) {
            // Promote the request from in_flight -> finished.
            _inflight_requests.pop_front();
        }
    }
    gc_requests_from_older_terms(term);
    result_promise_t ready{};
    ready.set_value(kafka_result{.last_offset = offset});
    _finished_requests.emplace_back(ss::make_lw_shared<request>(
      bid.first_seq, bid.last_seq, model::term_id{-1}, std::move(ready)));

    while (_finished_requests.size() > requests_cached_max) {
        _finished_requests.pop_front();
    }
}

void requests::gc_requests_from_older_terms(model::term_id current_term) {
    while (!_inflight_requests.empty()
           && _inflight_requests.front()->_term < current_term) {
        if (!_inflight_requests.front()->has_completed()) {
            // Here we know for sure the term change, these in flight
            // requests are going to fail anyway, mark them so.
            _inflight_requests.front()->set_error(errc::timeout);
        }
        _inflight_requests.pop_front();
    }
}

void requests::shutdown() { reset(cluster::errc::shutting_down); }

producer_state::producer_state(
  prefix_logger& logger,
  ss::noncopyable_function<void()> post_eviction_hook,
  producer_state_snapshot snapshot) noexcept
  : _logger(logger)
  , _id(snapshot.id)
  , _group(snapshot.group)
  , _post_eviction_hook(std::move(post_eviction_hook)) {
    if (snapshot.transaction_state) {
        _transaction_state
          = std::make_unique<producer_partition_transaction_state>(
            snapshot.transaction_state.value());
    }
    // Hydrate from snapshot.
    for (auto& req : snapshot.finished_requests) {
        result_promise_t ready{};
        ready.set_value(kafka_result{req.last_offset});
        _requests._finished_requests.push_back(ss::make_lw_shared<request>(
          req.first_sequence,
          req.last_sequence,
          model::term_id{-1},
          std::move(ready)));
    }
}

bool producer_state::operator==(const producer_state& other) const {
    return _id == other._id && _group == other._group
           && _requests == other._requests
           && _transaction_state == other._transaction_state;
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
      "ms_since_last_update: {}, evicted: {}, ",
      state._id,
      state._group,
      state._requests,
      state.ms_since_last_update(),
      state._evicted);
    if (state._transaction_state) {
        fmt::print(o, "transaction_state: {}", *state._transaction_state);
    } else {
        fmt::print(o, "transaction_state: {{ null }}");
    }
    fmt::print(o, "}}");
    return o;
}

void producer_state::shutdown_input() {
    _op_lock.broken();
    _requests.shutdown();
}

bool producer_state::can_evict() {
    if (
      // Check if already evicted
      _evicted
      // Check if an operation is in progress using this producer
      || !_op_lock.ready()
      // Check if there are operations pending state machine sync
      || !_requests._inflight_requests.empty()
      //  Check if there are any open transactions on this producer.
      || has_transaction_in_progress()) {
        vlog(_logger.debug, "[{}] cannot evict producer.", *this);
        return false;
    }
    vlog(_logger.debug, "[{}] evicting producer", *this);
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
    vassert(
      !_transaction_state,
      "Invalid epoch bump to {} for a non idempotent producer: {}",
      new_epoch,
      *this);
    vlog(_logger.info, "[{}] Reseting epoch to {}", *this, new_epoch);
    _requests.reset(errc::timeout);
    _id = model::producer_identity(_id.id, new_epoch);
}

result<request_ptr> producer_state::try_emplace_request(
  const model::batch_identity& bid, model::term_id current_term, bool reset) {
    if (bid.first_seq > bid.last_seq) {
        // malformed batch
        return cluster::errc::invalid_request;
    }
    vlog(
      _logger.trace,
      "[{}] new request, batch meta: {}, term: {}, "
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
          _logger.warn,
          "[{}] error {} processing request {}, term: {}, reset: {}",
          *this,
          result.error(),
          bid,
          current_term,
          reset);
    }
    return result;
}

void producer_state::apply_data(
  const model::record_batch_header& header, kafka::offset offset) {
    auto bid = model::batch_identity::from(header);
    if (!bid.is_idempotent() || _evicted) {
        return;
    }
    if (!bid.is_transactional && bid.pid.epoch > _id.epoch) {
        reset_with_new_epoch(bid.pid.epoch);
    }
    _requests.stm_apply(bid, header.ctx.term, offset);
    if (bid.is_transactional) {
        if (!_transaction_state) {
            // possible if begin batch got truncated.
            _transaction_state
              = std::make_unique<producer_partition_transaction_state>(
                producer_partition_transaction_state{
                  .first = header.base_offset,
                  .last = header.last_offset(),
                  .sequence = model::tx_seq{-1},
                  .timeout = std::nullopt,
                  .coordinator_partition = model::partition_id{-1},
                  .status = partition_transaction_status::ongoing});
        } else {
            _transaction_state->last = header.last_offset();
            _transaction_state->status = partition_transaction_status::ongoing;
        }
    }
    vlog(
      _logger.trace,
      "[{}] applied stm update, batch meta: {}, term: {}",
      *this,
      bid,
      header.ctx.term);
}

void producer_state::apply_transaction_begin(
  const model::record_batch_header& header,
  const fence_batch_data& parsed_batch) {
    const auto& pid = parsed_batch.bid.pid;
    if (pid.epoch < _id.epoch) {
        vlog(
          _logger.error,
          "[{}] Epoch downgrade to {}, ignoring batch {}",
          *this,
          pid,
          header);
        return;
    }
    if (has_transaction_in_progress() || _active_transaction_hook.is_linked()) {
        // We have checks in place in the stm so this does not happen. If it
        // still does it could be a bug or the log has been truncated and some
        // entries disappeared. We log and move on to make forward progress.
        vlog(
          _logger.error,
          "[{}] Encountered a begin batch {} while transaction already in "
          "progress, overriding transaction state to make forward progress. "
          "This can have unintended consequences. Hook state: {}",
          *this,
          header,
          _active_transaction_hook.is_linked());
    }
    _id = pid;
    _transaction_state = std::make_unique<producer_partition_transaction_state>(
      producer_partition_transaction_state{
        .first = header.base_offset,
        .last = header.last_offset(),
        .sequence = parsed_batch.tx_seq.value_or(model::tx_seq{0}),
        .timeout = parsed_batch.transaction_timeout_ms,
        .coordinator_partition = parsed_batch.tm,
        .status = partition_transaction_status::initialized,
      });
}

std::optional<model::tx_range>
producer_state::apply_transaction_end(model::control_record_type crt) {
    if (
      crt != model::control_record_type::tx_abort
      && crt != model::control_record_type::tx_commit) {
        return std::nullopt;
    }

    if (!_transaction_state) {
        vlog(
          _logger.debug,
          "[{}] Ignoring end transaction: {} as there is no "
          "transaction in progress, log may have been truncated?",
          *this,
          crt);
        return std::nullopt;
    }

    vlog(_logger.trace, "[{}] Applying transaction end batch: {}", *this, crt);
    if (crt == model::control_record_type::tx_commit) {
        _transaction_state->status = partition_transaction_status::committed;
    } else if (crt == model::control_record_type::tx_abort) {
        _transaction_state->status = partition_transaction_status::aborted;
    } else {
        vlog(_logger.error, "Ignoring invalid control batch type: {}", crt);
        return std::nullopt;
    }
    return model::tx_range{
      id(), _transaction_state->first, _transaction_state->last};
}

std::optional<seq_t> producer_state::last_sequence_number() const {
    auto maybe_ptr = _requests.last_request();
    if (!maybe_ptr) {
        return std::nullopt;
    }
    return maybe_ptr.value()->_last_sequence;
}

bool producer_state::has_transaction_in_progress() const {
    return _transaction_state ? _transaction_state->is_in_progress() : false;
}

bool producer_state::has_transaction_expired() const {
    if (!has_transaction_in_progress()) {
        // nothing to expire.
        return false;
    }
    return _force_transaction_expiry
           || ms_since_last_update() > _transaction_state->timeout_ms();
}

producer_state_snapshot
producer_state::snapshot(kafka::offset log_start_offset) const {
    producer_state_snapshot snapshot;
    snapshot.id = _id;
    snapshot.group = _group;
    snapshot.ms_since_last_update = ms_since_last_update();
    snapshot.finished_requests.reserve(_requests._finished_requests.size());
    for (auto& req : _requests._finished_requests) {
        if (!req->has_completed()) {
            vlog(
              _logger.error,
              "[{}] Ignoring unresolved finished request {} during snapshot",
              *this,
              *req);
            continue;
        }
        auto kafka_offset
          = req->_result.get_shared_future().get().value().last_offset;
        // offsets older than log start are no longer interesting.
        if (kafka_offset >= log_start_offset) {
            snapshot.finished_requests.emplace_back(
              req->_first_sequence, req->_last_sequence, kafka_offset);
        }
    }
    if (_transaction_state) {
        snapshot.transaction_state = *_transaction_state;
    }
    return snapshot;
}

std::optional<model::tx_seq> producer_state::get_transaction_sequence() const {
    if (has_transaction_in_progress()) {
        return _transaction_state->sequence;
    }
    return std::nullopt;
}

std::optional<model::offset>
producer_state::get_current_tx_start_offset() const {
    if (has_transaction_in_progress()) {
        return _transaction_state->first;
    }
    return std::nullopt;
}

std::optional<expiration_info> producer_state::get_expiration_info() const {
    if (!has_transaction_in_progress()) {
        return std::nullopt;
    }
    auto duration = std::chrono::duration_cast<clock_type::duration>(
      ms_since_last_update());
    auto timeout = std::chrono::duration_cast<clock_type::duration>(
      _transaction_state->timeout_ms());
    return expiration_info{
      .timeout = timeout,
      .last_update = tx::clock_type::now() - duration,
      .is_expiration_requested = has_transaction_expired()};
}

} // namespace cluster::tx
