/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/aggregated_uploader.h"

#include "cloud_topics/errc.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "ssx/sformat.h"
#include "storage/record_batch_utils.h"
#include "types.h"
#include "utils/retry_chain_node.h"
#include "utils/uuid.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <exception>
#include <iterator>

using namespace std::chrono_literals;

namespace cloud_topics {

template<class Clock>
aggregated_uploader<Clock>::aggregated_uploader(
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<Clock>& remote)
  : _remote(remote)
  , _bucket(std::move(bucket))
  , _upload_timeout(
      config::shard_local_cfg().cloud_storage_segment_upload_timeout_ms.bind())
  , _upload_interval(config::shard_local_cfg()
                       .cloud_storage_aggregated_log_upload_interval_ms.bind())
  , _rtc(_as)
  // TODO: fixme
  , _logger(cd_log, _rtc, "NEEDLE") {}

template<class Clock>
ss::future<> aggregated_uploader<Clock>::start() {
    if (ss::this_shard_id() == 0) {
        // Shard 0 aggregates information about all other shards
        ssx::spawn_with_gate(_gate, [this] { return bg_controller_loop(); });
    }
    co_return;
}

template<class Clock>
ss::future<> aggregated_uploader<Clock>::stop() {
    _as.request_abort();
    co_await _gate.close();
    co_return;
}

template<class Clock>
chunked_vector<
  ss::foreign_ptr<typename aggregated_uploader<Clock>::write_request_ptr>>
aggregated_uploader<Clock>::get_write_requests(
  aggregated_uploader_index lower_bound) {
    /*TODO: remove*/ vlog(
      _logger.debug,
      "get_write_requests called with lower_bound = {}",
      lower_bound);
    chunked_vector<ss::foreign_ptr<write_request_ptr>> requests;
    for (const auto& wr : _pending) {
        if (wr->index > lower_bound) {
            auto p = ss::make_foreign(wr);
            requests.push_back(std::move(p));
        }
    }
    /*TODO: remove*/ vlog(
      _logger.debug,
      "get_write_requests returned {} elements",
      requests.size());
    return requests;
}

namespace details {

serializing_consumer::serializing_consumer(
  std::optional<model::ntp> ntp,
  object_id id,
  iobuf* buf,
  data_layout* layout) // NOLINT
  : _ntp(std::move(ntp))
  , _id(id)
  , _output(buf)
  , _layout(layout) {}

ss::future<ss::stop_iteration>
serializing_consumer::operator()(model::record_batch batch) {
    auto tmp_copy = batch.copy();
    /*TODO: remove*/ vlog(
      cd_log.debug, "serializing consumer batch: {}", batch);
    auto hdr_iobuf = storage::batch_header_to_disk_iobuf(batch.header());
    auto rec_iobuf = std::move(batch).release_data();

    if (_ntp.has_value()) {
        // Register data layout information here. Only data batches
        // will have ntp set.
        _layout->register_bytes(
          _ntp.value(), _id, &hdr_iobuf, &rec_iobuf, std::move(tmp_copy));
    }

    // Propagate to the output
    _output->append(std::move(hdr_iobuf));
    _output->append(std::move(rec_iobuf));

    co_return ss::stop_iteration::no;
}
struct one_shot_provider : stream_provider {
    explicit one_shot_provider(ss::input_stream<char> s)
      : stream(std::move(s)) {}
    ss::input_stream<char> take_stream() override {
        auto tmp = std::exchange(stream, std::nullopt);
        return std::move(tmp.value());
    }
    ss::future<> close() override {
        if (stream) {
            co_await stream.value().close();
        }
    }
    std::optional<ss::input_stream<char>> stream;
};

} // namespace details

template<class Clock>
ss::future<> aggregated_uploader<Clock>::upload_object(
  ss::foreign_ptr<
    typename aggregated_uploader<Clock>::aggregated_write_request_ptr> p) {
    /*TODO: remove*/ vlog(_logger.debug, "upload_object is called");
    auto id = uuid_t::create();
    auto name = ssx::sformat("{}", id);
    details::data_layout layout;

    // Send error response to all waiters included into the upload
    auto ack_error = [&](errc e) {
        for (auto& [ntp, vec] : p->to_upload) {
            for (auto& ph : vec) {
                ph->set_value(e);
            }
        }
    };

    // Send successful response to all waiters included into the upload.
    // The response includes placeholder batches.
    auto ack_result = [&]() mutable {
        for (auto& [ntp, vec] : p->to_upload) {
            for (auto& ph : vec) {
                auto ntp = ph->ntp;
                auto& ref = layout.get_batch_ref(ntp);
                // TODO: add some defenses from missing 'set_value' calls
                // and from double 'set_value' calls.
                ph->set_value(std::move(ref.placeholders));
            }
        }
        // Invariant: we should set every promise in the request
    };

    // The failure model is all or nothing. In case of success the entire
    // content of the 'p' is uploaded. We need to notify every participant
    // about the success. Same is true in case of failure. This code can't
    // upload the input partially.
    std::exception_ptr err;
    try {
        // Clock type is not parametrized further down the call chain.
        basic_retry_chain_node<Clock> local_rtc(
          Clock::now() + _upload_timeout(),
          // Backoff doesn't matter, the operation never retries
          100ms,
          retry_strategy::disallow,
          &_rtc);

        auto path = cloud_storage_clients::object_key(name);

        // make input stream by joining different record_batch_reader outputs
        auto input_stream = ss::input_stream<char>(ss::data_source(
          std::make_unique<details::concatenating_stream_data_source<Clock>>(
            object_id{id}, p, layout)));

        // reset functor can be used only once
        // no retries are attempted
        auto reset_str = [s = std::move(input_stream)]() mutable {
            using provider_t = std::unique_ptr<stream_provider>;
            return ss::make_ready_future<provider_t>(
              std::make_unique<details::one_shot_provider>(std::move(s)));
        };
        // without retries we don't need lazy abort source
        lazy_abort_source noop{[]() { return std::nullopt; }};

        cloud_io::basic_transfer_details<Clock> td{
          .bucket = _bucket,
          .key = path,
          .parent_rtc = local_rtc,
        };

        auto upl_result = co_await _remote.upload_stream(
          std::move(td),
          p->size_bytes,
          std::move(reset_str),
          noop,
          std::string_view("L0"),
          0);

        switch (upl_result) {
        case cloud_io::upload_result::success:
            break;
        case cloud_io::upload_result::cancelled:
            err = std::make_exception_ptr(ss::gate_closed_exception());
            break;
        case cloud_io::upload_result::timedout:
            err = std::make_exception_ptr(ss::timed_out_error());
            break;
        case cloud_io::upload_result::failed:
            err = std::make_exception_ptr(
              std::system_error(errc::upload_failure));
        }
    } catch (...) {
        err = std::current_exception();
        /*TODO: remove*/ vlog(_logger.error, "Unexpected error {}", err);
    }

    if (err) {
        vlog(_logger.error, "Aggregated upload error: {}", err);
        ack_error(errc::timeout);
        std::rethrow_exception(err);
    }

    /*TODO: remove*/ vlog(_logger.debug, "upload_object ACK results");
    ack_result();
    /*TODO: remove*/ vlog(_logger.debug, "upload_object ACK results DONE");
}

/// This class implements x-shard information flow and manages
/// the state needed for this.
template<class Clock>
class upload_data_flow_controller {
    using write_request = aggregated_uploader<Clock>::write_request;
    using write_request_ptr = aggregated_uploader<Clock>::write_request_ptr;
    using aggregated_write_request = details::aggregated_write_request<Clock>;
    using aggregated_write_request_ptr
      = details::aggregated_write_request_ptr<Clock>;

public:
    explicit upload_data_flow_controller(
      aggregated_uploader<Clock>* p,
      std::chrono::milliseconds upload_interval,
      size_t max_bytes,
      basic_retry_chain_node<Clock>& rtc)
      : _shard_state(ss::smp::count, aggregated_uploader_index(-1))
      , _parent(p)
      , _upload_interval(upload_interval)
      , _max_bytes(max_bytes)
      // TODO: fixme
      , _logger(cd_log, rtc, "NEEDLE2") {}

    upload_data_flow_controller() = delete;
    ~upload_data_flow_controller() = default;
    upload_data_flow_controller(const upload_data_flow_controller&) = delete;
    upload_data_flow_controller& operator=(const upload_data_flow_controller&)
      = delete;
    upload_data_flow_controller(upload_data_flow_controller&&) noexcept
      = delete;
    upload_data_flow_controller&
    operator=(upload_data_flow_controller&&) noexcept
      = delete;

    ss::future<> start() { co_return; }
    ss::future<> stop() {
        _as.request_abort();
        _cv.broken();
        //_mutex.broken();
        co_await _gate.close();
    }
    ss::future<> schedule_uploads() {
        try {
            /*TODO: remove*/ vlog(
              _logger.debug,
              "[schedule_uploads] "
              "upload_data_flow_controller.schedule_uploads");
            auto h = _gate.hold();
            unsigned shard_rr{0};
            while (!_gate.is_closed() && !_as.abort_requested()) {
                co_await wait_for_next_upload();
                /*TODO: remove*/ vlog(
                  _logger.debug,
                  "[schedule_uploads] "
                  "upload_data_flow_controller.schedule_uploads acquiring "
                  "units");
                auto u = co_await ss::get_units(_mutex, 1);
                /*TODO: remove*/ vlog(
                  _logger.debug,
                  "[schedule_uploads] "
                  "upload_data_flow_controller.schedule_uploads acquiring "
                  "units done");
                auto size_bytes = _bytes_used;
                _bytes_used = 0;
                decltype(_pending) to_upload;
                std::swap(_pending, to_upload);
                u.return_all();
                // TODO: fixme
                // This code is performing simple round robin
                // search to schedule uploads from different shards.
                auto shard = ss::shard_id(shard_rr % ss::smp::count);
                shard_rr++;
                auto result = ss::make_lw_shared<aggregated_write_request>();
                result->to_upload = std::move(to_upload);
                result->size_bytes = size_bytes;
                /*TODO: remove*/ vlog(
                  _logger.debug,
                  "[schedule_uploads] "
                  "upload_data_flow_controller.schedule_uploads ~~~ target: "
                  "{}, "
                  "upload size: {}",
                  shard,
                  result->size_bytes);
                if (size_bytes > 0) {
                    co_await _parent->container().invoke_on(
                      shard,
                      [fp = ss::make_foreign(std::move(result)),
                       req = std::move(to_upload)](
                        aggregated_uploader<Clock>& svc) mutable {
                          /*TODO: remove*/ vlog(
                            cd_log.debug,
                            "NEEDLE [schedule_uploads] calling 'upload_object' "
                            "on "
                            "shard {}",
                            ss::this_shard_id());
                          return svc.upload_object(std::move(fp));
                      });
                }
            }
        } catch (...) {
            /*TODO: remove*/ vlog(
              _logger.debug,
              "[schedule_uploads] upload_data_flow_controller.schedule_uploads "
              "failure {}",
              std::current_exception());
        }
        /*TODO: remove*/ vlog(
          _logger.debug,
          "[schedule_uploads]upload_data_flow_controller.schedule_uploads "
          "exit");
        co_return;
    }
    ss::future<> collect_data() {
        /*TODO: remove*/ vlog(
          _logger.debug,
          "[collect_data] upload_data_flow_controller.collect_data");
        auto h = _gate.hold();
        try {
            while (!_gate.is_closed() && !_as.abort_requested()) {
                // use short interval to fetch write requests from
                // every shard
                constexpr static auto collection_interval = 10ms;
                co_await ss::sleep_abortable<Clock>(collection_interval, _as);
                using write_request_vec
                  = chunked_vector<ss::foreign_ptr<write_request_ptr>>;
                using nested_list = std::list<write_request_vec>;

                // shard_state is only updated by this fiber so it's safe
                // to read it
                auto st = _shard_state;
                auto map = [st](aggregated_uploader<Clock>& shard) {
                    /*TODO: remove*/ vlog(
                      cd_log.debug,
                      "NEEDLE[collect_data]  upload_data_flow_controller MAP "
                      "start");
                    auto sid = ss::this_shard_id();
                    aggregated_uploader_index index = st.at(sid);
                    auto items = shard.get_write_requests(index);
                    nested_list result;
                    result.push_back(std::move(items));
                    /*TODO: remove*/ vlog(
                      cd_log.debug,
                      "NEEDLE[collect_data]  upload_data_flow_controller MAP "
                      "end");
                    return result;
                };

                auto reducer = [](nested_list lhs, nested_list rhs) {
                    /*TODO: remove*/ vlog(
                      cd_log.debug,
                      "NEEDLE[collect_data]  upload_data_flow_controller "
                      "REDUCE start");
                    lhs.insert(
                      lhs.end(),
                      std::make_move_iterator(rhs.begin()),
                      std::make_move_iterator(rhs.end()));
                    /*TODO: remove*/ vlog(
                      cd_log.debug,
                      "NEEDLE[collect_data]  upload_data_flow_controller "
                      "REDUCE end");
                    return std::move(lhs);
                };

                /*TODO: remove*/ vlog(
                  _logger.debug,
                  "[collect_data]  upload_data_flow_controller MAP-REDUCE");
                nested_list collected
                  = co_await _parent->container().map_reduce0(
                    map, nested_list{}, reducer);
                /*TODO: remove*/ vlog(
                  _logger.debug,
                  "[collect_data]  upload_data_flow_controller MAP-REDUCE end, "
                  "collected {} lists",
                  collected.size());

                // Move requests from 'collected' to the 'pending' collection.
                // Note that 'pending' can be used by another fiber so mutual
                // exclusion is needed.
                auto units = co_await ss::get_units(_mutex, 1);

                /*TODO: remove*/ vlog(
                  _logger.debug,
                  "[collect_data]  upload_data_flow_controller acquired units");

                for (write_request_vec& vec : collected) {
                    for (auto& r : vec) {
                        auto ntp = r->ntp;
                        /*TODO: remove*/ vlog(
                          _logger.debug,
                          "[collect_data]  "
                          "upload_data_flow_controller.collect_data "
                          "handling batch for ntp: {}",
                          r->ntp);
                        if (!_pending.contains(r->ntp)) {
                            _pending.insert(
                              std::make_pair(ntp, write_request_vec()));
                        }
                        _shard_state.at(r->shard) = r->index;
                        _bytes_used += r->precise_serialized_size;
                        _pending.at(ntp).push_back(std::move(r));
                        /*TODO: remove*/ vlog(
                          _logger.debug,
                          "[collect_data]  "
                          "upload_data_flow_controller.collect_data "
                          "bytes used: {}",
                          _bytes_used);
                    }
                }
                if (_bytes_used >= _max_bytes) {
                    // We accumulated enough data to start upload without
                    // waiting for the full upload interval.
                    _cv.signal();
                }
                /*TODO: remove*/ vlog(
                  _logger.debug,
                  "[collect_data]upload_data_flow_controller.collect_data iter "
                  "done");
            }
        } catch (...) {
            vlog(
              _logger.error,
              "[collect_data]Failed to collect data {}",
              std::current_exception());
        }
        /*TODO: remove*/ vlog(
          _logger.debug,
          "[collect_data]upload_data_flow_controller.collect_data exit");
        co_return;
    }

    void request_abort(const std::optional<std::exception_ptr>& e) {
        if (e.has_value()) {
            _as.request_abort_ex(*e);
        } else {
            _as.request_abort();
        }
    }

    /// Wait until upload interval elapses or until
    /// enough bytes are accumulated
    ss::future<> wait_for_next_upload() {
        /*TODO: remove*/ vlog(
          _logger.debug,
          "upload_data_flow_controller.wait_for_next_upload {}",
          _upload_interval);
        try {
            co_await _cv.wait<Clock>(Clock::now() + _upload_interval, [this] {
                return _bytes_used >= _max_bytes;
            });
        } catch (const ss::condition_variable_timed_out&) {
        }
        /*TODO: remove*/ vlog(
          _logger.debug,
          "upload_data_flow_controller.wait_for_next_upload exit");
    }

private:
    absl::
      btree_map<model::ntp, chunked_vector<ss::foreign_ptr<write_request_ptr>>>
        _pending;
    std::vector<aggregated_uploader_index> _shard_state;

    aggregated_uploader<Clock>* _parent;
    ss::gate _gate;
    ss::abort_source _as;
    ss::semaphore _mutex{1};
    ss::condition_variable _cv;
    std::chrono::milliseconds _upload_interval;
    size_t _bytes_used{0};
    size_t _max_bytes; // TODO: add config and change dynamically
    basic_retry_chain_logger<Clock> _logger;
};

template<class Clock>
ss::future<> aggregated_uploader<Clock>::bg_controller_loop() {
    vassert(ss::this_shard_id() == 0, "Method can only be invoked on shard 0");
    auto h = _gate.hold();
    auto error_handler = [this](std::exception_ptr ptr, const char* target) {
        if (ssx::is_shutdown_exception(ptr)) {
            vlog(_logger.debug, "{} shutting down", target);
        } else {
            vlog(_logger.error, "{} failed: {}", target, ptr);
        }
    };
    upload_data_flow_controller<Clock> loop_state(
      this, _upload_interval(), max_buffer_size, _rtc);
    auto notification = _as.subscribe(
      [&loop_state](const std::optional<std::exception_ptr>& e) noexcept {
          /*TODO: remove*/ vlog(cd_log.debug, "NEEDLE abort requested");
          loop_state.request_abort(e);
          return loop_state.stop();
      });
    try {
        auto cdf = loop_state.collect_data();
        auto udf = loop_state.schedule_uploads();
        auto [cd_res, ud_res] = co_await ss::when_all(
          std::move(cdf), std::move(udf));
        if (cd_res.failed()) {
            error_handler(cd_res.get_exception(), "Data collection fiber");
        }
        if (ud_res.failed()) {
            error_handler(ud_res.get_exception(), "Data upload fiber");
        }
    } catch (...) {
        error_handler(std::current_exception(), "bg_controller_loop");
    }
}

inline size_t serialized_size(model::reader_data_layout dl) {
    return dl.num_headers * model::packed_record_batch_header_size
           + dl.total_payload_size;
}

template<class Clock>
ss::future<result<model::record_batch_reader>>
aggregated_uploader<Clock>::write_and_debounce(
  model::ntp ntp,
  model::record_batch_reader&& r,
  std::chrono::milliseconds timeout) {
    auto h = _gate.hold();
    auto index = _index++;
    auto layout = maybe_get_data_layout(r);
    if (!layout.has_value()) {
        // We expect to get in-memory record batch reader here so
        // we will be able to estimate the size.
        co_return errc::timeout;
    }
    auto size = serialized_size(layout.value());
    // The write request is stored on the stack of the
    // fiber until the 'response' promise is set. The
    // promise can be set by any fiber that uploaded the
    // data from the write request. The smart pointer is
    // needed so we could use foreign_ptr<> wrapper.
    auto request = ss::make_lw_shared<write_request>(
      std::move(ntp), index, size, std::move(r), timeout);
    _current_size += size;
    auto fut = request->response.get_future();
    _pending.push_back(request);
    auto res = co_await std::move(fut);
    if (res.has_error()) {
        co_return res.error();
    }
    // At this point the request is no longer referenced
    // by any other shard
    auto rdr = model::make_memory_record_batch_reader(std::move(res.value()));
    co_return std::move(rdr);
}

template class aggregated_uploader<ss::lowres_clock>;
template class aggregated_uploader<ss::manual_clock>;

} // namespace cloud_topics
