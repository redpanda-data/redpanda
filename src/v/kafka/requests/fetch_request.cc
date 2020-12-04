// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/requests/fetch_request.h"

#include "cluster/namespace.h"
#include "cluster/partition_manager.h"
#include "kafka/errors.h"
#include "kafka/fetch_session.h"
#include "kafka/requests/batch_consumer.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "resource_mgmt/io_priority.h"
#include "utils/to_string.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <chrono>
#include <string_view>

namespace kafka {

static constexpr std::chrono::milliseconds fetch_reads_debounce_timeout
  = std::chrono::milliseconds(5);

void fetch_request::encode(response_writer& writer, api_version version) {
    writer.write(replica_id());
    writer.write(int32_t(max_wait_time.count()));
    writer.write(min_bytes);
    if (version >= api_version(3)) {
        writer.write(max_bytes);
    }
    if (version >= api_version(4)) {
        writer.write(isolation_level);
    }
    if (version >= api_version(7)) {
        writer.write(session_id);
        writer.write(session_epoch);
    }
    writer.write_array(
      topics, [version](const topic& t, response_writer& writer) {
          writer.write(t.name());
          writer.write_array(
            t.partitions,
            [version](const partition& p, response_writer& writer) {
                writer.write(p.id);
                if (version >= api_version(9)) {
                    writer.write(p.current_leader_epoch);
                }
                writer.write(int64_t(p.fetch_offset));
                if (version >= api_version(5)) {
                    writer.write(int64_t(p.log_start_offset));
                }
                writer.write(p.partition_max_bytes);
            });
      });
    if (version >= api_version(7)) {
        writer.write_array(
          forgotten_topics,
          [](const forgotten_topic& t, response_writer& writer) {
              writer.write(t.name);
              writer.write_array(
                t.partitions,
                [](int32_t p, response_writer& writer) { writer.write(p); });
          });
    }
}

void fetch_request::decode(request_context& ctx) {
    auto& reader = ctx.reader();
    auto version = ctx.header().version;

    replica_id = model::node_id(reader.read_int32());
    max_wait_time = std::chrono::milliseconds(reader.read_int32());
    min_bytes = reader.read_int32();
    if (version >= api_version(3)) {
        max_bytes = reader.read_int32();
    }
    if (version >= api_version(4)) {
        isolation_level = reader.read_int8();
    }

    if (version >= api_version(7)) {
        session_id = reader.read_int32();
        session_epoch = reader.read_int32();
    }
    topics = reader.read_array([version](request_reader& reader) {
        return topic{
          .name = model::topic(reader.read_string()),
          .partitions = reader.read_array([version](request_reader& reader) {
              partition p;
              p.id = model::partition_id(reader.read_int32());
              if (version >= api_version(9)) {
                  p.current_leader_epoch = reader.read_int32();
              }
              p.fetch_offset = model::offset(reader.read_int64());
              if (version >= api_version(5)) {
                  p.log_start_offset = model::offset(reader.read_int64());
              }
              p.partition_max_bytes = reader.read_int32();
              return p;
          }),
        };
    });
    if (version >= api_version(7)) {
        forgotten_topics = reader.read_array([](request_reader& reader) {
            return forgotten_topic{
              .name = model::topic(reader.read_string()),
              .partitions = reader.read_array(
                [](request_reader& reader) { return reader.read_int32(); }),
            };
        });
    }
}

std::ostream&
operator<<(std::ostream& o, const fetch_request::forgotten_topic& t) {
    fmt::print(o, "{{topic {} partitions {}}}", t.name, t.partitions);
    return o;
}

std::ostream& operator<<(std::ostream& o, const fetch_request::partition& p) {
    fmt::print(
      o,
      "{{id {} off {} max {}}}",
      p.id,
      p.fetch_offset,
      p.partition_max_bytes);
    return o;
}

std::ostream& operator<<(std::ostream& o, const fetch_request::topic& t) {
    fmt::print(o, "{{name {} parts {}}}", t.name, t.partitions);
    return o;
}

std::ostream& operator<<(std::ostream& o, const fetch_request& r) {
    fmt::print(
      o,
      "{{replica {} max_wait_time {} session_id {} session_epoch {} min_bytes "
      "{} max_bytes {} isolation {} topics {} forgotten {}}}",
      r.replica_id,
      r.max_wait_time,
      r.session_id,
      r.session_epoch,
      r.min_bytes,
      r.max_bytes,
      r.isolation_level,
      r.topics,
      r.forgotten_topics);
    return o;
}

void fetch_response::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    auto version = ctx.header().version;

    writer.write(int32_t(throttle_time.count())); // v1

    if (version >= api_version(7)) {
        writer.write(error);
        writer.write(session_id);
    }

    writer.write_array(
      partitions, [version](partition& p, response_writer& writer) {
          writer.write(p.name);
          writer.write_array(
            p.responses,
            [version](partition_response& r, response_writer& writer) {
                writer.write(r.id);
                writer.write(r.error);
                writer.write(int64_t(r.high_watermark));
                writer.write(int64_t(r.last_stable_offset)); // v4
                if (version >= api_version(5)) {
                    writer.write(int64_t(r.log_start_offset));
                }
                writer.write_array( // v4
                  r.aborted_transactions,
                  [](const aborted_transaction& t, response_writer& writer) {
                      writer.write(t.producer_id);
                      writer.write(int64_t(t.first_offset));
                  });
                writer.write(std::move(r.record_set));
            });
      });
}

void fetch_response::decode(iobuf buf, api_version version) {
    request_reader reader(std::move(buf));

    throttle_time = std::chrono::milliseconds(reader.read_int32()); // v1

    error = version >= api_version(7) ? error_code(reader.read_int16())
                                      : kafka::error_code::none;

    session_id = version >= api_version(7) ? reader.read_int32() : 0;

    partitions = reader.read_array([version](request_reader& reader) {
        partition p(model::topic(reader.read_string()));
        p.responses = reader.read_array([version](request_reader& reader) {
            return partition_response{
              .id = model::partition_id(reader.read_int32()),
              .error = error_code(reader.read_int16()),
              .high_watermark = model::offset(reader.read_int64()),
              .last_stable_offset = model::offset(reader.read_int64()), // v4
              .log_start_offset = model::offset(
                version >= api_version(5) ? reader.read_int64() : -1),
              .aborted_transactions = reader.read_array( // v4
                [](request_reader& reader) {
                    return aborted_transaction{
                      .producer_id = reader.read_int64(),
                      .first_offset = model::offset(reader.read_int64()),
                    };
                }),
              .record_set = reader.read_fragmented_nullable_bytes()};
        });
        return p;
    });
}

std::ostream&
operator<<(std::ostream& o, const fetch_response::aborted_transaction& t) {
    fmt::print(
      o, "{{producer {} first_off {}}}", t.producer_id, t.first_offset);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const fetch_response::partition_response& p) {
    fmt::print(
      o,
      "{{id {} err {} high_water {} last_stable_off {} aborted {} "
      "record_set_len {}}}",
      p.id,
      p.error,
      p.high_watermark,
      p.last_stable_offset,
      p.aborted_transactions,
      (p.record_set ? p.record_set->size_bytes() : -1));
    return o;
}

std::ostream& operator<<(std::ostream& o, const fetch_response::partition& p) {
    fmt::print(o, "{{name {} responses {}}}", p.name, p.responses);
    return o;
}

std::ostream& operator<<(std::ostream& o, const fetch_response& r) {
    fmt::print(
      o,
      "{{session_id {} error {} partitions {}}}",
      r.session_id,
      r.error,
      r.partitions);
    return o;
}

/**
 * Make a partition response error.
 */
static fetch_response::partition_response
make_partition_response_error(model::partition_id p_id, error_code error) {
    return fetch_response::partition_response{
      .id = p_id,
      .error = error,
      .high_watermark = model::offset(-1),
      .last_stable_offset = model::offset(-1),
      .record_set = iobuf(),
    };
}

static ss::future<fetch_response::partition_response>
make_ready_partition_response_error(error_code error) {
    return ss::make_ready_future<fetch_response::partition_response>(
      // partiton id will be modified when assembling the response further
      make_partition_response_error(model::partition_id(-1), error));
}

/**
 * Low-level handler for reading from an ntp. Runs on ntp's home core.
 */
static ss::future<read_result> read_from_partition(
  partition_wrapper pw,
  fetch_config config,
  bool foreign_read,
  std::optional<model::timeout_clock::time_point> deadline) {
    auto hw = pw.high_watermark();
    auto lso = pw.last_stable_offset();
    // if we have no data read, return fast
    if (hw < config.start_offset) {
        return ss::make_ready_future<read_result>(hw, lso);
    }

    storage::log_reader_config reader_config(
      config.start_offset,
      model::model_limits<model::offset>::max(),
      0,
      config.max_bytes,
      kafka_read_priority(),
      raft::data_batch_type,
      std::nullopt,
      std::nullopt);

    reader_config.strict_max_bytes = config.strict_max_bytes;
    return pw.make_reader(reader_config)
      .then([hw, lso, foreign_read, deadline](model::record_batch_reader rdr) {
          // if we are on remote core, we MUST use foreign record batch reader.
          if (foreign_read) {
              return model::consume_reader_to_memory(
                       std::move(rdr), deadline.value_or(model::no_timeout))
                .then([hw, lso](ss::circular_buffer<model::record_batch> data) {
                    return read_result(
                      model::make_foreign_memory_record_batch_reader(
                        std::move(data)),
                      hw,
                      lso);
                });
          }
          return ss::make_ready_future<read_result>(
            read_result(std::move(rdr), hw, lso));
      });
}

/**
 * Entry point for reading from an ntp. This will forward the request to
 * the ntp's home core and build error responses if anything goes wrong.
 */
ss::future<fetch_response::partition_response>
read_from_ntp(op_context& octx, model::ntp ntp, fetch_config config) {
    /*
     * lookup the home shard for this ntp. the caller should check for
     * the tp in the metadata cache so that this condition is unlikely
     * to pass.
     */
    auto mntpv = model::materialized_ntp(std::move(ntp));
    auto shard = octx.rctx.shards().shard_for(mntpv.source_ntp());

    if (unlikely(!shard)) {
        return make_ready_partition_response_error(
          error_code::unknown_topic_or_partition);
    }
    bool foreign_read = shard != ss::this_shard_id();

    return octx.rctx.partition_manager()
      .invoke_on(
        *shard,
        octx.ssg,
        [mntpv = std::move(mntpv),
         foreign_read,
         config,
         deadline = octx.deadline](cluster::partition_manager& mgr) {
            /*
             * lookup the ntp's partition
             */
            auto partition = mgr.get(mntpv.source_ntp());
            if (unlikely(!partition)) {
                return ss::make_ready_future<read_result>(
                  error_code::unknown_topic_or_partition);
            }
            if (unlikely(!partition->is_leader())) {
                return ss::make_ready_future<read_result>(
                  error_code::not_leader_for_partition);
            }
            if (mntpv.is_materialized()) {
                if (auto log = mgr.log(mntpv.input_ntp())) {
                    return read_from_partition(
                      partition_wrapper(partition, log),
                      config,
                      foreign_read,
                      deadline);
                } else {
                    return ss::make_ready_future<read_result>(
                      error_code::unknown_topic_or_partition);
                }
            }

            auto high_watermark = partition->high_watermark();
            auto max_offset = high_watermark < model::offset(0)
                                ? model::offset(0)
                                : high_watermark + model::offset(1);
            if (
              config.start_offset < partition->start_offset()
              || config.start_offset > max_offset) {
                return ss::make_ready_future<read_result>(
                  error_code::offset_out_of_range);
            }

            return read_from_partition(
              partition_wrapper(partition), config, foreign_read, deadline);
        })
      .then([timeout = config.timeout](read_result res) mutable {
          vlog(klog.trace, "fetch reader {}", res.reader);
          // error case
          if (!res.reader) {
              return make_ready_partition_response_error(res.error);
          }
          return std::move(*res.reader)
            .consume(kafka_batch_serializer(), timeout)
            .then([](kafka_batch_serializer::result res) mutable {
                /*
                 * return path will fill in other response fields.
                 */
                return fetch_response::partition_response{
                  .error = error_code::none,
                  .record_set = std::move(res.data),
                };
            });
      });
}

/**
 * Top-level handler for fetching from a topic-partition. The result is
 * unwrapped and any errors from the storage sub-system are translated
 * into kafka specific response codes. On failure or success the
 * partition response is finalized and placed into its position in the
 * response message.
 */
static ss::future<> handle_ntp_fetch(
  op_context& octx,
  model::ntp ntp,
  fetch_config config,
  op_context::response_iterator resp_it) {
    using read_response_type = ss::future<fetch_response::partition_response>;
    auto p_id = ntp.tp.partition;
    return read_from_ntp(octx, std::move(ntp), config)
      .then_wrapped([p_id, resp_it](read_response_type&& f) mutable {
          try {
              auto response = f.get0();
              response.id = p_id;
              resp_it.set(std::move(response));
          } catch (...) {
              /*
               * TODO: this is where we will want to handle any storage
               * specific errors and translate them into kafka response
               * error codes.
               */
              resp_it.set(make_partition_response_error(
                p_id, error_code::unknown_server_error));
          }
      });
}

static ss::future<> fetch_topic_partition(
  op_context& octx,
  const fetch_partition& fp,
  op_context::response_iterator resp_it) {
    /*
     * the next topic-partition to fetch
     */
    auto& topic = fp.topic;

    // if over budget skip the fetch.
    if (octx.bytes_left <= 0) {
        return ss::now();
    }

    // if we already have data in response for this partition skip it
    if (!octx.initial_fetch) {
        auto& partition_response = resp_it->partition_response;

        /**
         * do not try to fetch again if partition response already contains an
         * error or it is not empty and we are already over the min bytes
         * threshold.
         */
        if (
          partition_response->has_error()
          || (!partition_response->record_set->empty() && octx.over_min_bytes())) {
            return ss::now();
        }
    }
    auto ntp = model::ntp(cluster::kafka_namespace, topic, fp.partition);

    fetch_config config{
      .start_offset = fp.fetch_offset,
      .max_bytes = std::min(octx.bytes_left, size_t(fp.max_bytes)),
      .timeout = octx.deadline.value_or(model::no_timeout),
      .strict_max_bytes = octx.response_size > 0,
    };
    return handle_ntp_fetch(octx, std::move(ntp), config, resp_it);
}

/**
 * Process partition fetch requests.
 *
 * Each request is handled serially in the order they appear in the request.
 * There are a couple reasons why we are not **yet** processing these in
 * parallel. First, Kafka expects to some extent that the order of the
 * partitions in the request is an implicit priority on which partitions to
 * read from. This is closely related to the request budget limits specified
 * in terms of maximum bytes and maximum time delay.
 *
 * Once we start processing requests in parallel we'll have to work through
 * various challenges. First, once we dispatch in parallel, we'll need to
 * develop heuristics for dealing with the implicit priority order. We'll
 * also need to develop techniques and heuristics for dealing with budgets
 * since global budgets aren't trivially divisible onto each core when
 * partition requests may produce non-uniform amounts of data.
 *
 * w.r.t. what is needed to parallelize this, there are no data dependencies
 * between partition requests within the fetch request, and so they can be
 * run fully in parallel. The only dependency that exists is that the
 * response must be reassembled such that the responses appear in these
 * order as the partitions in the request.
 */

static ss::future<> fetch_topic_partitions(op_context& octx) {
    auto resp_it = octx.response_begin();
    std::vector<ss::future<>> fetches;

    octx.for_each_fetch_partition(
      [&resp_it, &octx, &fetches](fetch_partition p) {
          return fetches.push_back(fetch_topic_partition(octx, p, resp_it++));
      });

    return ss::do_with(
      std::move(fetches), [&octx](std::vector<ss::future<>>& fetches) {
          return ss::when_all_succeed(fetches.begin(), fetches.end())
            .then([&octx] {
                if (octx.should_stop_fetch()) {
                    return ss::now();
                }
                octx.reset_context();
                // debounce next read retry
                return ss::sleep(std::min(
                  fetch_reads_debounce_timeout, octx.request.max_wait_time));
            });
      });
}

ss::future<response_ptr>
fetch_api::process(request_context&& rctx, ss::smp_service_group ssg) {
    return ss::do_with(op_context(std::move(rctx), ssg), [](op_context& octx) {
        // top-level error is used for session-level errors
        if (octx.session_ctx.has_error()) {
            octx.response.error = octx.session_ctx.error();
            return std::move(octx).send_response();
        }
        octx.response.error = error_code::none;
        // first fetch, do not wait
        return fetch_topic_partitions(octx)
          .then([&octx] {
              return ss::do_until(
                [&octx] { return octx.should_stop_fetch(); },
                [&octx] { return fetch_topic_partitions(octx); });
          })
          .then([&octx] { return std::move(octx).send_response(); });
    });
}

void op_context::reset_context() { initial_fetch = false; }

// decode request and initialize budgets
op_context::op_context(request_context&& ctx, ss::smp_service_group ssg)
  : rctx(std::move(ctx))
  , ssg(ssg)
  , response_size(0)
  , response_error(false) {
    /*
     * decode request and prepare the inital response
     */
    request.decode(rctx);
    if (likely(!request.topics.empty())) {
        response.partitions.reserve(request.topics.size());
    }

    if (auto delay = request.debounce_delay(); delay) {
        deadline = model::timeout_clock::now() + delay.value();
    }

    /*
     * TODO: max size is multifaceted. it needs to be absolute, but also
     * integrate with other resource contraints that are dynamic within the
     * kafka server itself.
     */
    static constexpr size_t max_size = 128_KiB;
    bytes_left = std::min(max_size, size_t(request.max_bytes));
    session_ctx = rctx.fetch_sessions().maybe_get_session(request);
    create_response_placeholders();
}

// insert and reserve space for a new topic in the response
void op_context::start_response_topic(const fetch_request::topic& topic) {
    auto& p = response.partitions.emplace_back(topic.name);
    p.responses.reserve(topic.partitions.size());
}

void op_context::start_response_partition(const fetch_request::partition& p) {
    response.partitions.back().responses.push_back(
      fetch_response::partition_response{
        .id = p.id,
        .error = error_code::none,
        .high_watermark = model::offset(-1),
        .last_stable_offset = model::offset(-1),
        .record_set = iobuf()});
}

void op_context::create_response_placeholders() {
    if (session_ctx.is_sessionless() || session_ctx.is_full_fetch()) {
        std::for_each(
          request.cbegin(),
          request.cend(),
          [this](const fetch_request::const_iterator::value_type& v) {
              if (v.new_topic) {
                  start_response_topic(*v.topic);
              }
              start_response_partition(*v.partition);
          });
    } else {
        model::topic last_topic;
        std::for_each(
          session_ctx.session()->partitions().cbegin_insertion_order(),
          session_ctx.session()->partitions().cend_insertion_order(),
          [this, &last_topic](const fetch_partition& fp) {
              if (last_topic != fp.topic) {
                  response.partitions.emplace_back(fp.topic);
                  last_topic = fp.topic;
              }
              fetch_response::partition_response p{
                .id = fp.partition,
                .error = error_code::none,
                .high_watermark = fp.high_watermark,
                .last_stable_offset = fp.high_watermark,
                .record_set = iobuf()};

              response.partitions.back().responses.push_back(std::move(p));
          });
    }
}

bool update_fetch_partition(
  const fetch_response::partition_response& resp, fetch_partition& partition) {
    bool include = false;
    if (resp.record_set && resp.record_set->size_bytes() > 0) {
        // Partitions with new data are always included in the response.
        include = true;
    }
    if (partition.high_watermark != resp.high_watermark) {
        partition.high_watermark = model::offset(resp.high_watermark);
        return true;
    }
    if (resp.error != error_code::none) {
        // Partitions with errors are always included in the response.
        // We also set the cached highWatermark to an invalid offset, -1.
        // This ensures that when the error goes away, we re-send the partition.
        partition.high_watermark = model::offset{-1};
        include = true;
    }
    return include;
}

ss::future<response_ptr> op_context::send_response() && {
    // Sessionless fetch
    if (session_ctx.is_sessionless()) {
        response.session_id = invalid_fetch_session_id;
        return rctx.respond(std::move(response));
    }
    // bellow we handle incremental fetches, set response session id
    response.session_id = session_ctx.session()->id();
    if (session_ctx.is_full_fetch()) {
        return rctx.respond(std::move(response));
    }

    fetch_response final_response;
    final_response.error = response.error;
    final_response.session_id = response.session_id;
    final_response.throttle_time = response.throttle_time;

    for (auto it = response.begin(true); it != response.end(); ++it) {
        if (it->is_new_topic) {
            final_response.partitions.emplace_back(it->partition->name);
            final_response.partitions.back().responses.reserve(
              it->partition->responses.size());
        }

        fetch_response::partition_response r{
          .id = it->partition_response->id,
          .error = it->partition_response->error,
          .high_watermark = it->partition_response->high_watermark,
          .last_stable_offset = it->partition_response->last_stable_offset,
          .log_start_offset = it->partition_response->log_start_offset,
          .aborted_transactions = std::move(
            it->partition_response->aborted_transactions),
          .record_set = std::move(it->partition_response->record_set)};

        final_response.partitions.back().responses.push_back(std::move(r));
    }

    return rctx.respond(std::move(final_response));
}

op_context::response_iterator::response_iterator(
  fetch_response::iterator it, op_context* ctx)
  : _it(it)
  , _ctx(ctx) {}

void op_context::response_iterator::set(
  fetch_response::partition_response&& response) {
    vassert(
      response.id == _it->partition_response->id,
      "Response and current partition ids have to be the same. Current "
      "response {}, update {}",
      _it->partition_response->id,
      response.id);

    if (response.has_error()) {
        _ctx->response_error = true;
    }
    auto& current_resp_data = _it->partition_response->record_set;
    if (current_resp_data) {
        auto sz = current_resp_data->size_bytes();
        _ctx->response_size -= sz;
        _ctx->bytes_left += sz;
    }

    if (response.record_set) {
        auto sz = response.record_set->size_bytes();
        _ctx->response_size += sz;
        _ctx->bytes_left -= std::min(_ctx->bytes_left, sz);
    }
    *_it->partition_response = std::move(response);

    // if we are not sessionless update session cache
    if (!_ctx->session_ctx.is_sessionless()) {
        auto& session_partitions = _ctx->session_ctx.session()->partitions();
        auto key = model::topic_partition_view(
          _it->partition->name, _it->partition_response->id);

        if (auto it = session_partitions.find(key);
            it != session_partitions.end()) {
            auto has_to_be_included = update_fetch_partition(
              *_it->partition_response, it->second->partition);

            _it->partition_response->has_to_be_included = has_to_be_included;
        }
    }
}

op_context::response_iterator& op_context::response_iterator::operator++() {
    _it++;
    return *this;
}

const op_context::response_iterator
op_context::response_iterator::operator++(int) {
    response_iterator tmp = *this;
    ++(*this);
    return tmp;
}

bool op_context::response_iterator::operator==(
  const response_iterator& o) const noexcept {
    return _it == o._it;
}

bool op_context::response_iterator::operator!=(
  const response_iterator& o) const noexcept {
    return !(*this == o);
}

} // namespace kafka
