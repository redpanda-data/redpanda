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
#include "kafka/requests/batch_consumer.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "resource_mgmt/io_priority.h"
#include "utils/to_string.h"

#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

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

std::ostream& operator<<(std::ostream& o, const fetch_request::partition& p) {
    return ss::fmt_print(
      o, "id {} off {} max {}", p.id, p.fetch_offset, p.partition_max_bytes);
}

std::ostream& operator<<(std::ostream& o, const fetch_request::topic& t) {
    return ss::fmt_print(o, "name {} parts {}", t.name, t.partitions);
}

std::ostream& operator<<(std::ostream& o, const fetch_request& r) {
    return ss::fmt_print(
      o,
      "replica {} max_wait_time {} min_bytes {} max_bytes {} isolation {} "
      "topics {}",
      r.replica_id,
      r.max_wait_time,
      r.min_bytes,
      r.max_bytes,
      r.isolation_level,
      r.topics);
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
    return ss::fmt_print(
      o, "producer {} first_off {}", t.producer_id, t.first_offset);
}

std::ostream&
operator<<(std::ostream& o, const fetch_response::partition_response& p) {
    return ss::fmt_print(
      o,
      "id {} err {} high_water {} last_stable_off {} aborted {} "
      "record_set_len "
      "{}",
      p.id,
      p.error,
      p.high_watermark,
      p.last_stable_offset,
      p.aborted_transactions,
      (p.record_set ? p.record_set->size_bytes() : -1));
}

std::ostream& operator<<(std::ostream& o, const fetch_response::partition& p) {
    return ss::fmt_print(o, "name {} responses {}", p.name, p.responses);
}

std::ostream& operator<<(std::ostream& o, const fetch_response& r) {
    return ss::fmt_print(o, "partitions {}", r.partitions);
}

/**
 * Make a partition response error.
 */
static fetch_response::partition_response
make_partition_response_error(error_code error) {
    return fetch_response::partition_response{
      .error = error,
      .high_watermark = model::offset(0),
      .last_stable_offset = model::offset(0),
      .record_set = iobuf(),
    };
}

static ss::future<fetch_response::partition_response>
make_ready_partition_response_error(error_code error) {
    return ss::make_ready_future<fetch_response::partition_response>(
      make_partition_response_error(error));
}

/**
 * Low-level handler for reading from an ntp. Runs on ntp's home core.
 */
static ss::future<fetch_response::partition_response> read_from_partition(
  partition_wrapper pw,
  fetch_config config,
  std::optional<model::timeout_clock::time_point> deadline) {
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

    return pw.make_reader(reader_config, deadline)
      .then([pw, timeout = config.timeout](
              model::record_batch_reader reader) mutable {
          vlog(klog.trace, "fetch reader {}", reader);
          return std::move(reader)
            .consume(kafka_batch_serializer(), timeout)
            .then([pw](kafka_batch_serializer::result res) mutable {
                /*
                 * return path will fill in other response fields.
                 */
                pw.probe().add_records_fetched(res.record_count);
                return fetch_response::partition_response{
                  .error = error_code::none,
                  .record_set = std::move(res.data),
                };
            });
      })
      .handle_exception_type([](const raft::offset_monitor::wait_aborted&) {
          // no data are returned
          return fetch_response::partition_response{
            .error = error_code::none,
            .record_set = iobuf(),
          };
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
    const auto mntpv = model::materialized_ntp(std::move(ntp));
    auto shard = octx.rctx.shards().shard_for(mntpv.source_ntp());

    if (unlikely(!shard)) {
        return make_ready_partition_response_error(
          error_code::unknown_topic_or_partition);
    }

    return octx.rctx.partition_manager().invoke_on(
      *shard,
      octx.ssg,
      [initial_fetch = octx.initial_fetch,
       deadline = octx.deadline,
       mntpv = std::move(mntpv),
       config](cluster::partition_manager& mgr) {
          /*
           * lookup the ntp's partition
           */
          auto partition = mgr.get(mntpv.source_ntp());
          if (unlikely(!partition)) {
              return make_ready_partition_response_error(
                error_code::unknown_topic_or_partition);
          }
          if (unlikely(!partition->is_leader())) {
              return make_ready_partition_response_error(
                error_code::not_leader_for_partition);
          }
          if (mntpv.is_materialized()) {
              if (auto log = mgr.log(mntpv.input_ntp())) {
                  return read_from_partition(
                    partition_wrapper(partition, log), config, std::nullopt);
              } else {
                  return make_ready_partition_response_error(
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
              return ss::make_ready_future<fetch_response::partition_response>(
                fetch_response::partition_response{
                  .error = error_code::offset_out_of_range,
                  .high_watermark = model::offset(-1),
                  .last_stable_offset = model::offset(-1),
                  .log_start_offset = model::offset(-1),
                  .record_set = iobuf(),
                });
          }
          /**
           * Check if we should wait for more data.
           *
           * If request allow waiting for more data we will wait in two
           * scenarios:
           *
           * - previous read didn't meet requested budged
           * - consumer requested read that is beyond high water mark
           */
          bool can_wait = !initial_fetch
                          || config.start_offset > high_watermark;

          return read_from_partition(
                   partition_wrapper(partition),
                   config,
                   can_wait ? deadline : std::nullopt)
            .then([partition](fetch_response::partition_response&& resp) {
                resp.last_stable_offset = partition->last_stable_offset();
                resp.high_watermark = partition->high_watermark();
                return std::move(resp);
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
static ss::future<>
handle_ntp_fetch(op_context& octx, model::ntp ntp, fetch_config config) {
    using read_response_type = ss::future<fetch_response::partition_response>;
    auto p_id = ntp.tp.partition;
    return read_from_ntp(octx, std::move(ntp), config)
      .then_wrapped([&octx, p_id](read_response_type&& f) {
          try {
              auto response = f.get0();
              response.id = p_id;
              octx.set_partition_response(std::move(response));
          } catch (...) {
              /*
               * TODO: this is where we will want to handle any storage
               * specific errors and translate them into kafka response
               * error codes.
               */
              octx.set_partition_response(make_partition_response_error(
                error_code::unknown_server_error));
          }
      });
}

static ss::future<> fetch_topic_partition(
  op_context& octx, const fetch_request::const_iterator::value_type& p) {
    /*
     * the next topic-partition to fetch
     */
    auto& topic = *p.topic;
    auto& part = *p.partition;
    if (p.new_topic && octx.initial_fetch) {
        octx.start_response_topic(topic);
    }

    if (
      model::timeout_clock::now() > octx.deadline.value_or(model::no_timeout)) {
        octx.set_partition_response(
          make_partition_response_error(error_code::request_timed_out));
        return ss::now();
    }

    // if over budget create placeholder response
    if (octx.bytes_left <= 0) {
        octx.set_partition_response(
          make_partition_response_error(error_code::message_too_large));
        return ss::now();
    }
    // if we already have data in response for this partition skip it
    if (!octx.initial_fetch) {
        auto& partition_response = octx.response_iterator->partition_response;

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

    auto ntp = model::ntp(cluster::kafka_namespace, topic.name, part.id);

    fetch_config config{
      .start_offset = part.fetch_offset,
      .max_bytes = std::min(octx.bytes_left, size_t(part.partition_max_bytes)),
      .timeout = octx.deadline.value_or(model::no_timeout),
      .strict_max_bytes = octx.response_size > 0,
    };
    return handle_ntp_fetch(octx, std::move(ntp), config);
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
    return ss::do_for_each(
             octx.request.cbegin(),
             octx.request.cend(),
             [&octx](const fetch_request::const_iterator::value_type& p) {
                 return fetch_topic_partition(octx, p).then([&octx] {
                     if (!octx.initial_fetch) {
                         octx.response_iterator++;
                     }
                 });
             })
      .then([&octx] { octx.reset_context(); });
}

ss::future<response_ptr>
fetch_api::process(request_context&& rctx, ss::smp_service_group ssg) {
    return ss::do_with(op_context(std::move(rctx), ssg), [](op_context& octx) {
        // top-level error is used for session-level errors, but we do
        // not yet implement session management.
        octx.response.error = error_code::none;
        // first fetch, do not wait
        return fetch_topic_partitions(octx)
          .then([&octx] {
              return ss::do_until(
                [&octx] { return octx.should_stop_fetch(); },
                [&octx] { return fetch_topic_partitions(octx); });
          })
          .then(
            [&octx] { return octx.rctx.respond(std::move(octx.response)); });
    });
}

} // namespace kafka
