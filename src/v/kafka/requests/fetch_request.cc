#include "kafka/requests/fetch_request.h"

#include "kafka/default_namespace.h"
#include "kafka/errors.h"
#include "kafka/requests/batch_consumer.h"
#include "likely.h"
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
    writer.write_array(
      topics, [version](const topic& t, response_writer& writer) {
          writer.write(t.name());
          writer.write_array(
            t.partitions,
            [version](const partition& p, response_writer& writer) {
                writer.write(p.id);
                writer.write(int64_t(p.fetch_offset));
                writer.write(p.partition_max_bytes);
            });
      });
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
    topics = reader.read_array([](request_reader& reader) {
        return topic{
          .name = model::topic(reader.read_string()),
          .partitions = reader.read_array([](request_reader& reader) {
              return partition{
                .id = model::partition_id(reader.read_int32()),
                .fetch_offset = model::offset(reader.read_int64()),
                .partition_max_bytes = reader.read_int32(),
              };
          }),
        };
    });
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

    if (version >= api_version(1)) {
        writer.write(int32_t(throttle_time.count()));
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
                if (version >= api_version(4)) {
                    writer.write(int64_t(r.last_stable_offset));
                    writer.write_array(
                      r.aborted_transactions,
                      [](
                        const aborted_transaction& t, response_writer& writer) {
                          writer.write(t.producer_id);
                          writer.write(int64_t(t.first_offset));
                      });
                }
                writer.write(std::move(r.record_set));
            });
      });
}

void fetch_response::decode(iobuf buf, api_version version) {
    request_reader reader(std::move(buf));

    if (version >= api_version(1)) {
        throttle_time = std::chrono::milliseconds(reader.read_int32());
    }

    partitions = reader.read_array([version](request_reader& reader) {
        partition p(model::topic(reader.read_string()));
        p.responses = reader.read_array([version](request_reader& reader) {
            return partition_response{
              .id = model::partition_id(reader.read_int32()),
              .error = error_code(reader.read_int16()),
              .high_watermark = model::offset(reader.read_int64()),
              .last_stable_offset = model::offset(reader.read_int64()),
              .aborted_transactions = reader.read_array(
                [](request_reader& reader) {
                    return aborted_transaction{
                      .producer_id = reader.read_int64(),
                      .first_offset = model::offset(reader.read_int64()),
                    };
                }),
              .record_set = reader.read_fragmented_nullable_bytes(),
            };
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
  ss::lw_shared_ptr<cluster::partition> partition, fetch_config config) {
    storage::log_reader_config reader_config(
      config.start_offset,
      model::model_limits<model::offset>::max(),
      0,
      config.max_bytes,
      kafka_read_priority(),
      raft::data_batch_type,
      std::nullopt);

    return partition->make_reader(reader_config)
      .then([timeout = config.timeout](model::record_batch_reader reader) {
          return std::move(reader)
            .consume(kafka_batch_serializer(), timeout)
            .then([](iobuf res) {
                /*
                 * return path will fill in other response fields.
                 */
                return fetch_response::partition_response{
                  .record_set = std::move(res),
                };
            });
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
    auto shard = octx.rctx.shards().shard_for(ntp);
    if (unlikely(!shard)) {
        return make_ready_partition_response_error(
          error_code::unknown_topic_or_partition);
    }

    return octx.rctx.partition_manager().invoke_on(
      *shard,
      octx.ssg,
      [ntp = std::move(ntp),
       config = std::move(config)](cluster::partition_manager& mgr) {
          /*
           * lookup the ntp's partition
           */
          auto partition = mgr.get(ntp);
          if (unlikely(!partition)) {
              return make_ready_partition_response_error(
                error_code::unknown_topic_or_partition);
          }
          if (unlikely(!partition->is_leader())) {
              return make_ready_partition_response_error(
                error_code::not_leader_for_partition);
          }
          return read_from_partition(partition, std::move(config))
            .then([partition](fetch_response::partition_response&& resp) {
                resp.last_stable_offset = partition->committed_offset();
                resp.high_watermark = partition->committed_offset();
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
    return read_from_ntp(octx, std::move(ntp), std::move(config))
      .then_wrapped([&octx, p_id](read_response_type&& f) {
          try {
              auto response = f.get0();
              response.id = p_id;
              response.error = error_code::none;
              octx.add_partition_response(std::move(response));
          } catch (...) {
              /*
               * TODO: this is where we will want to handle any storage specific
               * errors and translate them into kafka response error codes.
               */
              octx.response_error = true;
              octx.add_partition_response(make_partition_response_error(
                error_code::unknown_server_error));
          }
      });
}

/**
 * Process partition fetch requests.
 *
 * Each request is handled serially in the order they appear in the request.
 * There are a couple reasons why we are not **yet** processing these in
 * parallel. First, Kafka expects to some extent that the order of the
 * partitions in the request is an implicit priority on which partitions to read
 * from. This is closely related to the request budget limits specified in terms
 * of maximum bytes and maximum time delay.
 *
 * Once we start processing requests in parallel we'll have to work through
 * various challenges. First, once we dispatch in parallel, we'll need to
 * develop heuristics for dealing with the implicit priority order. We'll also
 * need to develop techniques and heuristics for dealing with budgets since
 * global budgets aren't trivially divisible onto each core when partition
 * requests may produce non-uniform amounts of data.
 *
 * w.r.t. what is needed to parallelize this, there are no data dependencies
 * between partition requests within the fetch request, and so they can be run
 * fully in parallel. The only dependency that exists is that the response must
 * be reassembled such that the responses appear in these order as the
 * partitions in the request.
 */
static ss::future<> fetch_topic_partitions(op_context& octx) {
    return ss::do_for_each(
      octx.request.cbegin(),
      octx.request.cend(),
      [&octx](const fetch_request::const_iterator::value_type& p) {
          /*
           * the next topic-partition to fetch
           */
          auto& topic = *p.topic;
          auto& part = *p.partition;

          if (p.new_topic) {
              octx.start_response_topic(topic);
          }

          // if over budget create placeholder response
          if (
            octx.bytes_left == 0
            || model::timeout_clock::now() > octx.deadline) {
              octx.add_partition_response(
                make_partition_response_error(error_code::message_too_large));
              return ss::make_ready_future<>();
          }

          auto ntp = model::ntp{
            .ns = default_namespace(),
            .tp = model::topic_partition{
              .topic = topic.name,
              .partition = part.id,
            },
          };

          fetch_config config{
            .start_offset = part.fetch_offset,
            .max_bytes = std::min(
              octx.bytes_left, size_t(part.partition_max_bytes)),
            .timeout = octx.deadline,
          };

          return handle_ntp_fetch(octx, std::move(ntp), std::move(config));
      });
}

ss::future<response_ptr>
fetch_api::process(request_context&& rctx, ss::smp_service_group ssg) {
    return ss::do_with(op_context(std::move(rctx), ssg), [](op_context& octx) {
        return fetch_topic_partitions(octx).then([&octx] {
            // build the final response message
            auto resp = std::make_unique<response>();
            octx.response.encode(octx.rctx, *resp.get());

            /*
             * fast out
             */
            if (
              !octx.request.debounce_delay()
              || octx.response_size >= octx.request.min_bytes
              || octx.request.topics.empty() || octx.response_error) {
                return ss::make_ready_future<response_ptr>(std::move(resp));
            }

            /*
             * debounce since not enough bytes were collected.
             *
             * TODO:
             *   - actual debouncing would collect additional data if available,
             *   but this only introduces the delay. the delay is still
             *   important for now so that the client and server do not sit in
             *   tight req/rep loop, and once the client gets an ack it can
             *   retry. the implementation of debouncing should be done or at
             *   least coordinated with the storage layer. Otherwise we'd have
             *   to developer heuristics on top of storage for polling.
             *
             *   - this needs to be abortable sleep coordinated with server
             *   shutdown.
             */
            auto delay = octx.deadline - model::timeout_clock::now();
            return ss::sleep<model::timeout_clock>(delay).then(
              [resp = std::move(resp)]() mutable {
                  return ss::make_ready_future<response_ptr>(std::move(resp));
              });
        });
    });
}

} // namespace kafka
