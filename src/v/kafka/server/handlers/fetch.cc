// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/fetch.h"

#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "config/configuration.h"
#include "kafka/protocol/batch_consumer.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/fetch_session.h"
#include "kafka/server/materialized_partition.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/server/replicated_partition.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_utils.h"
#include "model/timeout_clock.h"
#include "resource_mgmt/io_priority.h"
#include "storage/parser_utils.h"
#include "utils/to_string.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>

#include <boost/range/irange.hpp>
#include <fmt/ostream.h>

#include <chrono>
#include <string_view>

namespace kafka {

/**
 * Make a partition response error.
 */
static fetch_response::partition_response
make_partition_response_error(model::partition_id p_id, error_code error) {
    return fetch_response::partition_response{
      .partition_index = p_id,
      .error_code = error,
      .high_watermark = model::offset(-1),
      .last_stable_offset = model::offset(-1),
      .records = batch_reader(),
    };
}

int32_t
control_record_size(int64_t ts_delta, int32_t offset_delta, const iobuf& key) {
    static constexpr size_t zero_vint_size = vint::vint_size(0);
    return sizeof(model::record_attributes::type) // attributes
           + vint::vint_size(ts_delta)            // timestamp delta
           + vint::vint_size(offset_delta)        // offset_delta
           + vint::vint_size(key.size_bytes())    // key size
           + key.size_bytes()                     // key payload
           + zero_vint_size                       // value size
           + zero_vint_size;                      // headers size
}

iobuf make_control_record_batch_key() {
    iobuf b;
    response_writer w(b);
    /**
     * control record batch schema:
     *   [version, type]
     */
    w.write(model::current_control_record_version);
    w.write(model::control_record_type::unknown);
    return b;
}

/**
 * here we make sure that our internal control batches are correctly adapted
 * for Kafka clients
 */
model::record_batch adapt_fetch_batch(model::record_batch&& batch) {
    // pass through data batches
    if (likely(batch.header().type == raft::data_batch_type)) {
        return std::move(batch);
    }
    /**
     * We set control type flag and remove payload from internal batch types
     */
    batch.header().attrs.set_control_type();
    iobuf records;

    batch.for_each_record([&records](model::record r) {
        auto key = make_control_record_batch_key();
        auto key_size = key.size_bytes();
        auto r_size = control_record_size(
          r.timestamp_delta(), r.offset_delta(), key);
        model::append_record_to_buffer(
          records,
          model::record(
            r_size,
            r.attributes(),
            r.timestamp_delta(),
            r.offset_delta(),
            key_size,
            std::move(key),
            0,
            iobuf{},
            std::vector<model::record_header>{}));
    });
    auto header = batch.header();
    storage::internal::reset_size_checksum_metadata(header, records);
    return model::record_batch(
      header, std::move(records), model::record_batch::tag_ctor_ng{});
}

/**
 * Low-level handler for reading from an ntp. Runs on ntp's home core.
 */
static ss::future<read_result> read_from_partition(
  kafka::partition_proxy part,
  fetch_config config,
  bool foreign_read,
  std::optional<model::timeout_clock::time_point> deadline) {
    auto hw = part.high_watermark();
    auto lso = part.last_stable_offset();
    auto start_o = part.start_offset();
    // if we have no data read, return fast
    if (hw < config.start_offset) {
        co_return read_result(start_o, hw, lso);
    }

    storage::log_reader_config reader_config(
      config.start_offset,
      config.max_offset,
      0,
      config.max_bytes,
      kafka_read_priority(),
      std::nullopt,
      std::nullopt,
      std::nullopt);

    reader_config.strict_max_bytes = config.strict_max_bytes;
    auto rdr = co_await part.make_reader(reader_config);
    auto result = co_await std::move(rdr).consume(
      kafka_batch_serializer(), deadline ? *deadline : model::no_timeout);
    auto data = std::make_unique<iobuf>(std::move(result.data));
    std::vector<cluster::rm_stm::tx_range> aborted_transactions;
    if (result.record_count > 0) {
        aborted_transactions = co_await part.aborted_transactions(
          result.base_offset, result.last_offset);
    }

    if (foreign_read) {
        co_return read_result(
          ss::make_foreign<read_result::data_t>(std::move(data)),
          start_o,
          hw,
          lso,
          std::move(aborted_transactions));
    }
    co_return read_result(
      std::move(data), start_o, hw, lso, std::move(aborted_transactions));
}

std::optional<partition_proxy> make_partition_proxy(
  const ntp_fetch_config& fetch_cfg,
  ss::lw_shared_ptr<cluster::partition> partition,
  cluster::partition_manager& pm) {
    if (!fetch_cfg.is_materialized()) {
        return make_partition_proxy<replicated_partition>(partition);
    }
    if (auto log = pm.log(*fetch_cfg.materialized_ntp); log) {
        return make_partition_proxy<materialized_partition>(*log);
    }
    return std::nullopt;
}

/**
 * Entry point for reading from an ntp. This is executed on NTP home core and
 * build error responses if anything goes wrong.
 */
static ss::future<read_result> do_read_from_ntp(
  cluster::partition_manager& mgr,
  ntp_fetch_config ntp_config,
  bool foreign_read,
  std::optional<model::timeout_clock::time_point> deadline) {
    /*
     * lookup the ntp's partition
     */
    auto partition = mgr.get(ntp_config.ntp);
    if (unlikely(!partition)) {
        return ss::make_ready_future<read_result>(
          error_code::unknown_topic_or_partition);
    }
    if (unlikely(!partition->is_leader())) {
        return ss::make_ready_future<read_result>(
          error_code::not_leader_for_partition);
    }

    auto kafka_partition = make_partition_proxy(ntp_config, partition, mgr);
    if (!kafka_partition) {
        return ss::make_ready_future<read_result>(
          error_code::unknown_topic_or_partition);
    }

    auto high_watermark = partition->high_watermark();

    auto max_offset = high_watermark < model::offset(0) ? model::offset(0)
                                                        : high_watermark;

    if (config::shard_local_cfg().enable_transactions.value()) {
        if (
          ntp_config.cfg.isolation_level
          == model::isolation_level::read_committed) {
            ntp_config.cfg.max_offset = partition->last_stable_offset();
            max_offset = partition->last_stable_offset();
        }
    }

    if (
      ntp_config.cfg.start_offset < partition->start_offset()
      || ntp_config.cfg.start_offset > max_offset) {
        return ss::make_ready_future<read_result>(
          error_code::offset_out_of_range);
    }

    return read_from_partition(
      std::move(*kafka_partition), ntp_config.cfg, foreign_read, deadline);
}

static ntp_fetch_config make_ntp_fetch_config(
  const model::materialized_ntp& m_ntp, const fetch_config& fetch_cfg) {
    if (m_ntp.is_materialized()) {
        return ntp_fetch_config(
          m_ntp.source_ntp(), fetch_cfg, m_ntp.input_ntp());
    }

    return ntp_fetch_config(m_ntp.source_ntp(), fetch_cfg);
}

ss::future<read_result> read_from_ntp(
  cluster::partition_manager& pm,
  const model::materialized_ntp& ntp,
  fetch_config config,
  bool foreign_read,
  std::optional<model::timeout_clock::time_point> deadline) {
    return do_read_from_ntp(
      pm, make_ntp_fetch_config(ntp, config), foreign_read, deadline);
}

static void fill_fetch_responses(
  op_context& octx,
  std::vector<read_result> results,
  std::vector<op_context::response_iterator> responses) {
    auto range = boost::irange<size_t>(0, results.size());
    for (auto idx : range) {
        auto& res = results[idx];
        auto& resp_it = responses[idx];

        // error case
        if (unlikely(res.error != error_code::none)) {
            resp_it.set(
              make_partition_response_error(res.partition, res.error));
            continue;
        }

        model::ntp ntp(
          model::kafka_namespace,
          resp_it->partition->name,
          resp_it->partition_response->partition_index);
        /**
         * Cache fetch metadata
         */
        octx.rctx.get_fetch_metadata_cache().insert_or_assign(
          std::move(ntp),
          res.start_offset,
          res.high_watermark,
          res.last_stable_offset);
        /**
         * Over response budget, we will just waste this read, it will cause
         * data to be stored in the cache so next read is fast
         */
        fetch_response::partition_response resp;
        resp.partition_index = res.partition;
        resp.error_code = error_code::none;
        resp.log_start_offset = res.start_offset;
        resp.high_watermark = res.high_watermark;
        resp.last_stable_offset = res.last_stable_offset;

        if (res.has_data() && octx.bytes_left >= res.data_size_bytes()) {
            /**
             * set aborted transactions if present
             */
            if (!res.aborted_transactions.empty()) {
                std::vector<fetch_response::aborted_transaction> aborted;
                aborted.reserve(res.aborted_transactions.size());
                std::transform(
                  res.aborted_transactions.begin(),
                  res.aborted_transactions.end(),
                  std::back_inserter(aborted),
                  [](cluster::rm_stm::tx_range range) {
                      return fetch_response::aborted_transaction{
                        .producer_id = kafka::producer_id(range.pid.id),
                        .first_offset = range.first};
                  });
                resp.aborted = std::move(aborted);
            }
            resp.records = batch_reader(std::move(res).release_data());
        } else {
            // TODO: add probe to measure how much of read data is discarded
            resp.records = batch_reader();
        }

        resp_it.set(std::move(resp));
    }
}

static ss::future<std::vector<read_result>> do_fetch_ntps_in_parallel(
  cluster::partition_manager& mgr,
  std::vector<ntp_fetch_config>& ntp_fetch_configs,
  bool foreign_read,
  std::optional<model::timeout_clock::time_point> deadline) {
    return ssx::async_transform(
      ntp_fetch_configs, [&mgr, deadline, foreign_read](ntp_fetch_config& cfg) {
          auto p_id = cfg.ntp.tp.partition;
          return do_read_from_ntp(mgr, std::move(cfg), foreign_read, deadline)
            .then([p_id](read_result res) {
                res.partition = p_id;
                return res;
            });
      });
}

static ss::future<std::vector<read_result>> fetch_ntps_in_parallel(
  cluster::partition_manager& mgr,
  std::vector<ntp_fetch_config> ntp_fetch_configs,
  bool foreign_read,
  std::optional<model::timeout_clock::time_point> deadline) {
    return ss::do_with(
      std::move(ntp_fetch_configs),
      [&mgr, deadline, foreign_read](
        std::vector<ntp_fetch_config>& ntp_fetch_configs) {
          return do_fetch_ntps_in_parallel(
            mgr, ntp_fetch_configs, foreign_read, deadline);
      });
}

/**
 * Top-level handler for fetching from single shard. The result is
 * unwrapped and any errors from the storage sub-system are translated
 * into kafka specific response codes. On failure or success the
 * partition response is finalized and placed into its position in the
 * response message.
 */
static ss::future<>
handle_shard_fetch(ss::shard_id shard, op_context& octx, shard_fetch fetch) {
    // if over budget skip the fetch.
    if (octx.bytes_left <= 0) {
        return ss::now();
    }
    // no requests for this shard, do nothing
    if (fetch.requests.empty()) {
        return ss::now();
    }

    bool foreign_read = shard != ss::this_shard_id();

    // dispatch to remote core
    return octx.rctx.partition_manager()
      .invoke_on(
        shard,
        octx.ssg,
        [foreign_read,
         deadline = octx.deadline,
         configs = std::move(fetch.requests)](
          cluster::partition_manager& mgr) mutable {
            return fetch_ntps_in_parallel(
              mgr, std::move(configs), foreign_read, deadline);
        })
      .then([responses = std::move(fetch.responses),
             &octx](std::vector<read_result> results) mutable {
          fill_fetch_responses(octx, std::move(results), std::move(responses));
      });
}

static std::vector<shard_fetch> group_requests_by_shard(op_context& octx) {
    std::vector<shard_fetch> shard_fetches(ss::smp::count);
    auto resp_it = octx.response_begin();
    /**
     * group fetch requests by shard
     */
    octx.for_each_fetch_partition(
      [&resp_it, &octx, &shard_fetches](const fetch_session_partition& fp) {
          // if this is not an initial fetch we are allowed to skip
          // partions that aleready have an error or we have enough data
          if (!octx.initial_fetch) {
              bool has_enough_data
                = !resp_it->partition_response->records->empty()
                  && octx.over_min_bytes();

              if (
                resp_it->partition_response->error_code != error_code::none
                || has_enough_data) {
                  ++resp_it;
                  return;
              }
          }

          if (!octx.rctx.authorized(security::acl_operation::read, fp.topic)) {
              (resp_it).set(make_partition_response_error(
                fp.partition, error_code::topic_authorization_failed));
              ++resp_it;
              return;
          }

          auto ntp = model::ntp(model::kafka_namespace, fp.topic, fp.partition);
          auto materialized_ntp = model::materialized_ntp(std::move(ntp));

          auto shard = octx.rctx.shards().shard_for(
            materialized_ntp.source_ntp());
          if (!shard) {
              // no shard found, set error
              (resp_it).set(make_partition_response_error(
                fp.partition, error_code::unknown_topic_or_partition));
              ++resp_it;
              return;
          }

          fetch_config config{
            .start_offset = fp.fetch_offset,
            .max_offset = model::model_limits<model::offset>::max(),
            .isolation_level = octx.request.data.isolation_level,
            .max_bytes = std::min(octx.bytes_left, size_t(fp.max_bytes)),
            .timeout = octx.deadline.value_or(model::no_timeout),
            .strict_max_bytes = octx.response_size > 0,
          };

          shard_fetches[*shard].push_back(
            make_ntp_fetch_config(materialized_ntp, config), resp_it++);
      });

    return shard_fetches;
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
    std::vector<ss::future<>> fetches;
    fetches.reserve(ss::smp::count);

    ss::shard_id shard = 0;
    for (auto& shard_fetch : group_requests_by_shard(octx)) {
        fetches.push_back(
          handle_shard_fetch(shard++, octx, std::move(shard_fetch)));
    }

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
                  config::shard_local_cfg().fetch_reads_debounce_timeout(),
                  octx.request.data.max_wait_ms));
            });
      });
}

template<>
ss::future<response_ptr>
fetch_handler::handle(request_context rctx, ss::smp_service_group ssg) {
    return ss::do_with(op_context(std::move(rctx), ssg), [](op_context& octx) {
        // top-level error is used for session-level errors
        if (octx.session_ctx.has_error()) {
            octx.response.data.error_code = octx.session_ctx.error();
            return std::move(octx).send_response();
        }
        octx.response.data.error_code = error_code::none;
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
    request.decode(rctx.reader(), rctx.header().version);
    if (likely(!request.data.topics.empty())) {
        response.data.topics.reserve(request.data.topics.size());
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
    bytes_left = std::min(max_size, size_t(request.data.max_bytes));
    session_ctx = rctx.fetch_sessions().maybe_get_session(request);
    create_response_placeholders();
}

// insert and reserve space for a new topic in the response
void op_context::start_response_topic(const fetch_request::topic& topic) {
    auto& p = response.data.topics.emplace_back(
      fetchable_topic_response{.name = topic.name});
    p.partitions.reserve(topic.fetch_partitions.size());
}

void op_context::start_response_partition(const fetch_request::partition& p) {
    response.data.topics.back().partitions.push_back(
      fetch_response::partition_response{
        .partition_index = p.partition_index,
        .error_code = error_code::none,
        .high_watermark = model::offset(-1),
        .last_stable_offset = model::offset(-1),
        .records = batch_reader()});
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
          [this, &last_topic](const fetch_session_partition& fp) {
              if (last_topic != fp.topic) {
                  response.data.topics.emplace_back(
                    fetchable_topic_response{.name = fp.topic});
                  last_topic = fp.topic;
              }
              fetch_response::partition_response p{
                .partition_index = fp.partition,
                .error_code = error_code::none,
                .high_watermark = fp.high_watermark,
                .last_stable_offset = fp.last_stable_offset,
                .records = batch_reader()};

              response.data.topics.back().partitions.push_back(std::move(p));
          });
    }
}

bool update_fetch_partition(
  const fetch_response::partition_response& resp,
  fetch_session_partition& partition) {
    bool include = false;
    if (resp.records && resp.records->size_bytes() > 0) {
        // Partitions with new data are always included in the response.
        include = true;
    }
    if (partition.high_watermark != resp.high_watermark) {
        include = true;
        partition.high_watermark = model::offset(resp.high_watermark);
    }
    if (partition.last_stable_offset != resp.last_stable_offset) {
        include = true;
        partition.last_stable_offset = model::offset(resp.last_stable_offset);
    }
    if (include) {
        return include;
    }
    if (resp.error_code != error_code::none) {
        // Partitions with errors are always included in the response.
        // We also set the cached highWatermark to an invalid offset, -1.
        // This ensures that when the error goes away, we re-send the
        // partition.
        partition.high_watermark = model::offset{-1};
        include = true;
    }
    return include;
}

ss::future<response_ptr> op_context::send_response() && {
    // Sessionless fetch
    if (session_ctx.is_sessionless()) {
        response.data.session_id = invalid_fetch_session_id;
        return rctx.respond(std::move(response));
    }
    // bellow we handle incremental fetches, set response session id
    response.data.session_id = session_ctx.session()->id();
    if (session_ctx.is_full_fetch()) {
        return rctx.respond(std::move(response));
    }

    fetch_response final_response;
    final_response.data.error_code = response.data.error_code;
    final_response.data.session_id = response.data.session_id;
    final_response.data.throttle_time_ms = response.data.throttle_time_ms;

    for (auto it = response.begin(true); it != response.end(); ++it) {
        if (it->is_new_topic) {
            final_response.data.topics.emplace_back(
              fetchable_topic_response{.name = it->partition->name});
            final_response.data.topics.back().partitions.reserve(
              it->partition->partitions.size());
        }

        fetch_response::partition_response r{
          .partition_index = it->partition_response->partition_index,
          .error_code = it->partition_response->error_code,
          .high_watermark = it->partition_response->high_watermark,
          .last_stable_offset = it->partition_response->last_stable_offset,
          .log_start_offset = it->partition_response->log_start_offset,
          .aborted = std::move(it->partition_response->aborted),
          .records = std::move(it->partition_response->records)};

        final_response.data.topics.back().partitions.push_back(std::move(r));
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
      response.partition_index == _it->partition_response->partition_index,
      "Response and current partition ids have to be the same. Current "
      "response {}, update {}",
      _it->partition_response->partition_index,
      response.partition_index);

    if (response.error_code != error_code::none) {
        _ctx->response_error = true;
    }
    auto& current_resp_data = _it->partition_response->records;
    if (current_resp_data) {
        auto sz = current_resp_data->size_bytes();
        _ctx->response_size -= sz;
        _ctx->bytes_left += sz;
    }

    if (response.records) {
        auto sz = response.records->size_bytes();
        _ctx->response_size += sz;
        _ctx->bytes_left -= std::min(_ctx->bytes_left, sz);
    }
    *_it->partition_response = std::move(response);

    // if we are not sessionless update session cache
    if (!_ctx->session_ctx.is_sessionless()) {
        auto& session_partitions = _ctx->session_ctx.session()->partitions();
        auto key = model::topic_partition_view(
          _it->partition->name, _it->partition_response->partition_index);

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
