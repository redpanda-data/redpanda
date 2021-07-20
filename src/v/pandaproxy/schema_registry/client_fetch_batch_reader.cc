/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/client_fetch_batch_reader.h"

#include "kafka/protocol/exceptions.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "pandaproxy/logger.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/std-coroutine.hh>

namespace pandaproxy::schema_registry {

class client_fetcher final : public model::record_batch_reader::impl {
    using storage_t = model::record_batch_reader::storage_t;

public:
    client_fetcher(
      kafka::client::client& client,
      model::topic_partition tp,
      model::offset first,
      model::offset last)
      : _client{client}
      , _tp{std::move(tp)}
      , _next_offset{first}
      , _last_offset{last}
      , _batch_reader{} {}

    // Implements model::record_batch_reader::impl
    bool is_end_of_stream() const final { return _next_offset >= _last_offset; }

    // Implements model::record_batch_reader::impl
    ss::future<storage_t>
    do_load_slice(model::timeout_clock::time_point t) final {
        if (!_batch_reader || _batch_reader->is_end_of_stream()) {
            vlog(plog.debug, "Schema registry: fetch offset: {}", _next_offset);
            auto res = co_await _client.fetch_partition(
              _tp, _next_offset, 1_MiB, t - model::timeout_clock::now());
            vlog(plog.debug, "Schema registry: fetch result: {}", res);
            vassert(
              res.begin() != res.end() && ++res.begin() == res.end(),
              "Expected exactly one response from client::fetch_partition");
            _batch_reader = std::move(res.begin()->partition_response->records);
        }
        auto ret = co_await _batch_reader->do_load_slice(t);
        using data_t = model::record_batch_reader::data_t;
        vassert(
          std::holds_alternative<data_t>(ret),
          "Expected kafka::batch_reader to hold "
          "model::record_batch_reader::data_t");
        auto& data = std::get<data_t>(ret);
        if (data.empty()) {
            throw kafka::exception(
              kafka::error_code::unknown_server_error, "No records returned");
        }
        _next_offset = ++data.back().last_offset();
        vlog(plog.debug, "Schema registry: next_offset: {}", _next_offset);
        co_return ret;
    }

    // Implements model::record_batch_reader::impl
    void print(std::ostream& os) final {
        os << "{pandaproxy::schema_registry::client_fetcher}";
    }

private:
    kafka::client::client& _client;
    model::topic_partition _tp;
    model::offset _next_offset;
    model::offset _last_offset;
    std::optional<kafka::batch_reader> _batch_reader;
};

model::record_batch_reader make_client_fetch_batch_reader(
  kafka::client::client& client,
  model::topic_partition tp,
  model::offset first,
  model::offset last) {
    return model::make_record_batch_reader<client_fetcher>(
      client, std::move(tp), first, last);
}

class client_consumer_fetcher final : public model::record_batch_reader::impl {
    using storage_t = model::record_batch_reader::storage_t;

public:
    client_consumer_fetcher(
      kafka::client::client& client,
      kafka::group_id g_id,
      kafka::member_id m_id)
      : _client{client}
      , _group_id{std::move(g_id)}
      , _member_id{std::move(m_id)}
      , _batch_reader{} {}

    // Implements model::record_batch_reader::impl
    bool is_end_of_stream() const final { return false; }

    // Implements model::record_batch_reader::impl
    ss::future<storage_t>
    do_load_slice(model::timeout_clock::time_point t) final {
        using data_t = model::record_batch_reader::data_t;
        if (!_batch_reader || _batch_reader->is_end_of_stream()) {
            vlog(plog.debug, "Schema registry: fetch offset: {}", _next_offset);
            auto res = co_await _client.consumer_fetch(
              _group_id,
              _member_id,
              t - model::timeout_clock::now(),
              std::nullopt);
            vlog(plog.debug, "Schema registry: fetch result: {}", res);
            vassert(
              res.begin() == res.end() || ++res.begin() == res.end(),
              "Expected exactly zero or one response from "
              "client::fetch_partition");
            if (res.begin() == res.end()) {
                auto _current_offset = _next_offset - 1;
                vlog(
                  plog.debug,
                  "Schema registry: caught up: {}",
                  _current_offset);
                _caught_up.set_value(_current_offset);
                co_return data_t{};
            }
            _batch_reader = std::move(res.begin()->partition_response->records);
        }
        auto ret = co_await _batch_reader->do_load_slice(t);
        using data_t = model::record_batch_reader::data_t;
        vassert(
          std::holds_alternative<data_t>(ret),
          "Expected kafka::batch_reader to hold "
          "model::record_batch_reader::data_t");
        auto& data = std::get<data_t>(ret);
        if (data.empty()) {
            auto _current_offset = _next_offset - 1;
            vlog(plog.debug, "Schema registry: caught up: {}", _current_offset);
            _caught_up.set_value(_current_offset);
            co_return ret;
        }
        _next_offset = ++data.back().last_offset();
        vlog(plog.debug, "Schema registry: next_offset: {}", _next_offset);
        co_return ret;
    }

    // Implements model::record_batch_reader::impl
    void print(std::ostream& os) final {
        os << "{pandaproxy::schema_registry::client_fetcher}";
    }

    ///\brief This may only be called once. Returns when the topic has been read
    /// to the end
    ss::future<model::offset> caught_up() { return _caught_up.get_future(); }

private:
    kafka::client::client& _client;
    kafka::group_id _group_id;
    kafka::member_id _member_id;
    model::offset _next_offset{model::offset{0}};
    std::optional<kafka::batch_reader> _batch_reader;
    ss::promise<model::offset> _caught_up;
};

ss::future<consumer_batch_reader> make_client_consumer_batch_reader(
  kafka::client::client& client, model::topic_partition tp) {
    auto g_id = kafka::group_id{"schema_registry_group"};
    auto m_id = co_await client.create_consumer(g_id);
    co_await client.subscribe_consumer(
      g_id, m_id, {model::schema_registry_internal_tp.topic});
    auto rdr_impl = std::make_unique<client_consumer_fetcher>(
      client, g_id, m_id);
    auto caught_up = rdr_impl->caught_up();

    co_return consumer_batch_reader{
      .rdr{model::record_batch_reader(std::move(rdr_impl))},
      .caught_up{std::move(caught_up)}};
}

} // namespace pandaproxy::schema_registry
