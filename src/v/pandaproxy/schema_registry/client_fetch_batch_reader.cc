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

} // namespace pandaproxy::schema_registry
