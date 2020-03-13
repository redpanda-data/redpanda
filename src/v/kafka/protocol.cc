#include "protocol.h"

#include "kafka/protocol_utils.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "utils/utf8.h"
#include "vlog.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

#include <limits>

namespace kafka {
static ss::logger klog("kafka"); // NOLINT
using sequence_id = protocol::sequence_id;

protocol::protocol(
  ss::smp_service_group smp,
  ss::sharded<cluster::metadata_cache>& meta,
  ss::sharded<controller_dispatcher>& ctrl,
  ss::sharded<quota_manager>& quota,
  ss::sharded<kafka::group_router_type>& router,
  ss::sharded<cluster::shard_table>& tbl,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<coordinator_ntp_mapper>& coordinator_mapper) noexcept
  : _smp_group(smp)
  , _metadata_cache(meta)
  , _cntrl_dispatcher(ctrl)
  , _quota_mgr(quota)
  , _group_router(router)
  , _shard_table(tbl)
  , _partition_manager(pm)
  , _coordinator_mapper(coordinator_mapper) {}

ss::future<> protocol::apply(rpc::server::resources rs) {
    auto ctx = ss::make_lw_shared<protocol::connection_context>(
      *this, std::move(rs));
    return ss::do_until(
             [this, ctx] { return ctx->is_finished_parsing(); },
             [this, ctx] { return ctx->process_one_request(); })
      .finally([ctx] {});
}

ss::future<> protocol::connection_context::process_one_request() {
    return parse_size(_rs.conn->input())
      .then([this](std::optional<size_t> sz) mutable {
          if (!sz) {
              return ss::make_ready_future<>();
          }
          return parse_header(_rs.conn->input())
            .then(
              [this, s = sz.value()](std::optional<request_header> h) mutable {
                  if (!h) {
                      vlog(
                        klog.debug,
                        "could not parse header from client: {}",
                        _rs.conn->addr);
                      _rs.probe().header_corrupted();
                      return ss::make_ready_future<>();
                  }
                  return dispatch_method_once(std::move(h.value()), s);
              });
      });
}

bool protocol::connection_context::is_finished_parsing() const {
    return _rs.conn->input().eof() || _rs.abort_requested();
}
ss::future<> protocol::connection_context::dispatch_method_once(
  request_header hdr, size_t size) {
    // Allow for extra copies and bookkeeping
    auto mem_estimate = size * 2 + 8000;
    if (mem_estimate >= (size_t)std::numeric_limits<int32_t>::max()) {
        // TODO: Create error response using the specific API?
        throw std::runtime_error(fmt::format(
          "request too large > 1GB (size: {}; estimate: {})",
          size,
          mem_estimate));
    }
    auto fut = ss::get_units(_rs.memory(), mem_estimate);
    if (_rs.memory().waiters()) {
        _rs.probe().waiting_for_available_memory();
    }
    return fut.then([this, header = std::move(hdr), size](
                      ss::semaphore_units<> units) mutable {
        // update the throughput tracker for this client using the
        // size of the current request and return any computed delay
        // to apply for quota throttling.
        //
        // note that when throttling is first applied the request is
        // allowed to pass through and subsequent requests and
        // delayed. this is a similar strategy used by kafka: the
        // response is important because it allows clients to
        // distinguish throttling delays from real delays. delays
        // applied to subsequent messages allow backpressure to take
        // affect.
        auto delay = _proto._quota_mgr.local().record_tp_and_throttle(
          header.client_id, size);

        // apply the throttling delay, if any.
        auto throttle_delay = delay.first_violation ? ss::make_ready_future<>()
                                                    : ss::sleep(delay.duration);
        return throttle_delay.then([this,
                                    size,
                                    header = std::move(header),
                                    units = std::move(units),
                                    delay = delay]() mutable {
            auto remaining = size - sizeof(raw_request_header)
                             - header.client_id_buffer.size();
            return read_iobuf_exactly(_rs.conn->input(), remaining)
              .then([this,
                     header = std::move(header),
                     units = std::move(units),
                     delay = delay](iobuf buf) mutable {
                  auto rctx = request_context(
                    _proto._metadata_cache,
                    _proto._cntrl_dispatcher.local(),
                    std::move(header),
                    std::move(buf),
                    delay.duration,
                    _proto._group_router.local(),
                    _proto._shard_table.local(),
                    _proto._partition_manager,
                    _proto._coordinator_mapper);
                  // background process this one full request
                  (void)ss::with_gate(
                    _rs.conn_gate(),
                    [this, rctx = std::move(rctx)]() mutable {
                        return do_process(std::move(rctx));
                    })
                    .finally([units = std::move(units)] {});
              });
        });
    });
}

ss::future<> protocol::connection_context::do_process(request_context ctx) {
    const auto correlation = ctx.header().correlation;
    const sequence_id seq = _seq_idx;
    _seq_idx = _seq_idx + sequence_id(1);
    return kafka::process_request(std::move(ctx), _proto._smp_group)
      .then([this, seq, correlation](response_ptr r) mutable {
          auto msg = response_as_scattered(std::move(r), correlation);
          _responses.insert({seq, std::move(msg)});
          return process_next_response();
      });
}

ss::future<> protocol::connection_context::process_next_response() {
    return ss::repeat([this]() mutable {
        auto it = _responses.find(_next_response);
        if (it == _responses.end()) {
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }
        // found one; increment counter
        _next_response = _next_response + sequence_id(1);
        try {
            auto msg = std::move(it->second);
            _responses.erase(it);
            _rs.probe().add_bytes_sent(msg.size());
            _rs.probe().request_completed();
            return _rs.conn->write(std::move(msg)).then([this] {
                return ss::make_ready_future<ss::stop_iteration>(
                  ss::stop_iteration::no);
            });
        } catch (...) {
            vlog(
              klog.debug,
              "Failed to process request: {}",
              std::current_exception());
        }
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    });
}

} // namespace kafka
