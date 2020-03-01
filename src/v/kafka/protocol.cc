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
static ss::logger klog("kafka");

protocol::protocol(
  ss::smp_service_group smp,
  ss::sharded<cluster::metadata_cache>& meta,
  ss::sharded<controller_dispatcher>& ctrl,
  ss::sharded<quota_manager>& quota,
  ss::sharded<kafka::group_router_type>& router,
  ss::sharded<cluster::shard_table>& tbl,
  ss::sharded<cluster::partition_manager>& pm) noexcept
  : _smp_group(smp)
  , _metadata_cache(meta)
  , _cntrl_dispatcher(ctrl)
  , _quota_mgr(quota)
  , _group_router(router)
  , _shard_table(tbl)
  , _partition_manager(pm) {}

ss::future<> protocol::apply(rpc::server::resources rs) {
    return ss::do_until(
      [this, rs] { return rs.conn->input().eof() || rs.abort_requested(); },
      [this, rs] {
          return parse_size(rs.conn->input())
            .then([this, rs](std::optional<size_t> sz) mutable {
                if (!sz) {
                    return ss::make_ready_future<>();
                }
                return parse_header(rs.conn->input())
                  // GCC-9 workaround; r = rs is not really needed
                  .then([this, s = *sz, r = rs](
                          std::optional<request_header> h) mutable {
                      if (!h) {
                          vlog(
                            klog.debug,
                            "could not parse header from client: {}",
                            r.conn->addr);
                          r.probe().header_corrupted();
                          return ss::make_ready_future<>();
                      }
                      return dispatch_method_once(std::move(h.value()), s, r);
                  });
            });
      });
}

ss::future<> protocol::dispatch_method_once(
  request_header hdr, size_t size, rpc::server::resources rs) {
    // Allow for extra copies and bookkeeping
    auto mem_estimate = size * 2 + 8000;
    if (mem_estimate >= (size_t)std::numeric_limits<int32_t>::max()) {
        // TODO: Create error response using the specific API?
        throw std::runtime_error(fmt::format(
          "request too large > 1GB (size: {}; estimate: {})",
          size,
          mem_estimate));
    }
    auto fut = ss::get_units(rs.memory(), mem_estimate);
    if (rs.memory().waiters()) {
        rs.probe().waiting_for_available_memory();
    }
    return fut.then([this, header = std::move(hdr), size, rs](
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
        auto delay = _quota_mgr.local().record_tp_and_throttle(
          header.client_id, size);

        // apply the throttling delay, if any.
        auto throttle_delay = delay.first_violation ? ss::make_ready_future<>()
                                                    : ss::sleep(delay.duration);
        return throttle_delay.then([this,
                                    size,
                                    rs,
                                    header = std::move(header),
                                    units = std::move(units),
                                    delay = std::move(delay)]() mutable {
            auto remaining = size - sizeof(raw_request_header)
                             - header.client_id_buffer.size();
            return read_iobuf_exactly(rs.conn->input(), remaining)
              .then([this,
                     rs,
                     header = std::move(header),
                     units = std::move(units),
                     delay = std::move(delay)](iobuf buf) mutable {
                  auto ctx = request_context(
                    _metadata_cache,
                    _cntrl_dispatcher.local(),
                    std::move(header),
                    std::move(buf),
                    delay.duration,
                    _group_router.local(),
                    _shard_table.local(),
                    _partition_manager);
                  // background process this one full request
                  (void)ss::with_gate(
                    rs.conn_gate(),
                    [this, ctx = std::move(ctx), rs]() mutable {
                        return do_process(std::move(ctx), rs);
                    })
                    .finally([units = std::move(units)] {});
              });
        });
    });
}

ss::future<>
protocol::do_process(request_context ctx, rpc::server::resources rs) {
    auto correlation = ctx.header().correlation;
    sequence_id seq = _seq_idx;
    _seq_idx = _seq_idx + sequence_id(1);
    return kafka::process_request(std::move(ctx), _smp_group)
      .then([this, rs, seq, correlation](response_ptr f) mutable {
          _responses.insert({seq, std::make_pair(correlation, std::move(f))});
          return process_next_response(rs);
      });
}

ss::future<> protocol::process_next_response(rpc::server::resources rs) {
    return ss::repeat([this, rs]() mutable {
        auto it = _responses.find(_next_response);
        if (it == _responses.end()) {
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }
        // found one; increment counter
        _next_response = _next_response + sequence_id(1);
        try {
            correlation_id corr = it->second.first;
            response_ptr r = std::move(it->second.second);
            _responses.erase(it);
            auto msg = response_as_scattered(std::move(r), corr);
            rs.probe().add_bytes_sent(msg.size());
            rs.probe().request_completed();
            return rs.conn->write(std::move(msg)).then([this] {
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
