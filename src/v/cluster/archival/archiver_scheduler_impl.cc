

#include "archival/archiver_scheduler_impl.h"

#include "archival/archiver_scheduler_api.h"
#include "archival/logger.h"
#include "archival/types.h"
#include "config/configuration.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace archival {

template<class Clock>
archiver_scheduler<Clock>::archiver_scheduler(
  size_t upload_tput_rate, size_t upload_requests_rate)
  : _shard_tput_limit(upload_tput_rate, "archiver_upload_tput_rate")
  , _put_requests(upload_requests_rate, "archiver_upload_requests_rate")
  , _initial_backoff(config::shard_local_cfg()
                       .cloud_storage_upload_loop_initial_backoff_ms.bind())
  , _max_backoff(config::shard_local_cfg()
                   .cloud_storage_upload_loop_max_backoff_ms.bind()) {}

template<class Clock>
ss::future<result<upload_resource_quota>>
archiver_scheduler<Clock>::maybe_suspend_upload(
  upload_resource_usage arg) noexcept {
    auto holder = _gate.hold();
    auto it = _partitions.find(arg.ntp);
    if (it == _partitions.end()) {
        // create_ntp_state not called for ntp
        vlog(archival_log.error, "Unexpected NTP {}", arg.ntp);
        co_return error_outcome::unexpected_failure;
    }
    auto& v = it->second;
    auto ntp_holder = v->gate.hold();
    if (arg.errc) {
        // The error has occurred. We need to apply exponential backoff
        // to avoid consuming requests.
        v->backoff = v->backoff.value_or(0ms)
                     + v->backoff.value_or(_initial_backoff());
        v->backoff = std::clamp(v->backoff.value(), 0ms, _max_backoff());
        // Backoff is applied in case of any error including no-data error.
        co_await ss::sleep_abortable<Clock>(
          v->backoff.value(), arg.archiver_rtc.get().root_abort_source());
    } else {
        // No error so we can proceed to next upload immediately
        // if rate limits are not reached.
        v->backoff.reset();
    }
    co_await _put_requests.throttle(
      arg.put_requests_used, arg.archiver_rtc.get().root_abort_source());
    co_await _shard_tput_limit.throttle(
      arg.uploaded_bytes, arg.archiver_rtc.get().root_abort_source());

    upload_resource_quota res{
      .requests_quota = _put_requests.available(),
      .upload_size_quota = _shard_tput_limit.available(),
    };
    co_return res;
}

template<class Clock>
ss::future<> archiver_scheduler<Clock>::create_ntp_state(model::ntp ntp) {
    auto it = _partitions.find(ntp);
    vassert(it == _partitions.end(), "Partition {} is already scheduled", ntp);
    _partitions.insert(
      std::make_pair(ntp, std::make_unique<detail::ntp_scheduler_state>()));
    co_return;
}

template<class Clock>
ss::future<> archiver_scheduler<Clock>::dispose_ntp_state(model::ntp ntp) {
    auto it = _partitions.find(ntp);
    vassert(it != _partitions.end(), "Partition {} is not scheduled", ntp);
    co_await it->second->gate.close();
    _partitions.erase(it);
    co_return;
}

template<class Clock>
ss::future<> archiver_scheduler<Clock>::start() {
    co_return;
}

template<class Clock>
ss::future<> archiver_scheduler<Clock>::stop() {
    _shard_tput_limit.shutdown();
    _put_requests.shutdown();
    co_await _gate.close();
}

template class archiver_scheduler<ss::lowres_clock>;
template class archiver_scheduler<ss::manual_clock>;

} // namespace archival
