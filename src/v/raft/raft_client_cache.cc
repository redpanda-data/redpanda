#include "raft_client_cache.h"

#include <chrono>
#include <type_traits>

#include <seastar/core/reactor.hh>  // timer::arm/re-arm here
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>

#include "hashing/xx.h"

namespace v {
using client_t = tagged_ptr<raft::raft_api_client>;
using opt_client_t = std::optional<raft::raft_api_client *>;

static inline uint64_t
hash(const seastar::ipv4_addr &node) {
  std::array<int64_t, 2> arr{node.ip, node.port};
  return xxhash_64(arr);
}

// static helpers

static inline raft_client_cache::backoff
verify_backoff(raft_client_cache::backoff t) {
  DLOG_THROW_IF(t < raft_client_cache::backoff::none ||
                  t > raft_client_cache::backoff::max,
                "flags `{}` is out of range", t);
  return t;
}

static inline raft_client_cache::bitflags
verify_bitflags(raft_client_cache::bitflags t) {
  DLOG_THROW_IF(t < raft_client_cache::bitflags::none ||
                  t > raft_client_cache::bitflags::max,
                "flags `{}` is out of range", t);
  return t;
}

static inline int32_t
to_secs(raft_client_cache::backoff b) {
  using ef = raft_client_cache::backoff;
  switch (b) {
  case ef::none:
    return 0;
  case ef::wait_1_sec:
    return 1;
  case ef::wait_3_sec:
    return 3;
  case ef::wait_5_sec:
    return 5;
  case ef::wait_10_sec:
    return 10;
  case ef::wait_20_sec:
    return 20;
  case ef::wait_30_sec:
    return 30;
  case ef::wait_60_sec:
    return 60;
  default:
    return 60;
  }
}

static inline int32_t
to_ms(smf::random &rng, raft_client_cache::backoff b) {
  // add 128ms of jitter to every step
  // prevents somewhat the thundering herd problem
  uint32_t jitter = rng.next() % 128;
  return jitter + (1000 * to_secs(b));
}

// -- members

raft_client_cache::raft_client_cache() {}
raft_client_cache::raft_client_cache(raft_client_cache &&o) noexcept
  : reconnect_gate_(std::move(o.reconnect_gate_)), prng_(std::move(o.prng_)),
    cache_(std::move(o.cache_)) {}

typename raft_client_cache::underlying::iterator
raft_client_cache::find(const seastar::ipv4_addr &n) {
  return cache_.find(hash(n));
}

std::tuple<raft_client_cache::bitflags, raft_client_cache::backoff,
           raft::raft_api_client *>
raft_client_cache::get_or_create(const seastar::ipv4_addr &node) {
  auto it = find(node);
  if (it == cache_.end()) {
    auto ptr = client_t(new raft::raft_api_client(node), 0);
    auto [it2, sucess] = cache_.emplace(hash(node), std::move(ptr));
    // BUG: gcc8 - shadows variable
    it = it2;
    LOG_THROW_IF(!sucess, "could not emplace tagged ptr for {}", node);
  }
  uint16_t tag = it->second.get_tag();
  auto fail = bitflags(tag & std::numeric_limits<uint8_t>::max());
  auto bf = backoff(tag >> 8);
  return {verify_bitflags(fail), verify_backoff(bf), it->second.get_ptr()};
}
void
raft_client_cache::set_flags(raft_client_cache::bitflags f,
                             raft_client_cache::backoff b,
                             const seastar::ipv4_addr &node) {
  auto failure_int = static_cast<uint8_t>(verify_bitflags(f));
  auto bo_int = static_cast<uint8_t>(verify_backoff(b));
  auto it = find(node);
  if (it != cache_.end()) {
    uint16_t tag = uint16_t(failure_int) | uint16_t(bo_int >> 8);
    it->second.set_tag(tag);
  }
}

seastar::future<>
raft_client_cache::stage_next_reconnect(const seastar::ipv4_addr &node,
                                        raft_client_cache::backoff b) {
  return seastar::with_gate(
           reconnect_gate_,
           [this, node, b] {
             auto ms = std::chrono::milliseconds(to_ms(prng_, b));
             return seastar::sleep(ms).then([this, node, b, ms] {
               if (auto it = find(node); it == cache_.end()) {
                 // got deleted
                 return seastar::make_ready_future<>();
               }
               auto [failure, _, ptr] = get_or_create(node);
               LOG_INFO("Attempting ({}) re-connection after: {} millis",
                        ptr->server_addr, ms.count());
               constexpr uint8_t max_retries =
                 static_cast<uint8_t>(backoff::max);
               uint8_t next_backoff = static_cast<uint8_t>(b) + 1;
               if (next_backoff > max_retries) {
                 LOG_INFO("Giving up on retries for {}", node);
                 set_flags(failure | bitflags::reached_max_retries,
                           backoff::max, ptr->server_addr);
                 return seastar::make_ready_future<>();
               }
               return attempt_reconnect_with_next_backoff(ptr,
                                                          backoff(next_backoff))
                 .discard_result();
             });
           })
    .handle_exception([node](auto eptr) {
      try {
        std::rethrow_exception(eptr);
      } catch (const seastar::gate_closed_exception &e) {
        LOG_INFO("Ignoring '{}'. Reconnect closed for:{}", eptr, node);
        return seastar::make_ready_future<>();
      }
    });
}

seastar::future<raft::raft_api_client *>
raft_client_cache::attempt_reconnect_with_next_backoff(
  raft::raft_api_client *ptr, raft_client_cache::backoff b) {
  if (ptr->is_conn_valid()) {
    return seastar::make_ready_future<raft::raft_api_client *>(ptr);
  }
  return ptr->reconnect()
    .then([this, ptr] {
      set_flags(bitflags::none, backoff::none, ptr->server_addr);
      return seastar::make_ready_future<raft::raft_api_client *>(ptr);
    })
    .handle_exception([this, ptr, b](auto eptr) {
      LOG_INFO("Recovering from '{}'. Staging reconnect to: {}", eptr,
               ptr->server_addr);
      set_flags(bitflags::circuit_breaker, b, ptr->server_addr);
      // dispatch recovery in the background
      stage_next_reconnect(ptr->server_addr, b);
      return seastar::make_ready_future<raft::raft_api_client *>(nullptr);
    });
}

/// \brief returns a *connected* client
seastar::future<opt_client_t>
raft_client_cache::get_connection(const seastar::ipv4_addr &node) {
  auto [failure, bo, ptr] = get_or_create(node);
  if (failure == bitflags::none) {
    return attempt_reconnect_with_next_backoff(ptr, backoff::wait_1_sec)
      .then([](auto client) {
        if (client == nullptr) {
          return seastar::make_ready_future<opt_client_t>();
        }
        return seastar::make_ready_future<opt_client_t>(client);
      });
  }
  if ((failure & bitflags::reached_max_retries) ==
      bitflags::reached_max_retries) {
    // clear the stop bitflags and dispatch retry again
    set_flags(failure & (~bitflags::reached_max_retries), bo, ptr->server_addr);
    LOG_INFO("Starting new retry sequence for {}", ptr->server_addr);
    // launch in background, the new reconnect sequence
    attempt_reconnect_with_next_backoff(ptr, backoff::wait_1_sec)
      .discard_result();
  }
  return seastar::make_ready_future<opt_client_t>();
}

/// \brief closes all client connectiontags
seastar::future<>
raft_client_cache::close() {
  return reconnect_gate_.close().then([=] {
    return seastar::parallel_for_each(
      cache_.begin(), cache_.end(), [this](auto &p) {
        auto ptr = p.second.get_ptr();
        return ptr->stop().finally([this, ptr] {
          set_flags(bitflags::none, backoff::none, ptr->server_addr);
        });
      });
  });
}

raft_client_cache::~raft_client_cache() {
  for (auto &p : cache_) {
    raft::raft_api_client *ptr = p.second.get_ptr();
    DLOG_THROW_IF(p.second.get_tag() != 0,
                  "Fatal logic error. Unclean client shutdown for: {}",
                  ptr->server_addr);
    delete ptr;
  }
};
}  // namespace v
