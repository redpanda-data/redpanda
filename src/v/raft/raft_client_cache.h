#pragma once

#include "adt/tagged_ptr.h"
#include "seastarx.h"

#include <seastar/net/socket_defs.hh> // ipv4

#include <smf/random.h>

#include <cstdint>
#include <unordered_map>

// raft
#include "raft.smf.fb.h"

/// \brief use a tagged pointer and use the upper bits to store
/// the exponential backoff bucket. This client will
/// try to reconnect to an ip up to the backoff::max
///
class raft_client_cache final {
public:
    SMF_DISALLOW_COPY_AND_ASSIGN(raft_client_cache);
    using client_t = tagged_ptr<raft::raft_api_client>;
    using opt_client_t = std::optional<raft::raft_api_client*>;
    using underlying = std::unordered_map<uint64_t, client_t>;

    enum class bitflags : uint8_t {
        none = 0,
        circuit_breaker = 1,
        reached_max_retries = 1 << 1,
        // always update this one
        max = reached_max_retries
    };
    enum class backoff : uint8_t {
        none = 0,
        wait_1_sec,
        wait_3_sec,
        wait_5_sec,
        wait_10_sec,
        wait_20_sec,
        wait_30_sec,
        wait_60_sec,
        wait_300_sec,  // 5min
        wait_600_sec,  // 10min
        wait_1800_sec, // 30min
        // always update this one
        max = wait_1800_sec
    };

    raft_client_cache();
    raft_client_cache(raft_client_cache&& o) noexcept;
    ~raft_client_cache();

    /// \brief returns a *connected* client
    future<opt_client_t>
    get_connection(const ipv4_addr& node);

    /// \brief closes all client connections
    future<> close();

private:
    void set_flags(bitflags f, backoff b, const ipv4_addr& node);

    std::tuple<bitflags, backoff, raft::raft_api_client*>
    get_or_create(const ipv4_addr& node);

    future<raft::raft_api_client*>
    attempt_reconnect_with_next_backoff(raft::raft_api_client*, backoff);

    future<>
    stage_next_reconnect(const ipv4_addr& node, backoff bo);

    typename underlying::iterator find(const ipv4_addr& n);

private:
    gate reconnect_gate_;
    smf::random _prng;
    underlying _cache;
};

inline raft_client_cache::bitflags
operator|(raft_client_cache::bitflags s1, raft_client_cache::bitflags s2) {
    return raft_client_cache::bitflags(uint8_t(s1) | uint8_t(s2));
}
inline raft_client_cache::bitflags
operator&(raft_client_cache::bitflags s1, raft_client_cache::bitflags s2) {
    return raft_client_cache::bitflags(uint8_t(s1) & uint8_t(s2));
}
inline raft_client_cache::bitflags operator~(raft_client_cache::bitflags s1) {
    return raft_client_cache::bitflags(~uint8_t(s1));
}

namespace std {
inline ostream& operator<<(ostream& o, raft_client_cache::bitflags f) {
    return o << "raft_client_cache::bitflags{ " << uint8_t(f) << " }";
}
inline ostream& operator<<(ostream& o, raft_client_cache::backoff f) {
    return o << "raft_client_cache::backoff{ " << uint8_t(f) << " }";
}
} // namespace std
