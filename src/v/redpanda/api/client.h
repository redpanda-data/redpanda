#pragma once

#include "redpanda/api/client_opts.h"
#include "redpanda/api/client_stats.h"
#include "redpanda/api/redpanda.smf.fb.h"

#include <bytell_hash_map.hpp>
#include <string>

namespace api {

class client {
public:
    class txn {
    public:
        SMF_DISALLOW_COPY_AND_ASSIGN(txn);
        explicit txn(
          const client_opts& o,
          client_stats* m,
          int64_t transaction_id,
          std::vector<uint32_t> chain,
          seastar::shared_ptr<redpanda_api_client> c);
        ~txn() = default;
        txn(txn&& o) noexcept;

        /// \brief stage (copy) they key=value on the stack, before `submit()`
        ///
        void stage(
          const char* key,
          int32_t key_size,
          const char* value,
          int32_t value_size);
        /// \brief copy the key and value into a stack (local) stage area
        /// before sending to server
        inline void stage(const char* key_cstring, const char* value_cstring) {
            stage(
              key_cstring,
              std::strlen(key_cstring),
              value_cstring,
              std::strlen(value_cstring));
        }
        /// \brief submits the actual transaction.
        /// invalid after this call
        seastar::future<smf::rpc_recv_typed_context<chains::chain_put_reply>>
        submit();

    private:
        const client_opts& opts;
        seastar::shared_ptr<redpanda_api_client> _rpc;
        smf::rpc_typed_envelope<chains::chain_put_request> _data;
        bool _submitted = false;
        client_stats* _stats;
    };

    explicit client(client_opts o);
    ~client() = default;
    client(client&& o) noexcept
      : opts(std::move(o.opts)) {
    }
    client& operator=(client&& o) noexcept {
        if (this != &o) {
            this->~client();
            new (this) client(std::move(o));
        }
        return *this;
    }
    SMF_DISALLOW_COPY_AND_ASSIGN(client);

    /// \brief opens the connection to the seed server & bootstraps
    ///
    seastar::future<> open(seastar::ipv4_addr seed);

    /// \brief closes the connection of RPC
    ///
    seastar::future<> close();

    /// \brief will load balance reads across all shards (topic,partition)
    ///
    seastar::future<smf::rpc_recv_typed_context<chains::chain_get_reply>>
    consume(int32_t partition_override = -1);

    /// \brief gives a stage area for a chain-replication put
    ///
    txn create_txn();

    /// \brief determine if this client has enabled the histogram measuring
    /// This is expensive! cost about 185KB of memory per histogram,
    /// usually you want to have one per l-core via a thread_local tag on the
    /// var
    inline bool is_histogram_enabled() const {
        return _rpc->is_histogram_enabled();
    }

    /// \brief pointer to this histogram
    ///
    inline seastar::lw_shared_ptr<smf::histogram> get_histogram() {
        return _rpc->get_histogram();
    }

    /// \brief deallocates the histogram
    /// however, since the histogram is an lw_shared_ptr
    /// it will be destroyed only when the last metric is recorded
    /// in case some metric was dispatched for a background fiber
    ///
    inline void disable_histogram_metrics() {
        if (_rpc)
            _rpc->disable_histogram_metrics();
    };

    /// \brief if metrics == nullptr then it will allocate a new histogram
    ///
    inline void enable_histogram_metrics() {
        if (_rpc)
            _rpc->enable_histogram_metrics();
    };

    /// \brief all the environment opts
    ///
    const client_opts opts;

    /// \brief return internal counters
    ///
    inline const client_stats& stats() const {
        return _stats;
    }

private:
    seastar::future<smf::rpc_recv_typed_context<chains::chain_get_reply>>
      consume_from_partition(int32_t);

private:
    seastar::shared_ptr<redpanda_api_client> _rpc;
    uint64_t producer_txn_id_ = 0;
    struct offset_meta_idx {
        int64_t offset{0};
        seastar::semaphore lock{1};
    };
    ska::bytell_hash_map<int32_t, offset_meta_idx> partition_offsets_{};
    client_stats _stats;
};

} // namespace api
