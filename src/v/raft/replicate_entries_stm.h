#pragma once

#include "raft/consensus.h"
#include "seastarx.h"

namespace raft {

/// A single-shot class. Utility method with state
/// Use with a lw_shared_ptr like so:
/// auto ptr = ss::make_lw_shared<replicate_entries_stm>(..);
/// return ptr->apply()
///            .then([ptr]{
///                 // wait in background.
///                (void)ptr->wait().finally([ptr]{});
///            });
class replicate_entries_stm {
public:
    replicate_entries_stm(
      consensus*, int32_t max_retries, append_entries_request);
    ~replicate_entries_stm();

    /// assumes that this is operating under the consensus::_op_sem lock
    /// returns after majority have responded
    ss::future<result<replicate_result>> apply();

    /// waits for the remaining background futures
    ss::future<> wait();

private:
    struct retry_meta {
        retry_meta(model::node_id n, int32_t ret)
          : retries_left(ret)
          , node(n) {}

        bool is_success() const {
            if (!finished() || !value) {
                return false;
            }
            auto& ref = *value;
            return ref.has_value() && ref.value().success;
        }

        bool is_failure() const {
            return finished() && value && value->has_error();
        }

        bool finished() const { return retries_left <= 0 || value; }

        void set_value(result<append_entries_reply> r) {
            value = std::make_unique<result<append_entries_reply>>(
              std::move(r));
        }

        int32_t retries_left;
        model::node_id node;
        std::unique_ptr<result<append_entries_reply>> value;
    };

    friend std::ostream& operator<<(std::ostream&, const retry_meta&);

    ss::future<std::vector<append_entries_request>> share_request_n(size_t n);

    ss::future<> dispatch_one(retry_meta&);

    ss::future<result<append_entries_reply>>
      do_dispatch_one(model::node_id, append_entries_request);

    ss::future<result<void>> process_replies();

    std::pair<int32_t, int32_t> partition_count() const;

    consensus* _ptr;
    int32_t _max_retries;
    /// we keep a copy around until we finish the retries
    append_entries_request _req;
    ss::semaphore _share_sem;
    // list to all nodes & retries per node
    ss::semaphore _sem;
    std::vector<retry_meta> _replies;
    ss::gate _req_bg;
};

} // namespace raft
