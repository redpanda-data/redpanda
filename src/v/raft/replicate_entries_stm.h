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
        enum class state {
            in_progress,
            success,
            failed_response,
            out_of_retries,
            request_error
        };
        retry_meta(model::node_id n, int32_t ret)
          : retries_left(ret)
          , node(n) {}

        void cancel() { retries_left = 0; }

        void set_value(result<append_entries_reply> r) {
            value = std::make_unique<result<append_entries_reply>>(
              std::move(r));
        }

        bool finished() const { return get_state() != state::in_progress; }

        state get_state() const {
            if (!value) {
                if (retries_left <= 0) {
                    return state::out_of_retries;
                }
                return state::in_progress;
            }
            if (value->has_value()) {
                return value->value().result
                           == append_entries_reply::status::success
                         ? state::success
                         : state::failed_response;
            }

            return state::request_error;
        }

        int32_t retries_left;
        model::node_id node;
        std::unique_ptr<result<append_entries_reply>> value;
    };

    friend std::ostream& operator<<(std::ostream&, const retry_meta&);

    ss::future<append_entries_request> share_request();

    ss::future<> dispatch_one(retry_meta&);
    ss::future<> dispatch_retries(retry_meta&);
    ss::future<> dispatch_single_retry(retry_meta&);
    ss::future<result<append_entries_reply>>
      do_dispatch_one(model::node_id, append_entries_request);

    ss::future<result<append_entries_reply>>
    send_append_entries_request(model::node_id n, append_entries_request req);

    ss::future<result<void>> process_replies();

    std::pair<int32_t, int32_t> partition_count() const;
    // Sets retries left to 0 in order not retry when not required
    void cancel_not_finished();

    consensus* _ptr;
    int32_t _max_retries;
    /// we keep a copy around until we finish the retries
    append_entries_request _req;
    ss::semaphore _share_sem;
    // list to all nodes & retries per node
    ss::semaphore _sem;
    std::vector<retry_meta> _replies;
    ss::gate _req_bg;
    raft_ctx_log _ctxlog;
    ss::promise<> _self_append_done;
};

} // namespace raft
