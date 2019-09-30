#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "rpc/deserialize.h"
#include "rpc/serialize.h"
#include "utils/fragbuf.h"
#include "utils/named_type.h"

#include <seastar/net/socket_defs.hh>

#include <cstdint>

namespace raft {
using clock_type = lowres_clock;
using duration_type = typename clock_type::duration;
using timer_type = timer<clock_type>;

using group_id = named_type<int64_t, struct raft_group_id_type>;

/// special case. it uses underlying type because it is the most used type
/// by using the underlying::type we save 8 continuations per deserialization
struct [[gnu::packed]] protocol_metadata {
    unaligned<group_id::type> group = -1;
    unaligned<model::offset::type> commit_index = 0;
    unaligned<model::term_id::type> term = -1;

    /// \brief used for completeness
    unaligned<model::offset::type> prev_log_index = 0;
    unaligned<model::term_id::type> prev_log_term = -1;
};

struct group_configuration {
    model::node_id node_id;
    std::vector<model::broker> nodes;
    std::vector<model::broker> learners;
};

/// \brief a *collection* of record_batch. In other words
/// and array of array. This is done because the majority of
/// batches will come from the Kafka API which is already batched
/// Main constraint is that _all_ records and batches must be of the same type
class entry final {
public:
    explicit entry(model::record_batch_type t, model::record_batch_reader r)
      : _t(t)
      , _rdr(std::move(r)) {
    }
    ~entry() = default;
    entry(entry&& o) noexcept
      : _t(std::move(o._t))
      , _rdr(std::move(o._rdr)) {
    }
    entry& operator=(entry&& o) noexcept {
        if (this != &o) {
            this->~entry();
            new (this) entry(std::move(o));
        }
        return *this;
    }
    model::record_batch_type entry_type() const {
        return _t;
    }
    model::record_batch_reader& reader() {
        return _rdr;
    }

private:
    model::record_batch_type _t;
    model::record_batch_reader _rdr;
};

struct append_entries_request {
    model::node_id node_id;
    protocol_metadata meta;
    std::vector<entry> entries;
};

struct [[gnu::packed]] append_entries_reply {
    /// \brief callee's term, for the caller to upate itself
    unaligned<model::term_id::type> term = -1;
    /// \brief The recipient's last log index after it applied changes to
    /// the log. This is used to speed up finding the correct value for the
    /// nextIndex with a follower that is far behind a leader
    unaligned<model::offset::type> last_log_index = 0;
    /// \brief did the rpc succeed or not
    unaligned<bool> success = false;
};

/// \brief special use of underlying::type to save continuations on the
/// deserialization step
struct [[gnu::packed]] vote_request {
    unaligned<model::node_id::type> node_id = 0;
    unaligned<group_id::type> group = -1;
    /// \brief current term
    unaligned<model::term_id::type> term = -1;
    /// \brief used to compare completeness
    unaligned<model::offset::type> prev_log_index = 0;
    unaligned<model::term_id::type> prev_log_term = -1;
};

struct [[gnu::packed]] vote_reply {
    /// \brief callee's term, for the caller to upate itself
    unaligned<model::term_id::type> term = -1;

    /// True if the follower granted the candidate it's vote, false otherwise
    unaligned<bool> granted = false;

    /// set to true if the caller's log is as up to date as the recipient's
    /// - extension on raft. see Diego's phd dissertation, section 9.6
    /// - "Preventing disruptions when a server rejoins the cluster"
    unaligned<bool> log_ok = false;
};

} // namespace raft

namespace rpc {
template<>
void serialize(bytes_ostream&, model::broker&&);
template<>
future<model::offset> deserialize(source&);
template<>
future<model::broker> deserialize(source&);
template<>
void serialize(bytes_ostream&, raft::entry&&);
template<>
future<raft::entry> deserialize(source&);
template<>
void serialize(bytes_ostream&, model::record&&);
template<>
future<std::unique_ptr<model::record>> deserialize(source&);

} // namespace rpc
