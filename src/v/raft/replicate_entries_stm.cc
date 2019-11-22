#include "raft/replicate_entries_stm.h"

namespace raft {

future<> replicate_entries_stm::replicate(append_entries_request&&) {
    return make_ready_future<>();
}
} // namespace raft
