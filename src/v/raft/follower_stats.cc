#include "raft/follower_stats.h"

namespace raft {
std::ostream& operator<<(std::ostream& o, const follower_stats& s) {
    o << "{followers:" << s._followers.size() << ", [";
    for (auto& f : s) {
        o << f.second;
    }
    return o << "]}";
}

} // namespace raft
