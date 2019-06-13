#pragma once

#include "filesystem/wal_generated.h"
#include "filesystem/wal_opts.h"
#include "filesystem/wal_requests.h"
#include "filesystem/wal_segment.h"
#include "hashing/xx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <bytell_hash_map.hpp>
#include <functional>

/// \brief size of total keys we store in memory
constexpr static const int32_t kMaxPartialIndexKeysSize = 1 << 25; /*32MB*/

/// \brief class that drives the indexing of a compaction topic
class wal_segment_indexer {
public:
    wal_segment_indexer(
      seastar::sstring index_name,
      const wal_opts& op,
      const seastar::io_priority_class& prio)
      : filename(index_name)
      , wopts(op)
      , priority(prio) {
    }
    ~wal_segment_indexer() = default;
    SMF_DISALLOW_COPY_AND_ASSIGN(wal_segment_indexer);

    /// \brief main method.
    /// called after *every* successful write
    ///
    seastar::future<> index(int64_t offset, const wal_binary_record* r);

    /// \brief close index file
    ///
    seastar::future<> close();
    /// \brief opens index file
    ///
    seastar::future<> open();

    const seastar::sstring filename;
    const wal_opts& wopts;
    const seastar::io_priority_class& priority;

private:
    seastar::future<> flush_index();
    std::pair<wal_segment_index_key_entryT*, bool>
    get_or_create(int64_t offset, const wal_binary_record* r);
    void reset();
    /// \brief hash order followed by string order
    struct key_entry_order {
        bool operator()(
          const std::unique_ptr<wal_segment_index_key_entryT>& lhs,
          const std::unique_ptr<wal_segment_index_key_entryT>& rhs) const {
            if (lhs->hash == rhs->hash) {
                if (lhs->key.size() == rhs->key.size()) {
                    const char* clhs = reinterpret_cast<const char*>(
                      lhs->key.data());
                    const char* crhs = reinterpret_cast<const char*>(
                      rhs->key.data());
                    return std::strncmp(clhs, crhs, lhs->key.size()) < 0;
                }
                return lhs->key.size() < rhs->key.size();
            }
            return lhs->hash < rhs->hash;
        }
    };

public:
    using wal_segment_index_key_entry_set = std::
      set<std::unique_ptr<wal_segment_index_key_entryT>, key_entry_order>;

private:
    bool is_flushed_{true};
    int32_t _size{0};
    /// \brief the xor of all the hashes of all the keys
    /// useful for debugging - it becomes the key of the WAL record
    uint64_t xorwalkey{0};
    int64_t largest_offset_seen_{0};
    int64_t lens_bytes_{0};
    /// \brief map holding all tvhe keys
    wal_segment_index_key_entry_set _data;

    std::unique_ptr<wal_segment> _index = nullptr;
};
