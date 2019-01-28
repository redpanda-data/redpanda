#pragma once
#include <list>
#include <numeric>
#include <utility>

#include <bytell_hash_map.hpp>
#include <seastar/core/file.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <smf/log.h>
#include <smf/macros.h>

#include "wal_generated.h"
#include "wal_nstpidx.h"

// TODO(agallego) - rename types to be nstp_{read,write,create}*

// Note that the put/get requests are pointers because they typically come from
// an RPC call. it is the responsibility of the caller to ensure that the
// requests and pointers stay valid throughout the request lifecycle.
//
// In addition, they must be cheap to copy-construct - typically a couple of
// pointer copy.
//

namespace v {
static constexpr std::size_t kWalHeaderSize = sizeof(wal_header);

// create
struct wal_create_request final {
  SMF_DISALLOW_COPY_AND_ASSIGN(wal_create_request);

  wal_create_request(const wal_topic_create_request *ptr, uint32_t core,
                     std::vector<int32_t> assignments)
    : req(THROW_IFNULL(ptr)), runner_core(core),
      partition_assignments(std::move(assignments)) {}

  ~wal_create_request() = default;

  wal_create_request(wal_create_request &&o) noexcept
    : req(std::move(o.req)), runner_core(o.runner_core),
      partition_assignments(std::move(o.partition_assignments)) {}

  wal_create_request &
  operator=(wal_create_request &&o) noexcept {
    if (this != &o) {
      this->~wal_create_request();
      new (this) wal_create_request(std::move(o));
    }
    return *this;
  }

  const wal_topic_create_request *req;
  const uint32_t runner_core;
  const std::vector<int32_t> partition_assignments;

  static bool is_valid(const wal_create_request &r);
};

struct wal_create_reply final {
  SMF_DISALLOW_COPY_AND_ASSIGN(wal_create_reply);

  wal_create_reply() {}
  ~wal_create_reply() = default;
  wal_create_reply(wal_create_reply &&o) noexcept : data(std::move(o.data)) {}

  std::unique_ptr<wal_topic_create_replyT> data =
    std::make_unique<wal_topic_create_replyT>();
};

// reads

struct wal_read_request final {
  wal_read_request(const wal_get_request *ptr, uint32_t core, wal_nstpidx id)
    : req(THROW_IFNULL(ptr)), runner_core(core), idx(id) {}

  wal_read_request(wal_read_request &&o) noexcept
    : req(std::move(o.req)), runner_core(std::move(o.runner_core)),
      idx(std::move(o.idx)) {}

  wal_read_request(const wal_read_request &o)
    : req(o.req), runner_core(o.runner_core), idx(o.idx) {}

  const wal_get_request *req;

  const uint32_t runner_core;
  const wal_nstpidx idx;
  static bool is_valid(const wal_read_request &r);
};

class wal_read_reply final {
 public:
  SMF_DISALLOW_COPY_AND_ASSIGN(wal_read_reply);

  explicit wal_read_reply(int64_t ns, int64_t topic, int32_t partition,
                          int64_t request_offset, bool validate_checksum);

  wal_read_reply(wal_read_reply &&o) noexcept
    : validate_checksum(o.validate_checksum), data_(std::move(o.data_)),
      data_size_(o.data_size_) {}

  wal_read_reply &
  operator=(wal_read_reply &&o) noexcept {
    if (this != &o) {
      this->~wal_read_reply();
      new (this) wal_read_reply(std::move(o));
    }
    return *this;
  }

  const bool validate_checksum;

  /// \brief add record and update size cache
  inline void
  add_record(std::unique_ptr<wal_binary_recordT> r) {
    LOG_THROW_IF(r->data.empty(), "Cannot add empty records to result");
    data_size_ += r->data.size();
    data_.next_offset += r->data.size();
    data_.gets.push_back(std::move(r));
  }

  inline const wal_get_replyT &
  reply() const {
    return data_;
  }
  inline int64_t
  next_epoch() const {
    return data_.next_offset;
  }
  inline void
  set_next_offset(int64_t n) {
    data_.next_offset = n;
  }

  inline int64_t
  on_disk_size() const {
    return data_size_;
  }

  inline void
  set_error(wal_read_errno e) {
    data_.error = e;
  }
  inline bool
  empty() const {
    return data_.gets.empty();
  }
  /// \brief destructive op. releases underlying storage
  ///
  inline std::unique_ptr<wal_get_replyT>
  release() {
    return std::make_unique<wal_get_replyT>(std::move(data_));
  }

 private:
  wal_get_replyT data_;
  int64_t data_size_{0};
};

// writes

struct wal_write_request final {
  SMF_DISALLOW_COPY_AND_ASSIGN(wal_write_request);
  wal_write_request(const wal_put_request *ptr, const uint32_t core,
                    int32_t _partition, const wal_nstpidx &id,
                    std::vector<const wal_binary_record *> &&_data)
    : req(THROW_IFNULL(ptr)), runner_core(core), partition(_partition), idx(id),
      data(std::move(_data)) {}

  wal_write_request(wal_write_request &&o) noexcept
    : req(std::move(o.req)), runner_core(std::move(o.runner_core)),
      partition(std::move(o.partition)), idx(std::move(o.idx)),
      data(std::move(o.data)){};

  wal_write_request &
  operator=(wal_write_request &&o) noexcept {
    if (this != &o) {
      this->~wal_write_request();
      new (this) wal_write_request(std::move(o));
    }
    return *this;
  }

  auto
  begin() const {
    return data.begin();
  }

  auto
  end() const {
    return data.end();
  }

  const wal_put_request *req;
  const uint32_t runner_core;
  const int32_t partition;
  const wal_nstpidx idx;
  std::vector<const wal_binary_record *> data;

  inline static bool
  is_valid(const wal_write_request &r) {
    return !r.data.empty();
  }
};

/// \brief exposes a nested hashing for the underlying map
class wal_write_reply final {
 public:
  using underlying =
    ska::bytell_hash_map<wal_nstpidx,
                         std::unique_ptr<wal_put_reply_partition_tupleT>>;
  using iterator = typename underlying::iterator;
  using const_iterator = typename underlying::const_iterator;

  SMF_DISALLOW_COPY_AND_ASSIGN(wal_write_reply);
  wal_write_reply(int64_t _ns, int64_t _topic) : ns(_ns), topic(_topic) {}
  wal_write_reply(wal_write_reply &&o) noexcept
    : ns(o.ns), topic(o.topic), cache_(std::move(o.cache_)) {}

  /// \brief a write can be for many partitions. set metadata for one of them
  void set_reply_partition_tuple(int64_t ns, int64_t topic, int32_t partition,
                                 int64_t begin_offset, int64_t end_offset);

  iterator
  find(wal_nstpidx p) {
    return cache_.find(p);
  }

  /// \brief drains the internal cache and moves all elements into this
  /// destructive op. don't use this class after
  std::unique_ptr<wal_put_replyT> release();

  iterator
  begin() {
    return cache_.begin();
  }
  const_iterator
  begin() const {
    return cache_.begin();
  }
  const_iterator
  cbegin() const {
    return begin();
  }
  iterator
  end() {
    return cache_.end();
  }
  const_iterator
  end() const {
    return cache_.end();
  }
  const_iterator
  cend() const {
    return end();
  }
  bool
  empty() const {
    return cache_.empty();
  }
  size_t
  size() const {
    return cache_.size();
  }
  const int64_t ns;
  const int64_t topic;
  wal_put_errno err{wal_put_errno_none};

 private:
  underlying cache_{};
};

struct wal_stats_reply {
  using underlying =
    ska::bytell_hash_map<wal_nstpidx, std::unique_ptr<wal_partition_stats>>;
  using iterator = typename underlying::iterator;
  using const_iterator = typename underlying::const_iterator;

  iterator
  find(wal_nstpidx p) {
    return stats.find(p);
  }

  underlying stats;
};

}  // namespace v
