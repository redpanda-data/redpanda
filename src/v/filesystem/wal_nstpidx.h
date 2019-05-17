#pragma once

#include <seastar/core/sstring.hh>

#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"

/// \brief, no worries, this is exactly 8 bytes :)
///
class wal_nstpidx {
 public:
  static wal_nstpidx gen(int64_t ns, int64_t topic, int32_t partition);
  static wal_nstpidx gen(const seastar::sstring &ns,
                         const seastar::sstring &topic, int32_t partition);

 public:
  wal_nstpidx(int64_t ns, int64_t topic, int32_t partition)
    : id_(wal_nstpidx::gen(ns, topic, partition).id()) {}
  explicit wal_nstpidx(uint64_t x) : id_(x) {}
  wal_nstpidx(wal_nstpidx &&o) noexcept : id_(o.id_) {}
  wal_nstpidx(const wal_nstpidx &o) : id_(o.id_) {}
  wal_nstpidx &
  operator=(wal_nstpidx &&o) noexcept {
    if (this != &o) {
      this->~wal_nstpidx();
      new (this) wal_nstpidx(std::move(o));
    }
    return *this;
  }
  ~wal_nstpidx() = default;

  SMF_ALWAYS_INLINE uint64_t
  id() const {
    return id_;
  }

 private:
  friend void swap(wal_nstpidx &x, wal_nstpidx &y);
  uint64_t id_;
};

/// \brief, helper method for maps/etc
SMF_ALWAYS_INLINE void
swap(wal_nstpidx &x, wal_nstpidx &y) {
  std::swap(x.id_, y.id_);
}
SMF_ALWAYS_INLINE bool
operator<(const wal_nstpidx &lhs, const wal_nstpidx &rhs) {
  return lhs.id() < rhs.id();
}
SMF_ALWAYS_INLINE bool
operator==(const wal_nstpidx &lhs, const wal_nstpidx &rhs) {
  return lhs.id() == rhs.id();
}

namespace std {
template <>
struct hash<wal_nstpidx> {
  SMF_ALWAYS_INLINE size_t
  operator()(const wal_nstpidx &x) const {
    return hash<uint64_t>()(x.id());
  }
};
SMF_ALWAYS_INLINE ostream &
operator<<(ostream &o, const wal_nstpidx &idx) {
  o << "wal_nstpidx{ id=" << idx.id() << " }";
  return o;
}
}  // namespace std
