#pragma once
#include <cstdint>
#include <set>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <smf/human_bytes.h>
#include <smf/macros.h>

class wal_nstpidx_repair {
 public:
  struct item {
    int64_t epoch;
    int64_t term;
    int64_t size;
    seastar::sstring filename;
    seastar::lowres_system_clock::time_point modified;
  };
  struct comparator {
    bool
    operator()(const item &x, const item &y) const {
      return x.epoch < y.epoch;
    }
  };
  using set_t = std::set<item, comparator>;

  /// \brief main api
  static seastar::future<set_t> repair(seastar::sstring dir);

  SMF_DISALLOW_COPY_AND_ASSIGN(wal_nstpidx_repair);

 private:
  explicit wal_nstpidx_repair(seastar::sstring workdir) : work_dir(workdir) {}

  /// \brief callback from directory_walker::walk
  seastar::future<> visit(seastar::directory_entry de);

 private:
  const seastar::sstring work_dir;
  set_t files_;
};


namespace std {
inline ostream &
operator<<(ostream &o, const wal_nstpidx_repair::item &i) {
  return o << "wal_nstpidx_repair::item{epoch:" << i.epoch
           << ", size:" << smf::human_bytes(i.size) << "(" << i.size
           << "), filename:" << i.filename << "}";
}
}  // namespace std
