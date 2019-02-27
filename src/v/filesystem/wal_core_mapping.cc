#include "wal_core_mapping.h"

#include <limits>
#include <type_traits>

#include <fmt/format.h>
#include <seastar/core/reactor.hh>
#include <smf/macros.h>

#include "wal_nstpidx.h"

namespace v {

uint32_t
wal_core_mapping::nstpidx_to_lcore(wal_nstpidx idx) {
  return jump_consistent_hash(idx.id(), seastar::smp::count);
}

/// \brief core assignment for create requests
std::vector<wal_create_request>
wal_core_mapping::core_assignment(const wal_topic_create_request *p) {
  LOG_THROW_IF(p == nullptr, "null create request");
  std::vector<std::vector<int32_t>> assign;
  assign.resize(seastar::smp::count);

  for (int32_t i = 0, max = p->partitions(); i < max; ++i) {
    auto idx = wal_nstpidx::gen(p->ns()->c_str(), p->topic()->c_str(), i);
    assign[nstpidx_to_lcore(idx)].push_back(i);
  }

  std::vector<wal_create_request> retval;
  retval.reserve(assign.size());
  for (auto i = 0u; i < assign.size(); ++i) {
    if (assign[i].empty()) {
      DLOG_TRACE("Core `{}` has no partitions. ns: {}, topic: `{}`", i, p->ns(),
                 p->topic());
      continue;
    }
    retval.push_back(wal_create_request(p, i, std::move(assign[i])));
  }
  return std::move(retval);
}

/** \brief map the request to the lcore that is going to handle the put */
wal_read_request
wal_core_mapping::core_assignment(const wal_get_request *p) {
  LOG_THROW_IF(p == nullptr, "null get request");

  wal_nstpidx idx(p->ns(), p->topic(), p->partition());
  auto runner = wal_core_mapping::nstpidx_to_lcore(idx);
  return wal_read_request(p, runner, idx);
}

std::vector<wal_write_request>
wal_core_mapping::core_assignment(const wal_put_request *p) {
  LOG_THROW_IF(p == nullptr, "null put request");
  struct assigner {
    uint32_t runner_core{0};
    int32_t partition{0};
    std::vector<const wal_binary_record *> records;
  };

  if (SMF_UNLIKELY(p->partition_puts() == nullptr ||
                   p->partition_puts()->size() == 0)) {
    LOG_THROW_IF(p->partition_puts() == nullptr,
                 "null pointer to transactions");
    LOG_THROW_IF(p->partition_puts()->size() == 0,
                 "There are no partitions available");
    return {};
  }
  std::vector<std::unordered_map<uint32_t, assigner>> view;
  view.resize(seastar::smp::count);

  for (auto it : *(p->partition_puts())) {
    wal_nstpidx idx(p->ns(), p->topic(), it->partition());
    auto runner = wal_core_mapping::nstpidx_to_lcore(idx);
    auto &m = view[runner];
    auto &v = m[it->partition()];
    v.partition = it->partition();
    v.runner_core = runner;
    for (std::size_t i = 0; i < it->records()->size(); ++i) {
      v.records.push_back(it->records()->Get(i));
    }
  }
  std::vector<wal_write_request> retval;
  retval.reserve(
    std::accumulate(view.begin(), view.end(), int32_t(0),
                    [](int32_t acc, auto &next) { return acc + next.size(); }));

  for (auto &v : view) {
    for (auto &tup : v) {
      int32_t partition = tup.first;
      wal_nstpidx idx(p->ns(), p->topic(), partition);
      retval.push_back(wal_write_request(p, tup.second.runner_core, partition,
                                         idx, std::move(tup.second.records)));
    }
  }
  return retval;
}

}  // namespace v
