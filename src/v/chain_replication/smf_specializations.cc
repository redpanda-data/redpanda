#include "smf_specializations.h"

#include <numeric>

#include <smf/log.h>

namespace smf {  // customization point

seastar::temporary_buffer<char>
native_table_as_buffer(const v::chains::chain_get_replyT &r) {
  // reserved the size of type
  // To read the default of 1MB of data, there is a *mesured* 80KB of overhead
  // due to struct padding / alignment / etc from the raw size of in memory data
  //
  // size_after_serialization: 1123768
  // user request data:        (1<<20)
  // difference:               75192
  //
  // To recompute, please do this OUT OF BAND. No need for runtime dispatch:
  // LOG_INFO("Reserved: {}, but actual: {}", reserved, bdr.GetSize());
  //
  constexpr static int32_t kOverhead = 80000;
  const uint32_t reserved =
    kOverhead + std::accumulate(r.get->gets.begin(), r.get->gets.end(),
                                uint32_t(0), [](uint32_t acc, auto &n) {
                                  return acc + (sizeof(char) * n->data.size());
                                });
  flatbuffers::FlatBufferBuilder bdr(reserved);
  bdr.ForceDefaults(true);
  bdr.Finish(v::chains::chain_get_reply::Pack(bdr, &r, nullptr));

  DLOG_THROW_IF(bdr.GetSize() > reserved,
                "chains::get reservation failure. Should only allocate once. "
                "Reservation: {}, actual: {}",
                reserved, bdr.GetSize());

  auto mem = bdr.Release();
  auto ptr = reinterpret_cast<char *>(mem.data());
  auto sz = mem.size();
  return seastar::temporary_buffer<char>(
    ptr, sz, seastar::make_object_deleter(std::move(mem)));
}

}  // namespace smf
