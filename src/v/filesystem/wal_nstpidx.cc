#include "filesystem/wal_nstpidx.h"

wal_nstpidx wal_nstpidx::gen(int64_t ns, int64_t topic, int32_t partition) {
    incremental_xxhash64 inc;
    inc.update((const char*)&ns, sizeof(ns));
    inc.update((const char*)&topic, sizeof(topic));
    inc.update((const char*)&partition, sizeof(partition));
    return wal_nstpidx(inc.digest());
}
wal_nstpidx wal_nstpidx::gen(
  const seastar::sstring& ns,
  const seastar::sstring& topic,
  int32_t partition) {
    return wal_nstpidx::gen(
      xxhash_64(ns.data(), ns.size()),
      xxhash_64(topic.data(), topic.size()),
      partition);
}
