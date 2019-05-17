#include "wal_cold_boot.h"

#include "directory_walker.h"
#include "wal_core_mapping.h"
#include "wal_name_extractor_utils.h"


seastar::future<wal_cold_boot>
wal_cold_boot::filesystem_lcore_index(seastar::sstring top_level_dir) {
  auto x = std::make_unique<wal_cold_boot>(top_level_dir);
  auto ptr = x.get();
  return directory_walker::walk(top_level_dir,
                                [ptr](seastar::directory_entry de) {
                                  return ptr->visit_namespace(std::move(de));
                                })
    .then([x = std::move(x)] {
      return seastar::make_ready_future<wal_cold_boot>(std::move(*x.get()));
    });
}

seastar::future<>
wal_cold_boot::visit_namespace(seastar::directory_entry de) {
  if (!de.type) { return seastar::make_ready_future<>(); }
  if (de.type != seastar::directory_entry_type::directory) {
    return seastar::make_ready_future<>();
  }
  if (!wal_name_extractor_utils::is_valid_ns_topic_name(de.name.c_str())) {
    return seastar::make_ready_future<>();
  }
  // also create if not there via operator[]
  if (!fsidx[de.name].empty()) { return seastar::make_ready_future<>(); }

  seastar::sstring ns = de.name;
  return directory_walker::walk(fmt::format("{}/{}", top_level_dir, ns),
                                [ns, this](seastar::directory_entry de) {
                                  return visit_topic(ns, std::move(de));
                                });
}

seastar::future<>
wal_cold_boot::visit_topic(seastar::sstring ns, seastar::directory_entry de) {
  if (!de.type) { return seastar::make_ready_future<>(); }
  if (de.type != seastar::directory_entry_type::directory) {
    return seastar::make_ready_future<>();
  }
  if (!wal_name_extractor_utils::is_valid_ns_topic_name(de.name.c_str())) {
    return seastar::make_ready_future<>();
  }
  seastar::sstring topic = de.name;
  return directory_walker::walk(
    fmt::format("{}/{}/{}", top_level_dir, ns, topic),
    [this, ns, topic](seastar::directory_entry de) {
      return visit_partition(ns, topic, std::move(de));
    });
}
seastar::future<>
wal_cold_boot::visit_partition(seastar::sstring ns, seastar::sstring topic,
                               seastar::directory_entry de) {
  if (!de.type) { return seastar::make_ready_future<>(); }
  if (de.type != seastar::directory_entry_type::directory) {
    return seastar::make_ready_future<>();
  }
  if (!wal_name_extractor_utils::is_valid_partition_name(de.name)) {
    return seastar::make_ready_future<>();
  }
  int32_t partition =
    wal_name_extractor_utils::wal_partition_dir_extract(de.name);

  auto idx = wal_nstpidx::gen(ns, topic, partition);
  auto core = wal_core_mapping::nstpidx_to_lcore(idx);
  if (seastar::engine().cpu_id() == core) {
    fsidx[ns][topic].insert(partition);
  }
  return seastar::make_ready_future<>();
}
