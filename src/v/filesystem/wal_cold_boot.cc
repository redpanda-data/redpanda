#include "filesystem/wal_cold_boot.h"

#include "filesystem/wal_core_mapping.h"
#include "filesystem/wal_name_extractor_utils.h"
#include "utils/directory_walker.h"

#include <seastar/core/reactor.hh>

future<wal_cold_boot>
wal_cold_boot::filesystem_lcore_index(sstring top_level_dir) {
    auto x = std::make_unique<wal_cold_boot>(top_level_dir);
    auto ptr = x.get();
    return directory_walker::walk(
             top_level_dir,
             [ptr](directory_entry de) {
                 return ptr->visit_namespace(std::move(de));
             })
      .then([x = std::move(x)] {
          return make_ready_future<wal_cold_boot>(std::move(*x.get()));
      });
}

future<> wal_cold_boot::visit_namespace(directory_entry de) {
    if (!de.type) {
        return make_ready_future<>();
    }
    if (de.type != directory_entry_type::directory) {
        return make_ready_future<>();
    }
    if (!wal_name_extractor_utils::is_valid_ns_topic_name(de.name.c_str())) {
        return make_ready_future<>();
    }
    // also create if not there via operator[]
    if (!fsidx[de.name].empty()) {
        return make_ready_future<>();
    }

    sstring ns = de.name;
    return directory_walker::walk(
      fmt::format("{}/{}", top_level_dir, ns),
      [ns, this](directory_entry de) {
          return visit_topic(ns, std::move(de));
      });
}

future<>
wal_cold_boot::visit_topic(sstring ns, directory_entry de) {
    if (!de.type) {
        return make_ready_future<>();
    }
    if (de.type != directory_entry_type::directory) {
        return make_ready_future<>();
    }
    if (!wal_name_extractor_utils::is_valid_ns_topic_name(de.name.c_str())) {
        return make_ready_future<>();
    }
    sstring topic = de.name;
    return directory_walker::walk(
      fmt::format("{}/{}/{}", top_level_dir, ns, topic),
      [this, ns, topic](directory_entry de) {
          return visit_partition(ns, topic, std::move(de));
      });
}
future<> wal_cold_boot::visit_partition(
  sstring ns, sstring topic, directory_entry de) {
    if (!de.type) {
        return make_ready_future<>();
    }
    if (de.type != directory_entry_type::directory) {
        return make_ready_future<>();
    }
    if (!wal_name_extractor_utils::is_valid_partition_name(de.name)) {
        return make_ready_future<>();
    }
    int32_t partition = wal_name_extractor_utils::wal_partition_dir_extract(
      de.name);

    auto idx = wal_nstpidx::gen(ns, topic, partition);
    auto core = wal_core_mapping::nstpidx_to_lcore(idx);
    if (engine().cpu_id() == core) {
        fsidx[ns][topic].insert(partition);
    }
    return make_ready_future<>();
}
