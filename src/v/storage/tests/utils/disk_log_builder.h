/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "random/generators.h"
#include "seastarx.h"
#include "ssx/sformat.h"
#include "storage/api.h"
#include "storage/disk_log_impl.h"
#include "units.h"
#include "vassert.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <tuple>
#include <type_traits>
#include <vector>
namespace storage {

inline static ss::sstring random_dir() {
    return ssx::sformat(
      "test.dir_{}", random_generators::gen_alphanum_string(7));
}

inline static log_config log_builder_config() {
    return log_config(random_dir(), 100_MiB, debug_sanitize_files::yes);
}

inline static log_reader_config reader_config() {
    return log_reader_config{
      model::offset(0),
      model::model_limits<model::offset>::max(),
      ss::default_priority_class()};
}

inline static log_append_config append_config() {
    return log_append_config{
      .should_fsync = storage::log_append_config::fsync::yes,
      .io_priority = ss::default_priority_class(),
      .timeout = model::no_timeout};
}

inline static model::ntp log_builder_ntp() {
    return model::ntp(
      model::ns("test.log_builder"),
      model::topic(random_generators::gen_alphanum_string(8)),
      model::partition_id(0));
}

// Tags
struct add_random_batch_tag {};
struct add_random_batches_tag {};
struct truncate_log_tag {};
struct garbage_collect_tag {};
struct start_tag {};
struct stop_tag {};
struct add_segment_tag {};
struct add_batch_tag {};

using maybe_compress_batches
  = ss::bool_class<struct allow_maybe_compress_batches_builder_tag>;

template<size_t arg_size, size_t min_args, size_t max_args>
inline void arg_3_way_assert() {
    static_assert(arg_size >= min_args && arg_size <= max_args);
}

inline constexpr auto start = [](auto&&... args) {
    arg_3_way_assert<sizeof...(args), 0, 1>();
    return std::make_tuple(start_tag(), std::forward<decltype(args)>(args)...);
};

inline constexpr auto stop = []() { return std::make_tuple(stop_tag()); };

inline constexpr auto add_segment = [](auto&&... args) {
    arg_3_way_assert<sizeof...(args), 1, 3>();
    return std::make_tuple(
      add_segment_tag(), std::forward<decltype(args)>(args)...);
};

inline constexpr auto add_random_batch = [](auto&&... args) {
    arg_3_way_assert<sizeof...(args), 1, 7>();
    return std::make_tuple(
      add_random_batch_tag(), std::forward<decltype(args)>(args)...);
};

inline constexpr auto add_random_batches = [](auto&&... args) {
    arg_3_way_assert<sizeof...(args), 1, 6>();
    return std::make_tuple(
      add_random_batches_tag(), std::forward<decltype(args)>(args)...);
};

inline constexpr auto add_batch = [](auto&&... args) {
    arg_3_way_assert<sizeof...(args), 1, 4>();
    return std::make_tuple(
      add_batch_tag(), std::forward<decltype(args)>(args)...);
};

inline constexpr auto garbage_collect = [](auto&&... args) {
    arg_3_way_assert<sizeof...(args), 2, 2>();
    return std::make_tuple(
      garbage_collect_tag{}, std::forward<decltype(args)>(args)...);
};

inline constexpr auto truncate_log = [](auto&&... args) {
    arg_3_way_assert<sizeof...(args), 1, 1>();
    return std::make_tuple(
      truncate_log_tag{}, std::forward<decltype(args)>(args)...);
};

class disk_log_builder {
public:
    using should_flush_after = ss::bool_class<struct flush_after_tag>;
    // Constructors
    explicit disk_log_builder(
      storage::log_config config = log_builder_config());
    ~disk_log_builder() noexcept = default;
    disk_log_builder(const disk_log_builder&) = delete;
    disk_log_builder& operator=(const disk_log_builder&) = delete;
    disk_log_builder(disk_log_builder&&) noexcept = delete;
    disk_log_builder& operator=(disk_log_builder&&) noexcept = delete;

    // Syntactic sugar for pipes
    template<typename... Args>
    disk_log_builder& operator|(std::tuple<Args...> args) {
        constexpr auto size = std::tuple_size<std::tuple<Args...>>::value;
        using type = typename std::tuple_element<0, std::tuple<Args...>>::type;
        // This can be rewritten with TMP. start(decompose(args))
        if constexpr (std::is_same_v<type, start_tag>) {
            if constexpr (size == 1) {
                start().get();
            } else if constexpr (size == 2) {
                start(std::move(std::get<1>(args))).get();
            }
        } else if constexpr (std::is_same_v<type, stop_tag>) {
            stop().get();
        } else if constexpr (std::is_same_v<type, truncate_log_tag>) {
            truncate(model::offset(std::get<1>(args))).get();
        } else if constexpr (std::is_same_v<type, garbage_collect_tag>) {
            gc(std::get<1>(args), std::get<2>(args)).get();
        } else if constexpr (std::is_same_v<type, add_segment_tag>) {
            if constexpr (size == 2) {
                add_segment(model::offset(std::get<1>(args))).get();
            } else if constexpr (size == 3) {
                add_segment(
                  model::offset(std::get<1>(args)),
                  model::term_id(std::get<2>(args)))
                  .get();
            } else if constexpr (size == 4) {
                add_segment(
                  model::offset(std::get<1>(args)),
                  model::term_id(std::get<2>(args)),
                  std::get<3>(args))
                  .get();
            }
        } else if constexpr (std::is_same_v<type, add_random_batch_tag>) {
            if constexpr (size == 2) {
                add_random_batch(model::offset(std::get<1>(args)), 1).get();
            } else if constexpr (size == 3) {
                add_random_batch(
                  model::offset(std::get<1>(args)), std::get<2>(args))
                  .get();
            } else if constexpr (size == 4) {
                add_random_batch(
                  model::offset(std::get<1>(args)),
                  std::get<2>(args),
                  std::get<3>(args))
                  .get();
            } else if constexpr (size == 5) {
                add_random_batch(
                  model::offset(std::get<1>(args)),
                  std::get<2>(args),
                  std::get<3>(args),
                  std::get<4>(args))
                  .get();
            } else if constexpr (size == 6) {
                add_random_batch(
                  model::offset(std::get<1>(args)),
                  std::get<2>(args),
                  std::get<3>(args),
                  std::get<4>(args),
                  std::get<5>(args))
                  .get();
            } else if constexpr (size == 7) {
                add_random_batch(
                  model::offset(std::get<1>(args)),
                  std::get<2>(args),
                  std::get<3>(args),
                  std::get<4>(args),
                  std::get<5>(args),
                  std::get<6>(args))
                  .get();
            } else if constexpr (size == 8) {
                add_random_batch(
                  model::offset(std::get<1>(args)),
                  std::get<2>(args),
                  std::get<3>(args),
                  std::get<4>(args),
                  std::get<5>(args),
                  std::get<6>(args),
                  std::get<7>(args))
                  .get();
            }

        } else if constexpr (std::is_same_v<type, add_batch_tag>) {
            if constexpr (size == 2) {
                add_batch(std::move(std::get<1>(args))).get();
            } else if constexpr (size == 3) {
                add_batch(std::move(std::get<1>(args)), std::get<2>(args))
                  .get();
            } else if constexpr (size == 4) {
                add_batch(
                  std::move(std::get<1>(args)),
                  std::get<2>(args),
                  std::get<3>(args))
                  .get();
            }
        } else if constexpr (std::is_same_v<type, add_random_batches_tag>) {
            if constexpr (size == 2) {
                add_random_batches(model::offset(std::get<1>(args))).get();
            } else if constexpr (size == 3) {
                add_random_batches(
                  model::offset(std::get<1>(args)), std::get<2>(args))
                  .get();
            } else if constexpr (size == 4) {
                add_random_batches(
                  model::offset(std::get<1>(args)),
                  std::get<2>(args),
                  std::get<3>(args))
                  .get();
            } else if constexpr (size == 5) {
                add_random_batches(
                  model::offset(std::get<1>(args)),
                  std::get<2>(args),
                  std::get<3>(args),
                  std::get<4>(args))
                  .get();
            } else if constexpr (size == 6) {
                add_random_batches(
                  model::offset(std::get<1>(args)),
                  std::get<2>(args),
                  std::get<3>(args),
                  std::get<4>(args),
                  std::get<5>(args))
                  .get();
            } else if constexpr (size == 7) {
                add_random_batches(
                  model::offset(std::get<1>(args)),
                  std::get<2>(args),
                  std::get<3>(args),
                  std::get<4>(args),
                  std::get<5>(args),
                  std::get<6>(args))
                  .get();
            }
        }
        return *this;
    }
    // Batch generation
    ss::future<> add_random_batch(
      model::offset offset,
      int num_records,
      maybe_compress_batches comp = maybe_compress_batches::yes,
      model::record_batch_type bt = model::record_batch_type::raft_data, // data
      log_append_config config = append_config(),
      should_flush_after flush = should_flush_after::yes,
      std::optional<model::timestamp> base_ts = std::nullopt);
    ss::future<> add_random_batch(
      model::test::record_batch_spec spec,
      log_append_config config = append_config(),
      should_flush_after flush = should_flush_after::yes);
    ss::future<> add_random_batches(
      model::offset offset,
      int count,
      maybe_compress_batches comp = maybe_compress_batches::yes,
      log_append_config config = append_config(),
      should_flush_after flush = should_flush_after::yes,
      std::optional<model::timestamp> base_ts = std::nullopt);
    ss::future<> add_random_batches(
      model::offset offset,
      log_append_config config = append_config(),
      should_flush_after flush = should_flush_after::yes);
    ss::future<> truncate(model::offset);
    ss::future<> gc(
      model::timestamp collection_upper_bound,
      std::optional<size_t> max_partition_retention_size);
    ss::future<usage_report> disk_usage(
      model::timestamp collection_upper_bound,
      std::optional<size_t> max_partition_retention_size);
    ss::future<std::optional<model::offset>>
    apply_retention(compaction_config cfg);
    ss::future<> apply_compaction(
      compaction_config cfg,
      std::optional<model::offset> new_start_offset = std::nullopt);
    ss::future<bool> update_start_offset(model::offset start_offset);
    ss::future<> add_batch(
      model::record_batch batch,
      log_append_config config = append_config(),
      should_flush_after flush = should_flush_after::yes);
    //  Log managment
    ss::future<> start(model::ntp ntp = log_builder_ntp());
    ss::future<> start(storage::ntp_config);
    ss::future<> stop();

    // Low lever interface access
    // Access log impl
    log& get_log();
    disk_log_impl& get_disk_log_impl();
    segment_set& get_log_segments();
    // Index range is [0....total_segments)
    segment& get_segment(size_t index);
    segment_index& get_seg_index_ptr(size_t index);

    // Create segments
    ss::future<> add_segment(
      model::offset offset,
      model::term_id term = model::term_id(0),
      ss::io_priority_class pc = ss::default_priority_class());

    // Read interface
    // Default consume
    auto consume(log_reader_config config = reader_config()) {
        return _log->make_reader(config).then(
          [](model::record_batch_reader reader) {
              return model::consume_reader_to_memory(
                std::move(reader), model::no_timeout);
          });
    }

    // Consumer with config
    template<typename Consumer>
    requires model::BatchReaderConsumer<Consumer>
    auto consume(log_reader_config config = reader_config()) {
        return consume_impl(Consumer{}, std::move(config));
    }

    // Non default constructable Consumer with config
    template<typename Consumer>
    requires model::BatchReaderConsumer<Consumer>
    auto consume(Consumer c, log_reader_config config = reader_config()) {
        return consume_impl(std::move(c), std::move(config));
    }

    ss::future<> update_configuration(ntp_config::default_overrides o) {
        if (_log) {
            return _log->update_configuration(o);
        }

        return ss::make_ready_future<>();
    }

    // Configuration getters
    const log_config& get_log_config() const;

    size_t bytes_written() const { return _bytes_written; }

    storage::api& storage() { return _storage; }

    void set_time(model::timestamp t) { _ts_cursor = t; }

private:
    template<typename Consumer>
    auto consume_impl(Consumer c, log_reader_config config) {
        return _log->make_reader(config).then(
          [c = std::move(c)](model::record_batch_reader reader) mutable {
              return std::move(reader).consume(std::move(c), model::no_timeout);
          });
    }

    std::optional<model::timestamp> now(std::optional<model::timestamp> input) {
        if (input) {
            return input;
        } else if (_ts_cursor) {
            return _ts_cursor;
        } else {
            return model::timestamp::now();
        }
    }

    void advance_time(const model::record_batch& b) {
        _ts_cursor = model::timestamp{b.header().max_timestamp() + 1};
    }

    ss::future<> write(
      ss::circular_buffer<model::record_batch> buff,
      const log_append_config& config,
      should_flush_after flush);

    ss::sharded<features::feature_table> _feature_table;
    storage::log_config _log_config;
    storage::api _storage;
    std::optional<log> _log;
    size_t _bytes_written{0};
    std::vector<std::vector<model::record_batch>> _batches;
    ss::abort_source _abort_source;
    std::optional<model::timestamp> _ts_cursor;
};

} // namespace storage
