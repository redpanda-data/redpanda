#pragma once
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "random/generators.h"
#include "seastarx.h"
#include "storage/disk_log_impl.h"
#include "storage/log_manager.h"
#include "storage/tests/utils/random_batch.h"
#include "units.h"
#include "vassert.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sstring.hh>

#include <optional>
#include <tuple>
#include <type_traits>
#include <vector>
namespace storage {

inline static ss::sstring random_dir() {
    return ss::sstring(
      fmt::format("test.dir_{}", random_generators::gen_alphanum_string(7)));
}

inline static log_config log_builder_config() {
    return log_config(
      log_config::storage_type::disk,
      random_dir(),
      100_MiB,
      log_config::debug_sanitize_files::yes);
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
    arg_3_way_assert<sizeof...(args), 1, 5>();
    return std::make_tuple(
      add_random_batch_tag(), std::forward<decltype(args)>(args)...);
};

inline constexpr auto add_random_batches = [](auto&&... args) {
    arg_3_way_assert<sizeof...(args), 1, 3>();
    return std::make_tuple(
      add_random_batches_tag(), std::forward<decltype(args)>(args)...);
};

inline constexpr auto add_batch = [](auto&&... args) {
    arg_3_way_assert<sizeof...(args), 1, 3>();
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
    // Constructors
    explicit disk_log_builder(
      storage::log_config config = log_builder_config());
    ~disk_log_builder() noexcept = default;
    disk_log_builder(const disk_log_builder&) = delete;
    disk_log_builder& operator=(const disk_log_builder&) = delete;
    disk_log_builder(disk_log_builder&&) noexcept = default;
    disk_log_builder& operator=(disk_log_builder&&) noexcept = delete;

    // Syntactic sugar for pipes
    template<typename... Args>
    disk_log_builder& operator|(std::tuple<Args...> args) {
        auto op = std::get<0>(args);
        constexpr auto size = std::tuple_size<std::tuple<Args...>>::value;
        using type = typename std::tuple_element<0, std::tuple<Args...>>::type;
        // This can be rewritten with TMP. start(decompose(args))
        if constexpr (std::is_same_v<type, start_tag>) {
            if constexpr (size == 1) {
                start().get();
            } else if constexpr (size == 2) {
                start(std::get<1>(args)).get();
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
            }

        } else if constexpr (std::is_same_v<type, add_batch_tag>) {
            if constexpr (size == 2) {
                add_batch(std::move(std::get<1>(args))).get();
            } else if constexpr (size == 3) {
                add_batch(std::move(std::get<1>(args)), std::get<2>(args))
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
            }
        }
        return *this;
    }
    // Batch generation
    ss::future<> add_random_batch(
      model::offset offset,
      int num_records,
      maybe_compress_batches comp = maybe_compress_batches::yes,
      model::record_batch_type bt = model::record_batch_type(1), // data
      log_append_config config = append_config());
    ss::future<> add_random_batches(
      model::offset offset,
      int count,
      maybe_compress_batches comp = maybe_compress_batches::yes,
      log_append_config config = append_config());
    ss::future<> add_random_batches(
      model::offset offset, log_append_config config = append_config());
    ss::future<> truncate(model::offset);
    ss::future<> gc(
      model::timestamp collection_upper_bound,
      std::optional<size_t> max_partition_retention_size);
    ss::future<> add_batch(
      model::record_batch batch, log_append_config config = append_config());
    //  Log managment
    ss::future<> start(model::ntp ntp = log_builder_ntp());
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
    CONCEPT(requires model::BatchReaderConsumer<Consumer>())
    auto consume(log_reader_config config = reader_config()) {
        return consume_impl(Consumer{}, std::move(config));
    }

    // Non default constructable Consumer with config
    template<typename Consumer>
    CONCEPT(requires model::BatchReaderConsumer<Consumer>())
    auto consume(Consumer c, log_reader_config config = reader_config()) {
        return consume_impl(std::move(c), std::move(config));
    }

    // Configuration getters
    const log_config& get_log_config() const;

private:
    template<typename Consumer>
    auto consume_impl(Consumer c, log_reader_config config) {
        return _log->make_reader(config).then(
          [c = std::move(c)](model::record_batch_reader reader) mutable {
              return std::move(reader).consume(std::move(c), model::no_timeout);
          });
    }

    ss::future<> write(
      ss::circular_buffer<model::record_batch> buff,
      const log_append_config& config);

    log_manager _mgr;
    std::optional<log> _log;
    std::vector<std::vector<model::record_batch>> _batches;
    ss::abort_source _abort_source;
};

} // namespace storage
