#pragma once
#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/log_manager.h"
#include "storage/tests/random_batch.h"

#include <seastar/core/sstring.hh>

namespace storage {

class log_builder final {
public:
    static constexpr size_t default_max_segment_size = 1 << 30;
    static constexpr uint32_t max_random_batch_size = 20;

    /**
     * compression: currently any non-none compression choice will be translated
     * into zstd compression due to a random batch generation limitation.
     */
    struct batch_spec {
        std::optional<model::offset> offset;
        std::optional<uint32_t> num_records;
        std::optional<model::compression> compression;
    };

    log_builder(ss::sstring base_dir, model::ntp ntp)
      : base_dir_(std::move(base_dir))
      , ntp_(std::move(ntp)) {}

    log_builder& segment() {
        segments_.push_back({});
        return *this;
    }

    const ss::sstring& base_dir() const { return base_dir_; }

    const model::ntp& ntp() const { return ntp_; }

    /**
     * Add a random batch with a fixed number of records.
     */
    log_builder& add_random_batch(uint32_t records) {
        add_batch(batch_spec{.num_records = records});
        return *this;
    }

    /**
     * Add a batch with random records.
     */
    log_builder& add_random_batch() {
        add_batch(batch_spec{});
        return *this;
    }

    /**
     * \brief Add a record batch.
     *
     * The caller must ensure that the record batch offsets are valid.
     */
    log_builder& add_batch(model::record_batch batch) {
        next_offset_ = model::offset(batch.last_offset()() + 1);
        segments_.back().batches.push_back(std::move(batch));
        return *this;
    }

    /**
     * \brief Add record batches.
     *
     * The caller must ensure that the record batch offsets are valid.
     */
    log_builder& add_batch(std::vector<model::record_batch> batches) {
        for (auto& b : batches) {
            add_batch(std::move(b));
        }
        return *this;
    }

    /**
     * Add a batch with a specification.
     */
    log_builder& add_batch(batch_spec spec) {
        if (!spec.num_records) {
            spec.num_records = random_generators::get_int(
              uint32_t(1), max_random_batch_size);
        }

        if (spec.offset) {
            next_offset_ = *spec.offset;
        } else {
            spec.offset = next_offset_;
        }
        next_offset_ = model::offset(next_offset_() + *spec.num_records);

        if (!spec.compression) {
            switch (random_generators::get_int(0, 4)) {
            case 0:
                spec.compression = model::compression::none;
            case 1:
                spec.compression = model::compression::gzip;
            case 2:
                spec.compression = model::compression::snappy;
            case 3:
                spec.compression = model::compression::lz4;
            case 4:
                spec.compression = model::compression::zstd;
            }
        }

        segments_.back().batches.push_back(spec);
        return *this;
    }

    /**
     * \brief Persists the log configuration.
     */
    ss::future<> flush() {
        return flush(std::numeric_limits<size_t>::max(), true);
    }

    /**
     * \brief Write configured batches with a max segment size.
     *
     * Note that this will ignore any segment structure that was configured
     * and write all batches as if no segments were defined and the log
     * manager were configured with the specified max segment size.
     */
    ss::future<> flush_with_max_segment_size(
      size_t max_segment_size = default_max_segment_size) {
        return flush(max_segment_size, false);
    }

    /**
     * Operate on a log.
     *
     * NOTE: it is not enforced, but this interface is generally expected to be
     * a read-only interface.
     */
    template<typename Func>
    auto with_log(Func&& f) {
        log_config config{
          .base_dir = base_dir_,
          .max_segment_size = std::numeric_limits<size_t>::max(),
          .should_sanitize = log_config::sanitize_files::yes,
        };
        return with_log(config, std::forward<Func>(f));
    }

private:
    using batch_type = std::variant<batch_spec, model::record_batch>;

    struct segment_spec {
        std::vector<batch_type> batches;
    };

    using segments_type = std::vector<segment_spec>;

    static model::record_batch make_record_batch(batch_spec spec) {
        /*
         * TODO: upstream batch is pending to control compression choice
         */
        return test::make_random_batch(*spec.offset, *spec.num_records, true);
    }

    static model::record_batch_reader make_batch_reader(segment_spec segment) {
        std::vector<model::record_batch> batches;
        for (auto& batch : segment.batches) {
            if (std::holds_alternative<model::record_batch>(batch)) {
                batches.push_back(
                  std::get<model::record_batch>(std::move(batch)));
            } else {
                batches.push_back(
                  make_record_batch(std::get<batch_spec>(std::move(batch))));
            }
        }
        return model::make_memory_record_batch_reader(std::move(batches));
    }

    ss::future<> flush(storage::log log, segments_type& segments, bool roll) {
        return ss::do_for_each(
          segments, [log, roll](segment_spec& segment) mutable {
              auto reader = make_batch_reader(std::move(segment));
              auto f = ss::make_ready_future<>();
              return f.then([log, reader = std::move(reader)]() mutable {
                  return log
                    .append(
                      std::move(reader),
                      storage::log_append_config{
                        .should_fsync = storage::log_append_config::fsync::yes,
                        .io_priority = ss::default_priority_class(),
                        .timeout = model::no_timeout})
                    .discard_result();
              });
          });
    }

    template<typename Func>
    auto with_log(log_config config, Func&& f) {
        auto mgr = log_manager(std::move(config));
        return ss::do_with(
          std::move(mgr),
          [ntp = ntp_, f = std::forward<Func>(f)](log_manager& mgr) mutable {
              return mgr.manage(ntp)
                .then([f = std::forward<Func>(f)](log l) { return f(l); })
                .finally([&mgr] { return mgr.stop(); });
          });
    }

    ss::future<> flush(size_t max_segment_size, bool roll) {
        storage::log_config config{
          .base_dir = base_dir_,
          .max_segment_size = max_segment_size,
          .should_sanitize = storage::log_config::sanitize_files::yes,
        };
        return with_log(config, [this, roll](log l) {
            return ss::do_with(
              std::move(segments_), [this, l, roll](segments_type& segments) {
                  return flush(l, segments, roll);
              });
        });
    }

    ss::sstring base_dir_;
    model::ntp ntp_;
    segments_type segments_;
    model::offset next_offset_{0};
};

} // namespace storage
