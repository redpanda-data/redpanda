#include "storage/opfuzz/opfuzz.h"

#include "model/record.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "units.h"
#include "utils/directory_walker.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/backtrace.hh>

#include <boost/algorithm/string/predicate.hpp>

#include <memory>

namespace storage {
// NOLINTNEXTLINE
ss::logger fuzzlogger("opfuzz");

static size_t
record_count(const ss::circular_buffer<model::record_batch>& batches) {
    return std::accumulate(
      batches.begin(),
      batches.end(),
      0,
      [](size_t acc, const model::record_batch& batch) {
          return acc + batch.record_count();
      });
}

struct append_offsets_validator {
    append_offsets_validator(
      storage::log* log, size_t record_count, bool is_flushed)
      : log(log) {
        auto lstats = log->offsets();

        model::offset next = lstats.dirty_offset;
        if (next() < 0) {
            if (lstats.start_offset() < 0) {
                next = model::offset(0);
            } else {
                next = lstats.start_offset;
            }
        } else {
            next++;
        }
        expected_dirty = next + model::offset(record_count) - model::offset(1);
        start_seg_count = log->segment_count();
        if (is_flushed) {
            expected_committed = expected_dirty;
        } else {
            expected_committed = lstats.committed_offset;
        }
    }

    void operator()(storage::append_result res) {
        auto lstats = log->offsets();
        vassert(
          res.last_offset == expected_dirty,
          "Expected append result offset it different than what we have - "
          "expected: {}, current: {}",
          expected_dirty,
          res.last_offset);
        vassert(
          lstats.dirty_offset == expected_dirty,
          "Expected dirty offset is different than what we have - expected: "
          "{}, current: {}",
          expected_dirty,
          lstats.dirty_offset);

        vassert(
          lstats.committed_offset < expected_dirty,
          "Committed offset is greater than expected dirty offset - expected: "
          "{}, current: {}",
          expected_committed,
          lstats.committed_offset);
        // there was no flush
        if (start_seg_count == log->segment_count()) {
            vassert(
              lstats.committed_offset == expected_committed,
              "Expected committed offset is different than what we have - "
              "expected: {}, current: {}",
              expected_committed,
              lstats.committed_offset);
        }
    }

    storage::log* log;
    model::offset expected_dirty;
    model::offset expected_committed;
    size_t start_seg_count;
};

struct append_op final : opfuzz::op {
    ~append_op() noexcept override = default;
    const char* name() const final { return "append"; }
    ss::future<> invoke(opfuzz::op_context ctx) final {
        storage::log_append_config append_cfg{
          storage::log_append_config::fsync::no,
          ss::default_priority_class(),
          model::no_timeout};
        auto batches = storage::test::make_random_batches(model::offset(0), 10);
        vlog(
          fuzzlogger.info,
          "Appending: {} batches. {}-{}",
          batches.size(),
          batches.front().base_offset(),
          batches.back().last_offset());
        for (auto& b : batches) {
            b.set_term(*ctx.term);
        }
        auto validator = append_offsets_validator(
          ctx.log, record_count(batches), false);
        auto reader = model::make_memory_record_batch_reader(
          std::move(batches));
        return std::move(reader)
          .for_each_ref(ctx.log->make_appender(append_cfg), model::no_timeout)
          .then(validator);
    }
};

struct append_op_foreign final : opfuzz::op {
    ~append_op_foreign() noexcept override = default;
    const char* name() const final { return "append_op_foreign"; }
    ss::future<> invoke(opfuzz::op_context ctx) final {
        auto source_core = random_generators::get_int(ss::smp::count - 1);
        return ss::smp::submit_to(
                 source_core,
                 [term = *ctx.term] {
                     auto batches = storage::test::make_random_batches(
                       model::offset(0), 10);
                     vlog(
                       fuzzlogger.info,
                       "Foreign appending: {} batches. {}-{}",
                       batches.size(),
                       batches.front().base_offset(),
                       batches.back().last_offset());
                     for (auto& b : batches) {
                         b.set_term(term);
                     }
                     auto cnt = record_count(batches);
                     auto reader = model::make_memory_record_batch_reader(
                       std::move(batches));
                     return std::pair<model::record_batch_reader, size_t>(
                       model::make_foreign_record_batch_reader(
                         std::move(reader)),
                       cnt);
                 })
          .then([ctx](std::pair<model::record_batch_reader, size_t> p) {
              return ss::smp::submit_to(
                0, [rdr = std::move(p.first), cnt = p.second, ctx]() mutable {
                    storage::log_append_config append_cfg{
                      storage::log_append_config::fsync::no,
                      ss::default_priority_class(),
                      model::no_timeout};
                    auto validator = append_offsets_validator(
                      ctx.log, cnt, false);
                    return std::move(rdr)
                      .for_each_ref(
                        ctx.log->make_appender(append_cfg), model::no_timeout)
                      .then(validator);
                });
          });
    }
};

struct append_multi_term_op final : opfuzz::op {
    ~append_multi_term_op() noexcept override = default;
    const char* name() const final { return "append_with_multiple_terms"; }
    ss::future<> invoke(opfuzz::op_context ctx) final {
        storage::log_append_config append_cfg{
          storage::log_append_config::fsync::no,
          ss::default_priority_class(),
          model::no_timeout};
        auto batches = storage::test::make_random_batches(model::offset(0), 10);
        const size_t mid = batches.size() / 2;
        vlog(
          fuzzlogger.info,
          "Appending multi-term: {} - middle:{} - batches. {}-{}",
          batches.size(),
          mid,
          batches.front().base_offset(),
          batches.back().last_offset());
        for (size_t i = 0; i < mid; ++i) {
            batches[i].set_term(*ctx.term);
        }
        (*ctx.term)++;
        for (size_t i = mid; i < batches.size(); ++i) {
            batches[i].set_term(*ctx.term);
        }
        auto validator = append_offsets_validator(
          ctx.log, record_count(batches), false);
        auto reader = model::make_memory_record_batch_reader(
          std::move(batches));
        return std::move(reader)
          .for_each_ref(ctx.log->make_appender(append_cfg), model::no_timeout)
          .then(validator);
    }
};

struct truncate_op final : opfuzz::op {
    struct collect_base_offsets {
        ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
            offsets.push_back(batch.base_offset());
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }
        std::vector<model::offset> end_of_stream() {
            return std::move(offsets);
        }
        std::vector<model::offset> offsets;
    };

    ~truncate_op() noexcept override = default;
    const char* name() const final { return "truncate"; }
    ss::future<> invoke(opfuzz::op_context ctx) final {
        auto lstats = ctx.log->offsets();
        storage::log_reader_config cfg(
          lstats.start_offset,
          lstats.dirty_offset,
          ss::default_priority_class());
        vlog(fuzzlogger.info, "collect base offsets {} - {}", cfg, *ctx.log);
        return ctx.log->make_reader(cfg)
          .then([](model::record_batch_reader reader) {
              return std::move(reader).consume(
                collect_base_offsets{}, model::no_timeout);
          })
          .then([ctx](std::vector<model::offset> ofs) {
              vlog(fuzzlogger.info, "base offsets collected: {}", ofs);
              model::offset to{0};
              if (!ofs.empty()) {
                  to = ofs[random_generators::get_int<size_t>(
                    0, ofs.size() - 1)];
              }
              vlog(fuzzlogger.info, "Truncating log at suffix offset: {}", to);
              return ctx.log
                ->truncate(
                  storage::truncate_config(to, ss::default_priority_class()))
                .then([to] { return to; });
          })
          .then([ctx](model::offset to) {
              auto loffsets = ctx.log->offsets();
              /*
               * in the normal case, the new dirty offset is `to - 1`. however,
               * there are some special cases. if to <= start, then the new
               * dirty offset is `start - 1`, unless start is uninitialized or
               * 0, and `to` is 0. then new dirty offset is uninitialized.
               */
              auto expected = to - model::offset(1);
              if (to <= std::max(loffsets.start_offset, model::offset(0))) {
                  if (loffsets.start_offset() > 0) {
                      expected = loffsets.start_offset - model::offset(1);
                  } else {
                      expected = model::offset{};
                  }
              }
              vassert(
                loffsets.dirty_offset == expected,
                "Truncate failed - expected offset {}, "
                "have offset {}",
                expected,
                loffsets);
          });
    }
};

struct truncate_prefix_op final : opfuzz::op {
    struct collect_max_offsets {
        ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
            offsets.push_back(batch.last_offset());
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }
        std::vector<model::offset> end_of_stream() {
            return std::move(offsets);
        }
        std::vector<model::offset> offsets;
    };

    ~truncate_prefix_op() noexcept override = default;
    const char* name() const final { return "truncate_prefix"; }
    ss::future<> invoke(opfuzz::op_context ctx) final {
        auto lstats = ctx.log->offsets();
        storage::log_reader_config cfg(
          lstats.start_offset,
          lstats.dirty_offset,
          ss::default_priority_class());
        vlog(
          fuzzlogger.info,
          "collect header::max_offsets {} - {}",
          cfg,
          *ctx.log);
        return ctx.log->make_reader(cfg)
          .then([](model::record_batch_reader reader) {
              return std::move(reader).consume(
                collect_max_offsets{}, model::no_timeout);
          })
          .then([ctx](std::vector<model::offset> ofs) {
              vlog(fuzzlogger.info, "max offsets collected: {}", ofs);
              model::offset to{0};
              if (!ofs.empty()) {
                  to = ofs[random_generators::get_int<size_t>(
                    0, ofs.size() - 1)];
                  to++;
              }
              vlog(fuzzlogger.info, "Truncating log at prefix offset: {}", to);
              return ctx.log->truncate_prefix(storage::truncate_prefix_config(
                to, ss::default_priority_class()));
          });
    }
};

struct simple_verify_consumer {
    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        if (auto crc = model::crc_record_batch(b); b.header().crc != crc) {
            auto ptr = ss::make_backtraced_exception_ptr<std::runtime_error>(
              fmt::format(
                "Expected CRC: {}, but got CRC:{} - invalid batch: {}",
                b.header().crc,
                crc,
                b));
            return ss::make_exception_future<ss::stop_iteration>(ptr);
        }
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }
    void end_of_stream() {}
};

struct read_op final : opfuzz::op {
    ~read_op() noexcept override = default;
    const char* name() const final { return "read"; }
    ss::future<> invoke(opfuzz::op_context ctx) final {
        auto lstats = ctx.log->offsets();
        model::offset start = lstats.start_offset;
        model::offset end{0};
        if (lstats.dirty_offset > start) {
            start = model::offset(
              random_generators::get_int<model::offset::type>(
                start(), lstats.dirty_offset()));
        }
        if (start > end) {
            end = model::offset(random_generators::get_int<model::offset::type>(
              start(), lstats.dirty_offset));
        }
        storage::log_reader_config cfg(
          start, end, ss::default_priority_class());
        vlog(fuzzlogger.info, "Read [{},{}] - {}", start, end, *ctx.log);
        return ctx.log->make_reader(cfg).then(
          [](model::record_batch_reader reader) {
              return std::move(reader).consume(
                simple_verify_consumer{}, model::no_timeout);
          });
    }
};
struct flush_op final : opfuzz::op {
    ~flush_op() noexcept override = default;
    const char* name() const final { return "flush"; }
    ss::future<> invoke(opfuzz::op_context ctx) final {
        return ctx.log->flush();
    }
};
struct term_roll_op final : opfuzz::op {
    ~term_roll_op() noexcept override = default;
    const char* name() const final { return "term_roll"; }
    ss::future<> invoke(opfuzz::op_context ctx) final {
        (*ctx.term)++;
        return ss::make_ready_future<>();
    }
};

struct remove_all_compacted_indices_op final : opfuzz::op {
    ~remove_all_compacted_indices_op() noexcept override = default;
    const char* name() const final { return "remove_all_compacted_indices_op"; }
    ss::future<> invoke(opfuzz::op_context ctx) final {
        ss::sstring dir = ctx.log->config().work_directory();
        return directory_walker::walk(dir, [dir](ss::directory_entry de) {
            if (boost::algorithm::ends_with(de.name, ".compaction_index")) {
                vlog(
                  fuzzlogger.info,
                  "[COMPACTION_INDEX] removing: {}/{}",
                  dir,
                  de.name);
                return ss::remove_file(fmt::format("{}/{}", dir, de.name));
            }
            return ss::now();
        });
    }
};

struct compact_op final : opfuzz::op {
    ~compact_op() noexcept override = default;
    const char* name() const final { return "compact_op"; }
    ss::future<> invoke(opfuzz::op_context ctx) final {
        compaction_config cfg(
          model::timestamp::max(),
          std::nullopt,
          ss::default_priority_class(),
          *(ctx._as),
          debug_sanitize_files::yes);
        if (random_generators::get_int(0, 100) > 70) {
            cfg.eviction_time = model::timestamp::now();
            cfg.max_bytes = 10_MiB;
        }
        vlog(fuzzlogger.info, "COMPACT: {} - {}", cfg, *ctx.log);
        return ctx.log->compact(cfg);
    }
};

ss::future<> opfuzz::execute() {
    // execute commands in sequence
    return ss::do_for_each(_workload, [this](std::unique_ptr<op>& c) {
        vlog(fuzzlogger.info, "Executing: {}", c->name());
        return c->invoke(op_context{&_term, &_log, &_as});
    });
}

std::unique_ptr<opfuzz::op> opfuzz::random_operation() {
    auto next = op_name(
      random_generators::get_int((int)op_name::min, (int)op_name::max));
    switch (next) {
    case op_name::append:
        return std::make_unique<append_op>();
    case op_name::append_with_multiple_terms:
        return std::make_unique<append_multi_term_op>();
    case op_name::append_op_foreign:
        return std::make_unique<append_op_foreign>();
    case op_name::truncate:
        return std::make_unique<truncate_op>();
    case op_name::truncate_prefix:
        return std::make_unique<truncate_prefix_op>();
    case op_name::read:
        return std::make_unique<read_op>();
    case op_name::flush:
        return std::make_unique<flush_op>();
    case op_name::compact:
        return std::make_unique<compact_op>();
    case op_name::remove_all_compacted_indices:
        return std::make_unique<remove_all_compacted_indices_op>();
    case op_name::term_roll:
        return std::make_unique<term_roll_op>();
    }
    vassert(false, "could not generate random operation for log");
}

void opfuzz::generate_workload(size_t count) {
    _workload.reserve(count);
    std::generate_n(std::back_inserter(_workload), count, [this] {
        return random_operation();
    });
}
} // namespace storage
