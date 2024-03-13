/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "io/tests/common.h"

#include "base/units.h"

#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/later.hh>

#include <absl/strings/escaping.h>

#include <random>
#include <span>

using engine_type
  = std::independent_bits_engine<std::mt19937, CHAR_BIT, unsigned char>;

namespace {
seastar::future<seastar::temporary_buffer<char>> make_random_data(
  size_t size, std::optional<uint64_t> alignment, engine_type* eng) {
    static constexpr size_t chunk_size = 32_KiB;

    auto data = [&] {
        if (alignment.has_value()) {
            return seastar::temporary_buffer<char>::aligned(
              alignment.value(), size);
        }
        return seastar::temporary_buffer<char>(size);
    }();

    uint64_t offset = 0;
    std::span<char> span(data.get_write(), data.size());

    while (true) {
        auto len = std::min(span.size() - offset, chunk_size);
        if (len == 0) {
            break;
        }

        auto chunk = span.subspan(offset, len);
        std::generate(chunk.begin(), chunk.end(), [&] { return (*eng)(); });
        offset += len;

        co_await seastar::maybe_yield();
    }

    co_return data;
}
} // namespace

seastar::future<seastar::temporary_buffer<char>> make_random_data(
  size_t size,
  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
  std::optional<uint64_t> alignment,
  std::optional<uint64_t> seed) {
    static thread_local std::random_device rd;
    static thread_local engine_type eng(rd());

    if (seed.has_value()) {
        engine_type seeded_eng(seed.value());
        co_return co_await make_random_data(size, alignment, &seeded_eng);
    }

    co_return co_await make_random_data(size, alignment, &eng);
}

seastar::lw_shared_ptr<io::page>
make_page(uint64_t offset, std::optional<uint64_t> seed) {
    return seastar::make_lw_shared<io::page>(
      offset, make_random_data(4096, 4096, seed).get());
}

void sleep_ms(unsigned min, unsigned max) {
    const auto ms = random_generators::get_int(min, max);
    seastar::sleep(std::chrono::milliseconds(ms)).get();
}

io_queue_workload_generator::io_queue_workload_generator(
  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
  size_t max_file_size,
  size_t num_io_ops,
  std::function<void(io::page&)> submit_read,
  std::function<void(io::page&)> submit_write,
  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
  unsigned min_ms,
  unsigned max_ms)
  : max_file_size(max_file_size)
  , num_io_ops(num_io_ops)
  , submit_read(std::move(submit_read))
  , submit_write(std::move(submit_write))
  , min_ms(min_ms)
  , max_ms(max_ms) {}

seastar::future<> io_queue_workload_generator::run() {
    std::ignore = seastar::with_gate(
      gate, [this] { return generate_writes(); });
    std::ignore = seastar::with_gate(gate, [this] { return generate_reads(); });
    return gate.close();
}

void io_queue_workload_generator::handle_complete(io::page& page) {
    if (page.test_flag(io::page::flags::write)) {
        completed_writes.emplace(page.offset(), &page);
        ++completed_writes_count;
    } else if (page.test_flag(io::page::flags::read)) {
        completed_reads.push_back(&page);
    }
}

seastar::future<> io_queue_workload_generator::generate_writes() {
    return seastar::async([this] {
        while (completed_writes_count < num_io_ops) {
            const auto offset = seastar::align_down<size_t>(
              random_generators::get_int(max_file_size), 4096);
            submitted_writes.push_back(make_page(offset, offset));
            submit_write(*submitted_writes.back());
            sleep_ms(min_ms, max_ms);
        }
    });
}

seastar::future<> io_queue_workload_generator::generate_reads() {
    return seastar::async([this] {
        while (completed_reads.size() < num_io_ops) {
            if (auto* write = select_random_write(); write != nullptr) {
                submitted_reads.push_back(make_page(write->offset()));
                submit_read(*submitted_reads.back());
            }
            sleep_ms(min_ms, max_ms);
        }
    });
}

io::page* io_queue_workload_generator::select_random_write() const {
    auto delta = random_generators::get_int(completed_writes.size());
    auto it = completed_writes.begin();
    std::advance(it, delta);
    if (it != completed_writes.end()) {
        return it->second;
    }
    return nullptr;
}

io_queue_fault_injector::io_queue_fault_injector(
  io::persistence* storage,
  io::io_queue* queue,
  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
  unsigned min_ms,
  unsigned max_ms)
  : storage(storage)
  , queue(queue)
  , min_ms(min_ms)
  , max_ms(max_ms) {}

void io_queue_fault_injector::start() {
    injector = seastar::async([this] {
        while (!stop_) {
            auto op = random_generators::get_int(0, 1);
            if (op == 0) {
                storage->fail_next_open(
                  std::runtime_error("injected open failure"));

            } else if (op == 1) {
                auto file = io::testing_details::io_queue_accessor::get_file(
                  queue);
                if (file != nullptr) {
                    file->fail_next_close(
                      std::runtime_error("injected close failure"));
                }
            }
            sleep_ms(min_ms, max_ms);
        }
    });
}

seastar::future<> io_queue_fault_injector::stop() {
    stop_ = true;
    return std::move(injector);
}

testing::AssertionResult EqualInputStreams(
  seastar::input_stream<char>& input1, seastar::input_stream<char>& input2) {
    auto data1 = input1.read().get();
    auto data2 = input2.read().get();
    while (true) {
        auto len = std::min(data1.size(), data2.size());
        if (data1.share(0, len) != data2.share(0, len)) {
            return ::testing::AssertionFailure() << fmt::format(
                     "Input 1/2 chunk sizes {} vs {}. data {} vs {}",
                     data1.size(),
                     data2.size(),
                     absl::CHexEscape(
                       std::string_view(data1.get(), len).substr(0, 20)),
                     absl::CHexEscape(
                       std::string_view(data2.get(), len).substr(0, 20)));
        }
        data1.trim_front(len);
        data2.trim_front(len);
        if (data1.empty()) {
            data1 = input1.read().get();
        }
        if (data2.empty()) {
            data2 = input2.read().get();
        }
        if (data1.empty() || data2.empty()) {
            break;
        }
    }
    if (data1 != data2) {
        return ::testing::AssertionFailure() << fmt::format(
                 "Input 1/2 chunk size {} vs {}", data1.size(), data2.size());
    }
    return ::testing::AssertionSuccess();
}
