/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "storage/parser.h"

#include <seastar/core/future.hh>

#include <exception>

namespace storage {

/// Chain multiple batch_consumer instances to consume
/// from the same input_stream. The consumers are allowed to skip
/// batches or stop early.
///
/// Every consumer in the chain observes the batches as if it's the
/// only one in the chain. If one of the consumers skips batches or
/// stops early it shouldn't affect the others.
template<int N>
class chained_batch_consumer : public storage::batch_consumer {
public:
    template<typename... Args>
    explicit chained_batch_consumer(Args&&... args)
      : _cons{std::forward<Args>(args)...} {
        std::ignore = std::fill_n(
          _prev.begin(), N, consume_result::accept_batch);
    }

    consume_result
    accept_batch_start(const model::record_batch_header& h) const override {
        auto ix = 0;
        for (const auto& c : _cons) {
            if (_prev[ix] != consume_result::stop_parser) {
                auto r = c->accept_batch_start(h);
                _prev[ix] = r;
            }
            ix++;
        }
        size_t n_stop = 0;
        size_t n_skip = 0;
        for (auto cr : _prev) {
            switch (cr) {
            case consume_result::accept_batch:
                break;
            case consume_result::skip_batch:
                n_skip++;
                break;
            case consume_result::stop_parser:
                n_stop++;
                break;
            }
        }
        if (n_stop == _prev.size()) {
            return consume_result::stop_parser;
        }
        if (n_skip == _prev.size()) {
            return consume_result::skip_batch;
        }
        return consume_result::accept_batch;
    }

    void consume_batch_start(
      model::record_batch_header hdr,
      size_t offset, // NOLINT
      size_t size) override {
        auto ix = 0;
        for (const auto& c : _cons) {
            if (_prev[ix] == consume_result::accept_batch) {
                c->consume_batch_start(hdr, offset, size);
            } else if (_prev[ix] == consume_result::skip_batch) {
                c->skip_batch_start(hdr, offset, size);
            }
            ix++;
        }
    }

    void skip_batch_start(
      model::record_batch_header hdr,
      size_t offset, // NOLINT
      size_t size) override {
        auto ix = 0;
        for (const auto& c : _cons) {
            if (_prev[ix] == consume_result::skip_batch) {
                c->skip_batch_start(hdr, offset, size);
            }
            ix++;
        }
    }

    void consume_records(iobuf&& b) override {
        auto ix = 0;
        for (auto& c : _cons) {
            if (_prev[ix] == consume_result::accept_batch) {
                c->consume_records(b.share(0, b.size_bytes()));
            }
            ix++;
        }
    }

    ss::future<stop_parser> consume_batch_end() override {
        auto ix = 0;
        std::vector<ss::future<stop_parser>> fut;
        for (auto& c : _cons) {
            if (_prev[ix] == consume_result::accept_batch) {
                fut.emplace_back(c->consume_batch_end());
            } else {
                fut.emplace_back(ss::make_ready_future<stop_parser>(
                  _prev[ix] == consume_result::stop_parser ? stop_parser::yes
                                                           : stop_parser::no));
            }
            ix++;
        }
        auto ready = co_await ss::when_all(fut.begin(), fut.end());
        std::exception_ptr err;
        stop_parser stop = stop_parser::yes;
        ix = 0;
        for (ss::future<stop_parser>& f : ready) {
            if (f.failed()) {
                // We need to extract all exceptions to avoid the
                // warning but we can only rethrow the last one.
                err = f.get_exception();
                // Can't continue with the consumer after exception.
                // Assume it being stopped.
                _prev[ix] = consume_result::stop_parser;
            } else {
                auto s = f.get();
                if (s == stop_parser::yes) {
                    _prev[ix] = consume_result::stop_parser;
                } else {
                    // This could be removed
                    _prev[ix] = consume_result::accept_batch;
                    stop = stop_parser::no;
                }
            }
            ix++;
        }
        if (err) {
            std::rethrow_exception(err);
        }
        co_return stop;
    }

    void print(std::ostream& o) const override {
        o << "chained_batch_consumer";
    }

private:
    std::array<std::unique_ptr<storage::batch_consumer>, N> _cons;
    mutable std::array<consume_result, N> _prev;
};

} // namespace storage
