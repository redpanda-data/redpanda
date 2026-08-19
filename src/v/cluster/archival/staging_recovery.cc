/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/staging_recovery.h"

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/archival/logger.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "serde/rw/rw.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

#include <algorithm>

using namespace std::chrono_literals;

namespace archival {

namespace {

constexpr std::string_view staging_prefix = "staging/";
constexpr size_t footer_size = 16;

ss::future<std::optional<iobuf>> download_range_once(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  const ss::sstring& key,
  uint64_t first,
  uint64_t last,
  retry_chain_node& parent);

/// Ranged GET into an iobuf with its own retry budget (catalog reads race
/// the freshly-restored cluster's own uploads for S3 client-pool capacity,
/// so the shared recovery budget is not enough — observed live as
/// backoff-quota-exceeded skips). Returns nullopt on failure.
ss::future<std::optional<iobuf>> download_range(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  const ss::sstring& key,
  uint64_t first,
  uint64_t last, // inclusive
  retry_chain_node& parent) {
    for (int attempt = 0; attempt < 3; ++attempt) {
        auto res = co_await download_range_once(
          remote, bucket, key, first, last, parent);
        if (res.has_value()) {
            co_return res;
        }
        co_await ss::sleep(std::chrono::milliseconds(500 * (attempt + 1)));
    }
    co_return std::nullopt;
}

ss::future<std::optional<iobuf>> download_range_once(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  const ss::sstring& key,
  uint64_t first,
  uint64_t last, // inclusive
  retry_chain_node& parent) {
    iobuf buf;
    auto consume =
      [&buf](uint64_t, ss::input_stream<char> st) -> ss::future<uint64_t> {
        buf = co_await read_iobuf_exactly(
          st, std::numeric_limits<size_t>::max());
        co_await st.close();
        co_return buf.size_bytes();
    };
    auto res = co_await remote.download_stream(
      bucket,
      cloud_storage::remote_segment_path{std::filesystem::path{key}},
      consume,
      parent,
      "staging-recovery",
      cloud_storage::remote::download_metrics{},
      std::make_pair<uint64_t, uint64_t>(uint64_t(first), uint64_t(last)));
    if (res != cloud_storage::download_result::success) {
        co_return std::nullopt;
    }
    co_return std::move(buf);
}

struct footer {
    uint64_t index_size;
    uint32_t version;
    uint32_t magic;
};

std::optional<footer> parse_footer(iobuf buf) {
    if (buf.size_bytes() != footer_size) {
        return std::nullopt;
    }
    iobuf_parser p(std::move(buf));
    footer f{};
    f.index_size = p.consume_type<uint64_t>();
    f.version = p.consume_type<uint32_t>();
    f.magic = p.consume_type<uint32_t>();
    if (
      f.magic != staging_uploader::footer_magic
      || f.version != staging_uploader::footer_version) {
        return std::nullopt;
    }
    return f;
}

/// Walk raw disk-format batches; returns the sub-range of \p data whose
/// batches have base_offset > bound, plus offset bookkeeping. Returns
/// nullopt on parse failure.
struct trimmed_range {
    size_t byte_begin{0};
    model::offset first_base;
    model::offset last_offset;
    size_t batches{0};
    // translator-filtered batches above the bound, as [base, last]
    std::vector<std::pair<model::offset, model::offset>> gaps;
};

std::optional<trimmed_range>
trim_to_after(const iobuf& data, model::offset bound) {
    auto lin = iobuf_to_bytes(data); // staged extents are round-budget sized
    trimmed_range out{};
    size_t pos = 0;
    bool found = false;
    while (pos + model::packed_record_batch_header_size <= lin.size()) {
        auto hdr_bytes = std::string_view{
          reinterpret_cast<const char*>(lin.data() + pos),
          model::packed_record_batch_header_size};
        // fields at fixed little-endian offsets in the packed on-disk header:
        // [0..3] header_crc, [4..7] batch_size, [8..15] base_offset,
        // [16] type, [17..20] crc, [21..22] attrs, [23..26] last_offset_delta
        auto rd_i32 = [&](size_t off) {
            int32_t v;
            std::memcpy(&v, hdr_bytes.data() + off, sizeof(v));
            return v;
        };
        auto rd_i64 = [&](size_t off) {
            int64_t v;
            std::memcpy(&v, hdr_bytes.data() + off, sizeof(v));
            return v;
        };
        auto batch_size = rd_i32(4);
        auto base_offset = model::offset(rd_i64(8));
        auto delta = rd_i32(23);
        if (
          batch_size < int32_t(model::packed_record_batch_header_size)
          || pos + size_t(batch_size) > lin.size()) {
            return std::nullopt;
        }
        auto last = model::offset(base_offset() + delta);
        if (base_offset > bound) {
            if (!found) {
                out.byte_begin = pos;
                out.first_base = base_offset;
                found = true;
            }
            out.last_offset = last;
            out.batches++;
            static const auto translator_types
              = model::offset_translator_batch_types();
            auto type = model::record_batch_type(int8_t(hdr_bytes[16]));
            if (
              std::find(translator_types.begin(), translator_types.end(), type)
              != translator_types.end()) {
                out.gaps.emplace_back(base_offset, last);
            }
        } else if (found) {
            // batches are offset-ordered inside an extent; a below-bound
            // batch after an above-bound one means corruption
            return std::nullopt;
        }
        pos += size_t(batch_size);
    }
    if (pos != lin.size()) {
        return std::nullopt;
    }
    if (!found) {
        out.byte_begin = lin.size();
    }
    return out;
}

} // namespace

ss::future<staging_recovery_catalog> staging_recovery_catalog::build(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& parent,
  std::optional<model::cluster_uuid> source_cluster) {
    staging_recovery_catalog catalog;

    // NOTE: the catalog walks the full staging/ prefix. Scoping it to the
    // source cluster's identity would exclude objects staged by earlier
    // cluster generations that reused ntp names (revision fencing alone
    // cannot disambiguate those) and shrink the walk -- but neither
    // cluster_id nor cluster_uuid survives whole-cluster restore (both are
    // regenerated on the replacement cluster), so the local id is the WRONG
    // key here. Correct scoping requires threading the SOURCE cluster uuid
    // from the restore manifest into this build; until then the full walk
    // plus per-extent revision fencing is the safe, validated behavior.
    // Scope the walk to the SOURCE cluster's staging prefix when the restore
    // context names it (remote_label carries the source cluster_uuid, which a
    // fresh replacement cluster otherwise cannot know). This excludes objects
    // staged by earlier cluster generations that reused ntp names/revisions --
    // the pollution that a local-id scope could not solve since neither
    // cluster_id nor cluster_uuid survives WCR. No label (classic v1 tiered)
    // falls back to the full walk plus per-extent revision fencing.
    ss::sstring scoped_prefix{staging_prefix};
    if (source_cluster.has_value()) {
        scoped_prefix = ssx::sformat(
          "{}{}/", staging_prefix, source_cluster.value());
    }
    vlog(
      archival_log.info,
      "staging-recovery: catalog walk prefix {} (source-scoped={})",
      scoped_prefix,
      source_cluster.has_value());

    chunked_vector<cloud_storage_clients::client::list_bucket_item> items;
    std::optional<ss::sstring> continuation;
    while (true) {
        auto list = co_await remote.list_objects(
          bucket,
          parent,
          cloud_storage_clients::object_key{scoped_prefix},
          std::nullopt,
          std::nullopt,
          std::nullopt,
          continuation);
        if (list.has_error()) {
            vlog(
              archival_log.warn,
              "staging-recovery: listing {} failed; skipping staged-tail "
              "replay",
              staging_prefix);
            co_return catalog;
        }
        for (auto& item : list.value().contents) {
            items.push_back(std::move(item));
        }
        if (!list.value().is_truncated) {
            break;
        }
        continuation = list.value().next_continuation_token;
    }

    size_t objects = 0;
    size_t skipped = 0;
    // The walk is O(objects under staging/) with two range reads per
    // object: read concurrently, and give every object its own retry
    // budget. With a single shared budget the parent deadline expired
    // mid-walk on a large bucket and every later read failed as "not
    // available" (observed live: 300s of budget vs a ~7k-object walk).
    co_await ss::max_concurrent_for_each(
      items,
      16,
      [&](const cloud_storage_clients::client::list_bucket_item& item)
        -> ss::future<> {
          if (item.size_bytes <= footer_size) {
              skipped++;
              co_return;
          }
          retry_chain_node object_node(
            std::chrono::seconds(20), std::chrono::milliseconds(100), &parent);
          auto foot_buf = co_await download_range(
            remote,
            bucket,
            item.key,
            item.size_bytes - footer_size,
            item.size_bytes - 1,
            object_node);
          auto foot = foot_buf ? parse_footer(std::move(*foot_buf))
                               : std::nullopt;
          if (
            !foot || foot->index_size == 0
            || foot->index_size + footer_size > item.size_bytes) {
              vlog(
                archival_log.warn,
                "staging-recovery: {} has an unreadable footer; skipped",
                item.key);
              skipped++;
              co_return;
          }
          auto idx_begin = item.size_bytes - footer_size - foot->index_size;
          auto idx_buf = co_await download_range(
            remote,
            bucket,
            item.key,
            idx_begin,
            idx_begin + foot->index_size - 1,
            object_node);
          if (!idx_buf) {
              skipped++;
              co_return;
          }
          try {
              auto index = serde::from_iobuf<staging_index>(
                std::move(*idx_buf));
              for (auto& e : index.extents) {
                  catalog._extents[e.ntp].push_back(
                    staged_extent_ref{
                      .term = e.term,
                      .base = e.base,
                      .last = e.last,
                      .key = item.key,
                      .byte_offset = e.byte_offset,
                      .byte_len = e.byte_len,
                      .compacted = e.compacted,
                      .revision = e.revision,
                    });
              }
              objects++;
          } catch (...) {
              vlog(
                archival_log.warn,
                "staging-recovery: {} index decode failed ({}); skipped",
                item.key,
                std::current_exception());
              skipped++;
          }
      });
    for (auto& [ntp, extents] : catalog._extents) {
        std::sort(
          extents.begin(), extents.end(), [](const auto& a, const auto& b) {
              return std::tie(a.base, b.term) < std::tie(b.base, a.term);
          });
    }
    catalog._skipped_downloads = skipped;
    if (skipped > 0) {
        vlog(
          archival_log.error,
          "staging-recovery: {} staging object(s) unreadable after retries — "
          "the catalog may be missing extents and staged-tail replay may be "
          "INCOMPLETE",
          skipped);
    }
    vlog(
      archival_log.info,
      "staging-recovery: catalog built from {} object(s) ({} skipped), {} "
      "partition(s) have staged data",
      objects,
      skipped,
      catalog._extents.size());
    co_return catalog;
}

ss::future<staged_tail_result> materialize_staged_tail(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  const staging_recovery_catalog& catalog,
  const model::ntp& ntp,
  model::initial_revision_id revision,
  model::offset last_canonical,
  const std::filesystem::path& dir,
  retry_chain_node& parent) {
    staged_tail_result result{.max_offset = last_canonical};
    const auto* all_extents = catalog.find(ntp);
    if (all_extents == nullptr || all_extents->empty()) {
        co_return result;
    }
    // Only this topic incarnation's data may be replayed: extents staged by
    // an earlier cluster/topic incarnation that reused the ntp name carry a
    // different revision and are ignored.
    std::vector<staged_extent_ref> revision_extents;
    revision_extents.reserve(all_extents->size());
    for (const auto& e : *all_extents) {
        if (e.revision == revision) {
            revision_extents.push_back(e);
        }
    }
    const auto* extents = &revision_extents;
    if (extents->empty()) {
        vlog(
          archival_log.info,
          "staging-recovery: {} has {} staged extent(s) but none match "
          "revision {} (revisions present: first={}) — nothing to replay",
          ntp,
          all_extents->size(),
          revision,
          all_extents->front().revision);
        co_return result;
    }

    iobuf tail;
    auto expected = model::next_offset(last_canonical);
    model::term_id first_term{};
    // Extents are sorted by (base asc, term desc). Repeatedly take the
    // highest-term extent covering `expected`.
    while (true) {
        const staged_extent_ref* chosen = nullptr;
        for (const auto& e : *extents) {
            if (e.base > expected) {
                break; // sorted: nothing further can cover expected
            }
            if (e.last >= expected) {
                chosen = &e;
                break; // first hit is highest-term for this base range
            }
        }
        if (chosen == nullptr) {
            if (result.batches_applied == 0) {
                vlog(
                  archival_log.info,
                  "staging-recovery: {} no staged extent covers offset {} "
                  "({} extents at revision {}, range [{},{}]) — staged tail "
                  "not applicable",
                  ntp,
                  expected,
                  extents->size(),
                  revision,
                  extents->front().base,
                  extents->back().last);
            }
            break;
        }
        auto bytes = co_await download_range(
          remote,
          bucket,
          chosen->key,
          chosen->byte_offset,
          chosen->byte_offset + chosen->byte_len - 1,
          parent);
        if (!bytes) {
            vlog(
              archival_log.warn,
              "staging-recovery: {} extent [{},{}] in {} unreadable; replay "
              "stops at {}",
              ntp,
              chosen->base,
              chosen->last,
              chosen->key,
              model::prev_offset(expected));
            break;
        }
        auto trimmed = trim_to_after(*bytes, model::prev_offset(expected));
        if (!trimmed) {
            vlog(
              archival_log.warn,
              "staging-recovery: {} extent [{},{}] in {} failed batch walk; "
              "replay stops at {}",
              ntp,
              chosen->base,
              chosen->last,
              chosen->key,
              model::prev_offset(expected));
            break;
        }
        if (trimmed->batches == 0) {
            break;
        }
        if (trimmed->first_base != expected) {
            // Compacted source ranges legitimately contain offset gaps —
            // compaction removed records between the canonical end and the
            // staged batches. Chaining forward is safe: the strict
            // above-canonical filter already discarded anything the
            // canonical tier covers.
            if (chosen->compacted && trimmed->first_base > expected) {
                vlog(
                  archival_log.info,
                  "staging-recovery: {} compacted extent: chaining {} -> {}",
                  ntp,
                  expected,
                  trimmed->first_base);
            } else {
                vlog(
                  archival_log.warn,
                  "staging-recovery: {} gap: expected {} but staged batch "
                  "starts at {}; replay stops",
                  ntp,
                  expected,
                  trimmed->first_base);
                break;
            }
        }
        if (result.batches_applied == 0) {
            first_term = chosen->term;
        }
        result.last_term = chosen->term;
        result.translator_gaps.insert(
          result.translator_gaps.end(),
          trimmed->gaps.begin(),
          trimmed->gaps.end());
        auto slice = bytes->share(
          trimmed->byte_begin, bytes->size_bytes() - trimmed->byte_begin);
        result.batches_applied += trimmed->batches;
        tail.append(std::move(slice));
        expected = model::next_offset(trimmed->last_offset);
    }

    if (result.batches_applied == 0) {
        co_return result;
    }

    auto base = model::next_offset(last_canonical);
    auto filename = fmt::format("{}-{}-{}.log", base(), first_term(), "v1");
    auto path = dir / filename;
    auto file = co_await ss::open_file_dma(
      path.native(),
      ss::open_flags::create | ss::open_flags::rw | ss::open_flags::truncate);
    auto stream = co_await ss::make_file_output_stream(std::move(file));
    result.bytes_written = tail.size_bytes();
    co_await write_iobuf_to_output_stream(std::move(tail), stream);
    co_await stream.flush();
    co_await stream.close();

    result.max_offset = model::prev_offset(expected);
    vlog(
      archival_log.info,
      "staging-recovery: {} staged tail applied: ({},{}] — {} batches, {} "
      "bytes -> {}",
      ntp,
      last_canonical,
      result.max_offset,
      result.batches_applied,
      result.bytes_written,
      path.native());
    co_return result;
}

} // namespace archival
