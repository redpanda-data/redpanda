/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/partition_manifest.h"

#include "bytes/iobuf_istreambuf.h"
#include "bytes/iobuf_ostreambuf.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/types.h"
#include "hashing/xx.h"
#include "json/istreamwrapper.h"
#include "json/ostreamwrapper.h"
#include "json/writer.h"
#include "model/timestamp.h"
#include "ssx/sformat.h"
#include "storage/fs_utils.h"
#include "utils/to_string.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

#include <fmt/ostream.h>
#include <rapidjson/error/en.h>

#include <algorithm>
#include <charconv>
#include <iterator>
#include <memory>
#include <optional>
#include <utility>

namespace fmt {
template<>
struct fmt::formatter<cloud_storage::partition_manifest::segment_meta> {
    using segment_meta = cloud_storage::partition_manifest::segment_meta;

    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(segment_meta const& m, FormatContext& ctx) {
        return fmt::format_to(
          ctx.out(),
          "{{o={}-{} t={}-{}}}",
          m.base_offset,
          m.committed_offset,
          m.base_timestamp,
          m.max_timestamp);
    }
};
} // namespace fmt

namespace cloud_storage {
std::ostream&
operator<<(std::ostream& s, const partition_manifest_path_components& c) {
    fmt::print(
      s, "{{{}: {}-{}-{}-{}}}", c._origin, c._ns, c._topic, c._part, c._rev);
    return s;
}

static bool parse_partition_and_revision(
  std::string_view s, partition_manifest_path_components& comp) {
    auto pos = s.find('_');
    if (pos == std::string_view::npos) {
        // Invalid segment file name
        return false;
    }
    uint64_t res = 0;
    // parse first component
    auto sv = s.substr(0, pos);
    auto e = std::from_chars(sv.data(), sv.data() + sv.size(), res);
    if (e.ec != std::errc()) {
        return false;
    }
    comp._part = model::partition_id(res);
    // parse second component
    sv = s.substr(pos + 1);
    e = std::from_chars(sv.data(), sv.data() + sv.size(), res);
    if (e.ec != std::errc()) {
        return false;
    }
    comp._rev = model::initial_revision_id(res);
    return true;
}

std::optional<partition_manifest_path_components>
get_partition_manifest_path_components(const std::filesystem::path& path) {
    // example: b0000000/meta/kafka/redpanda-test/4_2/manifest.json
    enum {
        ix_prefix,
        ix_meta,
        ix_namespace,
        ix_topic,
        ix_part_rev,
        ix_file_name,
        total_components
    };
    partition_manifest_path_components res;
    res._origin = path;
    int ix = 0;
    for (const auto& c : path) {
        ss::sstring p = c.string();
        switch (ix++) {
        case ix_prefix:
            break;
        case ix_namespace:
            res._ns = model::ns(std::move(p));
            break;
        case ix_topic:
            res._topic = model::topic(std::move(p));
            break;
        case ix_part_rev:
            if (!parse_partition_and_revision(p, res)) {
                return std::nullopt;
            }
            break;
        case ix_file_name:
            if (p != "manifest.json") {
                return std::nullopt;
            }
            break;
        }
    }
    if (ix == total_components) {
        return res;
    }
    return std::nullopt;
}

std::optional<segment_name_components>
parse_segment_name(const segment_name& name) {
    auto parsed = storage::segment_path::parse_segment_filename(name);
    if (!parsed) {
        return std::nullopt;
    }
    return segment_name_components{
      .base_offset = parsed->base_offset,
      .term = parsed->term,
    };
}

remote_segment_path generate_remote_segment_path(
  const model::ntp& ntp,
  model::initial_revision_id rev_id,
  const segment_name& name,
  model::term_id archiver_term) {
    vassert(
      rev_id != model::initial_revision_id(),
      "ntp {}: ntp revision must be known for segment {}",
      ntp,
      name);

    auto path = ssx::sformat("{}_{}/{}", ntp.path(), rev_id(), name());
    uint32_t hash = xxhash_32(path.data(), path.size());
    if (archiver_term != model::term_id{}) {
        return remote_segment_path(
          fmt::format("{:08x}/{}.{}", hash, path, archiver_term()));
    } else {
        return remote_segment_path(fmt::format("{:08x}/{}", hash, path));
    }
}

segment_name generate_local_segment_name(model::offset o, model::term_id t) {
    return segment_name(ssx::sformat("{}-{}-v1.log", o(), t()));
}

partition_manifest::partition_manifest()
  : _ntp()
  , _rev()
  , _last_offset(0) {}

partition_manifest::partition_manifest(
  model::ntp ntp, model::initial_revision_id rev)
  : _ntp(std::move(ntp))
  , _rev(rev)
  , _last_offset(0) {}

// NOTE: the methods that generate remote paths use the xxhash function
// to randomize the prefix. S3 groups the objects into chunks based on
// these prefixes. It also applies rate limit to chunks so if all segments
// and manifests will have the same prefix we will be able to do around
// 3000-5000 req/sec. AWS doc mentions that having only two prefix
// characters should be enough for most workloads
// (https://aws.amazon.com/blogs/aws/amazon-s3-performance-tips-tricks-seattle-hiring-event/)
// We're using eight because it's free and because AWS S3 is not the only
// backend and other S3 API implementations might benefit from that.

remote_manifest_path generate_partition_manifest_path(
  const model::ntp& ntp, model::initial_revision_id rev) {
    // NOTE: the idea here is to split all possible hash values into
    // 16 bins. Every bin should have lowest 28-bits set to 0.
    // As result, for segment names all prefixes are possible, but
    // for manifests, only 0x00000000, 0x10000000, ... 0xf0000000
    // are used. This will allow us to quickly find all manifests
    // that S3 bucket contains.
    constexpr uint32_t bitmask = 0xF0000000;
    auto path = ssx::sformat("{}_{}", ntp.path(), rev());
    uint32_t hash = bitmask & xxhash_32(path.data(), path.size());
    return remote_manifest_path(
      fmt::format("{:08x}/meta/{}_{}/manifest.json", hash, ntp.path(), rev()));
}

remote_manifest_path partition_manifest::get_manifest_path() const {
    return generate_partition_manifest_path(_ntp, _rev);
}

const model::ntp& partition_manifest::get_ntp() const { return _ntp; }

const model::offset partition_manifest::get_last_offset() const {
    return _last_offset;
}

std::optional<model::offset> partition_manifest::get_start_offset() const {
    if (_segments.empty()) {
        return std::nullopt;
    }
    return _segments.begin()->second.base_offset;
}

model::initial_revision_id partition_manifest::get_revision_id() const {
    return _rev;
}

remote_segment_path partition_manifest::generate_segment_path(
  const partition_manifest::key& key, const segment_meta& meta) const {
    auto name = generate_remote_segment_name(key, meta);
    return cloud_storage::generate_remote_segment_path(
      _ntp, meta.ntp_revision, name, meta.archiver_term);
}

segment_name partition_manifest::generate_remote_segment_name(
  const partition_manifest::key& k, const partition_manifest::value& val) {
    switch (val.sname_format) {
    case segment_name_format::v1:
        return segment_name(
          ssx::sformat("{}-{}-v1.log", k.base_offset(), k.term()));
    case segment_name_format::v2:
        // Use new stlyle format ".../base-committed-term-size-v1.log"
        return segment_name(ssx::sformat(
          "{}-{}-{}-{}-v1.log",
          k.base_offset(),
          val.committed_offset(),
          val.size_bytes,
          k.term()));
    }
    __builtin_unreachable();
}

remote_segment_path partition_manifest::generate_remote_segment_path(
  const model::ntp& ntp,
  const partition_manifest::key& k,
  const partition_manifest::value& val) {
    auto name = generate_remote_segment_name(k, val);
    return cloud_storage::generate_remote_segment_path(
      ntp, val.ntp_revision, name, val.archiver_term);
}

local_segment_path partition_manifest::generate_local_segment_path(
  const model::ntp& ntp,
  const partition_manifest::key& k,
  const partition_manifest::value& val) {
    auto name = cloud_storage::generate_local_segment_name(
      k.base_offset, k.term);
    return local_segment_path(
      fmt::format("{}_{}/{}", ntp.path(), val.ntp_revision, name()));
}

partition_manifest::const_iterator partition_manifest::begin() const {
    return _segments.begin();
}

partition_manifest::const_iterator partition_manifest::end() const {
    return _segments.end();
}

partition_manifest::const_reverse_iterator partition_manifest::rbegin() const {
    return _segments.rbegin();
}

partition_manifest::const_reverse_iterator partition_manifest::rend() const {
    return _segments.rend();
}

size_t partition_manifest::size() const { return _segments.size(); }

bool partition_manifest::contains(const partition_manifest::key& key) const {
    return _segments.contains(key);
}

bool partition_manifest::contains(const segment_name& name) const {
    auto maybe_key = parse_segment_name(name);
    if (!maybe_key) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "can't parse segment name \"{}\"", name));
    }
    key key = {.base_offset = maybe_key->base_offset, .term = maybe_key->term};
    return _segments.contains(key);
}

std::vector<partition_manifest::segment_name_meta>
partition_manifest::replaced_segments() const {
    std::vector<partition_manifest::segment_name_meta> segments;
    segments.reserve(_replaced.size());
    for (const auto& kv : _replaced) {
        segments.push_back(segment_name_meta{
          .name = generate_local_segment_name(
            kv.first.base_offset, kv.first.term),
          .meta = kv.second,
        });
    }
    return segments;
}

void partition_manifest::move_aligned_offset_range(
  model::offset begin_inclusive, model::offset end_inclusive) {
    auto k = key{
      .base_offset = begin_inclusive,
      .term = model::term_id::min(),
    };
    auto it = _segments.lower_bound(k);
    while (it != _segments.end()
           // The segment is considered replaced only if all its
           // offsets are covered by new segment's offset range
           && it->first.base_offset >= begin_inclusive
           && it->second.committed_offset <= end_inclusive) {
        _replaced.insert(*it);
        it = _segments.erase(it);
    }
}

bool partition_manifest::add(
  const partition_manifest::key& key, const segment_meta& meta) {
    move_aligned_offset_range(meta.base_offset, meta.committed_offset);
    auto [it, ok] = _segments.insert(std::make_pair(key, meta));
    if (ok && it->second.ntp_revision == model::initial_revision_id{}) {
        it->second.ntp_revision = _rev;
    }
    if (ok && it->second.segment_term == model::term_id{}) {
        it->second.segment_term = key.term;
    }
    _last_offset = std::max(meta.committed_offset, _last_offset);
    return ok;
}

bool partition_manifest::add(
  const segment_name& name, const segment_meta& meta) {
    auto maybe_key = parse_segment_name(name);
    if (!maybe_key) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "can't parse segment name \"{}\"", name));
    }
    key key = {.base_offset = maybe_key->base_offset, .term = maybe_key->term};
    return add(key, meta);
}

partition_manifest
partition_manifest::truncate(model::offset starting_rp_offset) {
    partition_manifest removed(_ntp, _rev);
    for (auto it : _segments) {
        if (it.second.committed_offset < starting_rp_offset) {
            removed.add(it.first, it.second);
        }
    }
    for (auto s : removed._segments) {
        _segments.erase(s.first);
    }
    return removed;
}

const partition_manifest::segment_meta*
partition_manifest::get(const partition_manifest::key& key) const {
    auto it = _segments.find(key);
    if (it == _segments.end()) {
        return nullptr;
    }
    return &it->second;
}

const partition_manifest::segment_meta*
partition_manifest::get(const segment_name& name) const {
    auto maybe_key = parse_segment_name(name);
    if (!maybe_key) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "can't parse segment name \"{}\"", name));
    }
    key key = {.base_offset = maybe_key->base_offset, .term = maybe_key->term};
    return get(key);
}

partition_manifest::const_iterator
partition_manifest::find(model::offset o) const {
    auto it = _segments.lower_bound(
      {.base_offset = o, .term = model::term_id(0)});
    if (it == _segments.end() || it->first.base_offset != o) {
        return end();
    }
    return it;
}

std::insert_iterator<partition_manifest::segment_map>
partition_manifest::get_insert_iterator() {
    return std::inserter(_segments, _segments.begin());
}

// clang-format off
/**
     ┌──────────────────────┐
     │ expect_manifest_start│
     └──────────┬───────────┘
                │ 
                │ 
                │ 
                │                                                                            EndObject()
                │  ┌────────────────────────────────────────────────────────────────────────────────────┐
                │  │                                                     Key("replaced")                |
                │  │  ┌───────────────────────────────────────────────────────────────────┐             |
                |  |  |                                                                   ▼             |
                │  │  │                       ┌───────────────┐                    ┌───────────────┐    |
                │  │  │      Key("segments")  │expect_segments│                    │expect_replaced│    |
   StartObject()│  │  │  ┌───────────────────►│    start      │                    │    start      │    |
                │  │  │  │                    └──────┬────────┘                    └──────┬────────┘    |
                │  │  │  │                           │StartObject()          StartObject()│             |
                ▼  ▼  ▼  │                           ▼                                    ▼             |
        ┌────────────────┴──┐                  ┌──────────────┐                    ┌───────────────┐    |
┌───────┤expect_manifest_key│◄─────────────────┤expect_segment│◄───┐               |expect_replaced│◄───┘
│       └──┬────────────────┘   EndObject()    │   path       │    │               │   path        │◄───┐
│          │          ▲                        └────┬─────────┘    │               └─────┬─────────┘    │
│     Key()│          │String()                     │              │                     │              │  
│          │          │Uint()                  Key()│              │                Key()│              │   
│          │          │Null()                       ▼              │EndObject()          ▼              │ 
│          ▼          │                       ┌──────────────┐     │              ┌───────────────┐     │     
│      ┌──────────────┴──────┐                │expect_segment│     │              │expect_replaced│     │     
│      │expect_manifest_value│                │ meta_start   │     │              │ meta_start    │     │      
│      └─────────────────────┘                └─────┬────────┘     │              └──────┬────────┘     │     
│                                                   │              │                     │              │
│                                      StartObject()│              │        StartObject()│              │
│EndObject()                                        ▼              │                     ▼              │
│                                             ┌───────────────┐    │              ┌────────────────┐    │
│                                             │ expect_segment├────┘              │ expect_replaced├────┘
│                                             │  meta_key     │                   │  meta_key      │
│            ┌────────┐                       └───┬───────────┘                   └────┬───────────┘
│            │terminal│                           │      ▲String()                     │     ▲String()
└───────────►│  state │                           │      │Uint()                       │     │Uint()
             └────────┘                      Key()│      │Bool()                  Key()│     │Bool()
                                                  ▼      │Null()                       ▼     │Null()
                                               ┌─────────┴─────┐                  ┌──────────┴─────┐
                                               │ expect_segment│                  │ expect_replaced│
                                               │  meta_value   │                  │  meta_value    │
                                               └───────────────┘                  └────────────────┘    
**/
// clang-format on

struct partition_manifest_handler
  : public rapidjson::
      BaseReaderHandler<rapidjson::UTF8<>, partition_manifest_handler> {
    using key_string = ss::basic_sstring<char, uint32_t, 31>;
    bool StartObject() {
        switch (_state) {
        case state::expect_manifest_start:
            _state = state::expect_manifest_key;
            return true;
        case state::expect_segments_start:
            _state = state::expect_segment_path;
            return true;
        case state::expect_replaced_start:
            _state = state::expect_replaced_path;
            return true;
        case state::expect_segment_meta_start:
            _state = state::expect_segment_meta_key;
            return true;
        case state::expect_replaced_meta_start:
            _state = state::expect_replaced_meta_key;
            return true;
        case state::expect_manifest_key:
        case state::expect_manifest_value:
        case state::expect_segment_path:
        case state::expect_segment_meta_key:
        case state::expect_segment_meta_value:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_key:
        case state::expect_replaced_meta_value:
        case state::terminal_state:
            return false;
        }
    }

    bool Key(const char* str, rapidjson::SizeType length, bool /*copy*/) {
        switch (_state) {
        case state::expect_manifest_key:
            _manifest_key = key_string(str, length);
            if (_manifest_key == "segments") {
                _state = state::expect_segments_start;
            } else if (_manifest_key == "replaced") {
                _state = state::expect_replaced_start;
            } else {
                _state = state::expect_manifest_value;
            }
            return true;
        case state::expect_segment_path:
        case state::expect_replaced_path:
            _segment_name = segment_name{str};
            _parsed_segment_key = parse_segment_name(_segment_name);
            if (!_parsed_segment_key) {
                throw std::runtime_error(fmt_with_ctx(
                  fmt::format,
                  "can't parse segment name \"{}\"",
                  _segment_name));
            }
            _segment_key = {
              .base_offset = _parsed_segment_key->base_offset,
              .term = _parsed_segment_key->term};
            if (_state == state::expect_segment_path) {
                _state = state::expect_segment_meta_start;
            } else {
                _state = state::expect_replaced_meta_start;
            }
            return true;
        case state::expect_segment_meta_key:
            _segment_meta_key = key_string(str, length);
            _state = state::expect_segment_meta_value;
            return true;
        case state::expect_replaced_meta_key:
            _segment_meta_key = key_string(str, length);
            _state = state::expect_replaced_meta_value;
            return true;
        case state::expect_manifest_start:
        case state::expect_manifest_value:
        case state::expect_segments_start:
        case state::expect_replaced_start:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_value:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_value:
        case state::terminal_state:
            return false;
        }
    }

    bool String(const char* str, rapidjson::SizeType length, bool /*copy*/) {
        std::string_view sv(str, length);
        switch (_state) {
        case state::expect_manifest_value:
            if ("namespace" == _manifest_key) {
                _namespace = model::ns(ss::sstring(sv));
            } else if ("topic" == _manifest_key) {
                _topic = model::topic(ss::sstring(sv));
            } else {
                return false;
            }
            _state = state::expect_manifest_key;
            return true;
        case state::expect_manifest_start:
        case state::expect_manifest_key:
        case state::expect_segments_start:
        case state::expect_replaced_start:
        case state::expect_segment_path:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_key:
        case state::expect_segment_meta_value:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_key:
        case state::expect_replaced_meta_value:
        case state::terminal_state:
            return false;
        }
    }

    bool Uint(unsigned u) { return Uint64(u); }

    bool Uint64(uint64_t u) {
        switch (_state) {
        case state::expect_manifest_value:
            if ("version" == _manifest_key) {
                _version = u;
            } else if ("partition" == _manifest_key) {
                _partition_id = model::partition_id(u);
            } else if ("revision" == _manifest_key) {
                _revision_id = model::initial_revision_id(u);
            } else if ("last_offset" == _manifest_key) {
                _last_offset = model::offset(u);
            } else {
                return false;
            }
            _state = state::expect_manifest_key;
            return true;
        case state::expect_replaced_meta_value:
        case state::expect_segment_meta_value:
            if ("size_bytes" == _segment_meta_key) {
                _size_bytes = static_cast<size_t>(u);
            } else if ("base_offset" == _segment_meta_key) {
                _base_offset = model::offset(u);
            } else if ("committed_offset" == _segment_meta_key) {
                _committed_offset = model::offset(u);
            } else if ("base_timestamp" == _segment_meta_key) {
                _base_timestamp = model::timestamp(u);
            } else if ("max_timestamp" == _segment_meta_key) {
                _max_timestamp = model::timestamp(u);
            } else if ("delta_offset" == _segment_meta_key) {
                _delta_offset = model::offset_delta(u);
            } else if ("ntp_revision" == _segment_meta_key) {
                _ntp_revision = model::initial_revision_id(u);
            } else if ("archiver_term" == _segment_meta_key) {
                _archiver_term = model::term_id(u);
            } else if ("segment_term" == _segment_meta_key) {
                _segment_term = model::term_id(u);
            } else if ("delta_offset_end" == _segment_meta_key) {
                _delta_offset_end = model::offset_delta(u);
            } else if ("sname_format" == _segment_meta_key) {
                _meta_sname_format = segment_name_format(u);
            }
            if (_state == state::expect_segment_meta_value) {
                _state = state::expect_segment_meta_key;
            } else {
                _state = state::expect_replaced_meta_key;
            }
            return true;
        case state::expect_manifest_start:
        case state::expect_manifest_key:
        case state::expect_segments_start:
        case state::expect_replaced_start:
        case state::expect_segment_path:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_key:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_key:
        case state::terminal_state:
            return false;
        }
    }

    bool EndObject(rapidjson::SizeType /*size*/) {
        switch (_state) {
        case state::expect_manifest_key:
            check_manifest_fields_are_present();
            _state = state::terminal_state;
            return true;
        case state::expect_segment_path:
        case state::expect_replaced_path:
            _state = state::expect_manifest_key;
            return true;
        case state::expect_segment_meta_key:
        case state::expect_replaced_meta_key:
            check_that_required_meta_fields_are_present();
            _meta = {
              .is_compacted = _is_compacted.value(),
              .size_bytes = _size_bytes.value(),
              .base_offset = _base_offset.value(),
              .committed_offset = _committed_offset.value(),
              .base_timestamp = _base_timestamp.value_or(
                model::timestamp::missing()),
              .max_timestamp = _max_timestamp.value_or(
                model::timestamp::missing()),
              .delta_offset = _delta_offset.value_or(
                model::offset_delta::min()),
              .ntp_revision = _ntp_revision.value_or(
                _revision_id.value_or(model::initial_revision_id())),
              .archiver_term = _archiver_term.value_or(model::term_id{}),
              .segment_term = _segment_term.value_or(_segment_key.term),
              .delta_offset_end = _delta_offset_end.value_or(
                model::offset_delta::min()),
              .sname_format = _meta_sname_format.value_or(
                segment_name_format::v1)};
            if (_state == state::expect_segment_meta_key) {
                if (!_segments) {
                    _segments = std::make_unique<segment_map>();
                }
                _segments->insert(std::make_pair(_segment_key, _meta));
                _state = state::expect_segment_path;
            } else {
                if (!_replaced) {
                    _replaced = std::make_unique<segment_multimap>();
                }
                _replaced->insert(std::make_pair(_segment_key, _meta));
                _state = state::expect_replaced_path;
            }
            clear_meta_fields();
            return true;
        case state::expect_manifest_start:
        case state::expect_manifest_value:
        case state::expect_segments_start:
        case state::expect_replaced_start:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_value:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_value:
        case state::terminal_state:
            return false;
        }
    }

    bool Bool(bool b) {
        switch (_state) {
        case state::expect_segment_meta_value:
            if ("is_compacted" == _segment_meta_key) {
                _is_compacted = b;
                _state = state::expect_segment_meta_key;
                return true;
            }
            return false;
        case state::expect_replaced_meta_value:
            if ("is_compacted" == _segment_meta_key) {
                _is_compacted = b;
                _state = state::expect_replaced_meta_key;
                return true;
            }
            return false;
        case state::expect_manifest_start:
        case state::expect_manifest_key:
        case state::expect_manifest_value:
        case state::expect_segments_start:
        case state::expect_segment_path:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_key:
        case state::expect_replaced_start:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_key:
        case state::terminal_state:
            return false;
        }
    }

    bool Null() {
        switch (_state) {
        case state::expect_manifest_value:
            _state = state::expect_manifest_key;
            return true;
        case state::expect_segment_meta_value:
            _state = state::expect_segment_meta_key;
            return true;
        case state::expect_manifest_start:
        case state::expect_manifest_key:
        case state::expect_segments_start:
        case state::expect_segment_path:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_key:
        case state::expect_replaced_start:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_key:
        case state::expect_replaced_meta_value:
            _state = state::expect_replaced_meta_key;
            return true;
        case state::terminal_state:
            return false;
        }
    }

    bool Default() { return false; }

    enum class state {
        expect_manifest_start,
        expect_manifest_key,
        expect_manifest_value,
        expect_segments_start,
        expect_segment_path,
        expect_segment_meta_start,
        expect_segment_meta_key,
        expect_segment_meta_value,
        expect_replaced_start,
        expect_replaced_path,
        expect_replaced_meta_start,
        expect_replaced_meta_key,
        expect_replaced_meta_value,
        terminal_state,
    } _state{state::expect_manifest_start};

    using segment_map = partition_manifest::segment_map;
    using segment_multimap = partition_manifest::segment_multimap;

    key_string _manifest_key;
    key_string _segment_meta_key;
    segment_name _segment_name;
    std::optional<segment_name_components> _parsed_segment_key;
    partition_manifest::key _segment_key;
    partition_manifest::segment_meta _meta;
    std::unique_ptr<segment_map> _segments;
    std::unique_ptr<segment_multimap> _replaced;

    // required manifest fields
    std::optional<int32_t> _version;
    std::optional<model::ns> _namespace;
    std::optional<model::topic> _topic;
    std::optional<int32_t> _partition_id;
    std::optional<model::initial_revision_id> _revision_id;
    std::optional<model::offset> _last_offset;

    // required segment meta fields
    std::optional<bool> _is_compacted;
    std::optional<size_t> _size_bytes;
    std::optional<model::offset> _base_offset;
    std::optional<model::offset> _committed_offset;

    // optional segment meta fields
    std::optional<model::timestamp> _base_timestamp;
    std::optional<model::timestamp> _max_timestamp;
    std::optional<model::offset_delta> _delta_offset;
    std::optional<model::initial_revision_id> _ntp_revision;
    std::optional<model::term_id> _archiver_term;
    std::optional<model::term_id> _segment_term;
    std::optional<model::offset_delta> _delta_offset_end;
    std::optional<segment_name_format> _meta_sname_format;

    void check_that_required_meta_fields_are_present() {
        if (!_is_compacted) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Missing is_compacted value in {} segment meta",
              _segment_name));
        }
        if (!_size_bytes) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Missing size_bytes value in {} segment meta",
              _segment_name));
        }
        if (!_base_offset) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Missing base_offset value in {} segment meta",
              _segment_name));
        }
        if (!_committed_offset) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Missing committed_offset value in {} segment meta",
              _segment_name));
        }
    }

    void clear_meta_fields() {
        // required fields
        _is_compacted = std::nullopt;
        _size_bytes = std::nullopt;
        _base_offset = std::nullopt;
        _committed_offset = std::nullopt;

        // optional segment meta fields
        _base_timestamp = std::nullopt;
        _max_timestamp = std::nullopt;
        _delta_offset = std::nullopt;
        _ntp_revision = std::nullopt;
        _archiver_term = std::nullopt;
        _segment_term = std::nullopt;
        _delta_offset_end = std::nullopt;
        _meta_sname_format = std::nullopt;
    }

    void check_manifest_fields_are_present() {
        if (!_version) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing version value partition manifest"));
        }
        if (!_namespace) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing namespace value in partition manifest"));
        }
        if (!_topic) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing topic value in partition manifest"));
        }
        if (!_partition_id) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing partition_id value in partition manifest"));
        }
        if (!_revision_id) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing revision_id value in partition manifest"));
        }
        if (!_last_offset) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing last_offset value in partition manifest"));
        }
    }
};

ss::future<> partition_manifest::update(ss::input_stream<char> is) {
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os);
    iobuf_istreambuf ibuf(result);
    std::istream stream(&ibuf);
    json::IStreamWrapper wrapper(stream);
    rapidjson::Reader reader;
    partition_manifest_handler handler;

    if (reader.Parse(wrapper, handler)) {
        partition_manifest::update(std::move(handler));
    } else {
        rapidjson::ParseErrorCode e = reader.GetParseErrorCode();
        size_t o = reader.GetErrorOffset();
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Failed to parse topic manifest {}: {} at offset {}",
          get_manifest_path(),
          rapidjson::GetParseError_En(e),
          o));
    }
    co_return;
}

void partition_manifest::update(partition_manifest_handler&& handler) {
    if (handler._version != static_cast<int>(manifest_version::v1)) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "partition manifest version {} is not supported",
          handler._version));
    }
    _rev = handler._revision_id.value();
    _ntp = model::ntp(
      handler._namespace.value(),
      handler._topic.value(),
      handler._partition_id.value());
    _last_offset = handler._last_offset.value();

    if (handler._segments) {
        _segments = std::move(*handler._segments);
    }
    if (handler._replaced) {
        _replaced = std::move(*handler._replaced);
    }
}

serialized_json_stream partition_manifest::serialize() const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialize(os);
    if (!os.good()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "could not serialize partition manifest {}",
          get_manifest_path()));
    }
    size_t size_bytes = serialized.size_bytes();
    return {
      .stream = make_iobuf_input_stream(std::move(serialized)),
      .size_bytes = size_bytes};
}

void partition_manifest::serialize(std::ostream& out) const {
    json::OStreamWrapper wrapper(out);
    json::Writer<json::OStreamWrapper> w(wrapper);
    w.StartObject();
    w.Key("version");
    w.Int(static_cast<int>(manifest_version::v1));
    w.Key("namespace");
    w.String(_ntp.ns().c_str());
    w.Key("topic");
    w.String(_ntp.tp.topic().c_str());
    w.Key("partition");
    w.Int64(_ntp.tp.partition());
    w.Key("revision");
    w.Int64(_rev());
    w.Key("last_offset");
    w.Int64(_last_offset());
    auto serialize_meta = [this, &w](const key& key, const segment_meta& meta) {
        auto sn = generate_local_segment_name(key.base_offset, key.term);
        w.Key(sn());
        w.StartObject();
        w.Key("is_compacted");
        w.Bool(meta.is_compacted);
        w.Key("size_bytes");
        w.Int64(meta.size_bytes);
        w.Key("committed_offset");
        w.Int64(meta.committed_offset());
        w.Key("base_offset");
        w.Int64(meta.base_offset());
        if (meta.base_timestamp != model::timestamp::missing()) {
            w.Key("base_timestamp");
            w.Int64(meta.base_timestamp.value());
        }
        if (meta.max_timestamp != model::timestamp::missing()) {
            w.Key("max_timestamp");
            w.Int64(meta.max_timestamp.value());
        }
        if (meta.delta_offset != model::offset_delta::min()) {
            w.Key("delta_offset");
            w.Int64(meta.delta_offset());
        }
        if (meta.ntp_revision != _rev) {
            vassert(
              meta.ntp_revision != model::initial_revision_id(),
              "ntp {}: missing ntp_revision for segment {} in the manifest",
              _ntp,
              sn);
            w.Key("ntp_revision");
            w.Int64(meta.ntp_revision());
        }
        if (meta.archiver_term != model::term_id::min()) {
            w.Key("archiver_term");
            w.Int64(meta.archiver_term());
        }
        w.Key("segment_term");
        if (meta.segment_term == model::term_id::min()) {
            w.Int64(key.term());
        } else {
            w.Int64(meta.segment_term());
        }
        if (
          meta.sname_format == segment_name_format::v2
          && meta.delta_offset_end != model::offset_delta::min()) {
            w.Key("delta_offset_end");
            w.Int64(meta.delta_offset_end());
        }
        if (meta.sname_format != segment_name_format::v1) {
            w.Key("sname_format");
            w.Int64(static_cast<int16_t>(meta.sname_format));
        }
        w.EndObject();
    };
    if (!_segments.empty()) {
        w.Key("segments");
        w.StartObject();
        for (const auto& [key, meta] : _segments) {
            serialize_meta(key, meta);
        }
        w.EndObject();
    }
    if (!_replaced.empty()) {
        w.Key("replaced");
        w.StartObject();
        for (const auto& [key, meta] : _replaced) {
            serialize_meta(key, meta);
        }
        w.EndObject();
    }
    w.EndObject();
}

bool partition_manifest::delete_permanently(
  const partition_manifest::key& key) {
    auto it = _segments.find(key);
    if (it != _segments.end()) {
        _segments.erase(it);
        return true;
    }
    return false;
}

partition_manifest::const_iterator
partition_manifest::segment_containing(model::offset o) const {
    if (o > _last_offset || _segments.empty()) {
        return end();
    }

    // Make sure to only compare based on the offset and not the term.
    auto it = _segments.upper_bound(
      key{.base_offset = o, .term = model::term_id::max()});
    if (it == _segments.begin()) {
        return end();
    }

    it = std::prev(it);
    if (it->first.base_offset <= o && it->second.committed_offset >= o) {
        return it;
    }

    // o lies in a gap in manifest
    return end();
}

/**
 * Look up the first segment containing timestamps >= the query timestamp.
 *
 * We do not keep a direct index of timestamp to segment, because
 * it is relatively rarely used, and would have a high memory cost.
 * In the absence of a random-access container of timestamps,
 * we may use offset as an indirect way of sampling the segments
 * for a bisection search, and use the total partition min+max
 * timestamps to establish a good initial guess: for a partition
 * with evenly spaced messages over time, this will result in very
 * fast lookup.
 * The procedure is:
 * 1. Make an initial guess by interpolating the timestamp between
 *    the partitions min+max timestamp and mapping that to an offset
 * 2. Convert offset guess into segment guess.  This is not a strictly
 *    correct offset lookup, we just want something close.
 * 3. Do linear search backward or forward from the location
 *    we probed, to find the right segment.
 */
std::optional<std::reference_wrapper<const partition_manifest::segment_meta>>
partition_manifest::timequery(model::timestamp t) const {
    if (_segments.empty()) {
        return std::nullopt;
    }

    auto base_t = _segments.begin()->second.base_timestamp;
    auto max_t = _segments.rbegin()->second.max_timestamp;
    auto base_offset = _segments.begin()->second.base_offset;
    auto max_offset = _segments.rbegin()->second.committed_offset;

    // Fast handling of bounds/edge cases to simplify subsequent
    // arithmetic steps
    if (t < base_t) {
        return _segments.begin()->second;
    } else if (t > max_t) {
        return std::nullopt;
    } else if (max_t == base_t) {
        // This is plausible in the case of a single segment with
        // a single batch using LogAppendTime.
        return _segments.begin()->second;
    }

    // Single-offset case should have hit max_t==base_t above
    vassert(
      max_offset > base_offset,
      "Unexpected offsets {} {} (times {} {})",
      base_offset,
      max_offset,
      base_t,
      max_t);

    // From this point on, we have finite positive differences between
    // base and max for both offset and time.
    model::offset interpolated_offset
      = base_offset
        + model::offset{
          (float(t() - base_t()) / float(max_t() - base_t()))
          * float(max_offset() - base_offset())};

    vlog(
      cst_log.debug, "timequery t={} offset guess {}", t, interpolated_offset);

    // 2. Convert offset guess into segment guess.  This is not a strictly
    // correct offset lookup, we just want something close.
    auto segment_iter = _segments.lower_bound(
      key{interpolated_offset, model::term_id{-1}});
    if (segment_iter == _segments.end()) {
        segment_iter = --_segments.end();
    }
    vlog(
      cst_log.debug,
      "timequery t={} segment initial guess {}",
      t,
      segment_iter->second);

    // 3. Do linear search backward or forward from the location
    //    we probed, to find the right segment.
    if (segment_iter->second.base_timestamp < t) {
        // Our guess's base_timestamp is before the search point, so our
        // result must be this segment or a later one: search forward
        for (; segment_iter != _segments.end(); ++segment_iter) {
            auto base_timestamp = segment_iter->second.base_timestamp;
            auto max_timestamp = segment_iter->second.max_timestamp;
            if (max_timestamp >= t) {
                // We found a segment bounding the search point
                break;
            } else if (base_timestamp > t) {
                // We found the first segment after the search point
                break;
            }
        }

        return segment_iter->second;
    } else {
        // Search backwards: we must peek at each preceding segment
        // to see if its max_timestamp is >= the query t, before
        // deciding whether to walk back to it.
        vlog(
          cst_log.debug, "timequery t={} reverse {}", t, segment_iter->second);

        while (segment_iter != _segments.begin()) {
            vlog(
              cst_log.debug,
              "timequery t={} reverse {}",
              t,
              segment_iter->second);
            auto prev = std::prev(segment_iter);
            if (prev->second.max_timestamp >= t) {
                segment_iter = prev;
            } else {
                break;
            }
        }
        return segment_iter->second;
    }
}

std::ostream& operator<<(std::ostream& o, const partition_manifest::key& k) {
    o << generate_local_segment_name(k.base_offset, k.term);
    return o;
}
} // namespace cloud_storage
