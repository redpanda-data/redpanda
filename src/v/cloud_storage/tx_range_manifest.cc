/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_storage/tx_range_manifest.h"

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "bytes/streambuf.h"
#include "cloud_storage/types.h"
#include "container/chunked_vector.h"
#include "json/istreamwrapper.h"
#include "json/ostreamwrapper.h"
#include "json/reader.h"
#include "json/writer.h"
#include "model/record.h"

#include <rapidjson/error/en.h>

#include <stdexcept>

namespace cloud_storage {

struct tx_range_manifest_json_handler {
    using Ch = rapidjson::UTF8<>::Ch;
    using SizeType = rapidjson::SizeType;

    // rapidjson's Handler concept.
    bool Null() { return false; }
    bool Bool(bool) { return false; }
    bool Int(int value) { return Int64(value); }
    bool Uint(unsigned value) { return Int64(value); }
    bool Int64(int64_t value) {
        switch (_state) {
        case state::expect_version_value:
            version = static_cast<int>(value);
            _manifest_keys |= static_cast<uint8_t>(manifest_keys::version);
            _state = state::expect_manifest_key;
            return true;
        case state::expect_compat_version_value:
            compat_version = static_cast<int>(value);
            if (
              compat_version
              > static_cast<int>(tx_range_manifest_version::current_version)) {
                throw std::runtime_error(fmt::format(
                  "Can't deserialize tx manifest, supported version {}, "
                  "manifest "
                  "version {}, compatible version {}",
                  static_cast<int32_t>(
                    tx_range_manifest_version::current_version),
                  version,
                  compat_version));
            }
            _manifest_keys |= static_cast<uint8_t>(
              manifest_keys::compat_version);
            _state = state::expect_manifest_key;
            return true;
        case state::expect_range_pid_id_value:
            ranges.back().pid.id = model::producer_id(value);
            _range_keys |= static_cast<uint8_t>(range_keys::pid_id);
            _state = state::expect_range_key;
            return true;
        case state::expect_range_pid_epoch_value:
            if (
              value < std::numeric_limits<int16_t>::min()
              || value > std::numeric_limits<int16_t>::max()) {
                return false;
            }
            ranges.back().pid.epoch = model::producer_epoch(
              static_cast<int16_t>(value));
            _range_keys |= static_cast<uint8_t>(range_keys::pid_epoch);
            _state = state::expect_range_key;
            return true;
        case state::expect_range_first_value:
            ranges.back().first = model::offset(value);
            _range_keys |= static_cast<uint8_t>(range_keys::first);
            _state = state::expect_range_key;
            return true;
        case state::expect_range_last_value:
            ranges.back().last = model::offset(value);
            _range_keys |= static_cast<uint8_t>(range_keys::last);
            _state = state::expect_range_end_object;
            return true;
        default:
            return false;
        }
    }
    bool Uint64(uint64_t value) {
        if (value > std::numeric_limits<int64_t>::max()) {
            return false;
        } else {
            return Int64(static_cast<int64_t>(value));
        }
    }
    bool Double(double) { return false; }
    /// enabled via kParseNumbersAsStringsFlag, string is not null-terminated
    /// (use length)
    bool
    RawNumber(const Ch* /*str*/, rapidjson::SizeType /*len*/, bool /*copy*/) {
        return false;
    }
    bool String(const Ch*, rapidjson::SizeType, bool) { return false; }
    bool StartObject() {
        switch (_state) {
        case state::expect_manifest_object:
            _state = state::expect_manifest_key;
            return true;
        case state::expect_ranges_entry_or_end_array:
            ranges.push_back(model::tx_range());
            _range_keys = 0;
            _state = state::expect_range_key;
            return true;
        default:
            return false;
        }
    }
    bool Key(const Ch* str, SizeType len, bool /*copy*/) {
        std::string_view sv(str, len);
        switch (_state) {
        case state::expect_manifest_key: {
            auto s{
              string_switch<std::optional<state>>(sv)
                .match("version", state::expect_version_value)
                .match("compat_version", state::expect_compat_version_value)
                .match("ranges", state::expect_ranges_value)
                .default_match(std::nullopt)};
            if (s.has_value()) {
                _state = *s;
            }
            return s.has_value();
        }
        case state::expect_range_key: {
            auto s{string_switch<std::optional<state>>(sv)
                     .match("pid.id", state::expect_range_pid_id_value)
                     .match("pid.epoch", state::expect_range_pid_epoch_value)
                     .match("first", state::expect_range_first_value)
                     .match("last", state::expect_range_last_value)
                     .default_match(std::nullopt)};
            if (s.has_value()) {
                _state = *s;
            }
            return s.has_value();
        }
        default:
            return false;
        }
    }
    bool EndObject(SizeType) {
        switch (_state) {
        case state::expect_range_end_object:
            if (!_range_keys.all()) {
                return false;
            }
            _state = state::expect_ranges_entry_or_end_array;
            return true;
        case state::expect_manifest_end_object:
            if (!_manifest_keys.all()) {
                return false;
            }
            _state = state::expect_done;
            return true;
        default:
            return false;
        }
    }
    bool StartArray() {
        switch (_state) {
        case state::expect_ranges_value:
            _state = state::expect_ranges_entry_or_end_array;
            return true;
        default:
            return false;
        }
    }
    bool EndArray(SizeType) {
        switch (_state) {
        case state::expect_ranges_entry_or_end_array:
            ranges.shrink_to_fit();
            _manifest_keys |= static_cast<uint8_t>(manifest_keys::ranges);
            _state = state::expect_manifest_end_object;
            return true;
        default:
            return false;
        }
    }

    // Handler state.
    enum class state {
        expect_manifest_object,
        expect_manifest_key,
        expect_version_value,
        expect_compat_version_value,
        expect_ranges_value,
        expect_ranges_entry_or_end_array,
        expect_range_key,
        expect_range_pid_id_value,
        expect_range_pid_epoch_value,
        expect_range_first_value,
        expect_range_last_value,
        expect_range_end_object,
        expect_manifest_end_object,
        expect_done,
    } _state{state::expect_manifest_object};

    // Per object tracking state.
    enum class manifest_keys : uint8_t {
        version = 1u,
        compat_version = 1u << 1u,
        ranges = 1u << 2u,
    };
    std::bitset<3> _manifest_keys = 0;

    enum class range_keys : uint8_t {
        pid_id = 1u,
        pid_epoch = 1u << 1u,
        first = 1u << 2u,
        last = 1u << 3u,
    };
    std::bitset<4> _range_keys = 0;

    // User data.
    int version{-1};
    int compat_version{-1};
    chunked_vector<model::tx_range> ranges;
};

remote_manifest_path generate_remote_tx_path(const remote_segment_path& path) {
    return remote_manifest_path(fmt::format("{}.tx", path().native()));
}

tx_range_manifest::tx_range_manifest(
  remote_segment_path spath, chunked_vector<model::tx_range> range)
  : _path(std::move(spath))
  , _ranges(std::move(range)) {}

tx_range_manifest::tx_range_manifest(remote_segment_path spath)
  : _path(std::move(spath)) {}

ss::future<> tx_range_manifest::update(ss::input_stream<char> is) {
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os).finally([&is, &os]() mutable {
        return is.close().finally([&os]() mutable { return os.close(); });
    });
    iobuf_istreambuf ibuf(result);
    std::istream stream(&ibuf);
    json::IStreamWrapper wrapper(stream);

    json::Reader reader;
    tx_range_manifest_json_handler handler;

    if (reader.Parse(wrapper, handler)) {
        if (
          handler._state
          != tx_range_manifest_json_handler::state::expect_done) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to parse tx range manifest {} stopped early {}",
              _path,
              static_cast<
                std::underlying_type_t<tx_range_manifest_json_handler::state>>(
                handler._state)));
        }
        _ranges = std::move(handler.ranges);
    } else {
        rapidjson::ParseErrorCode e = reader.GetParseErrorCode();
        size_t o = reader.GetErrorOffset();

        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Failed to parse tx range manifest {} at offset {}: {}",
          _path,
          o,
          rapidjson::GetParseError_En(e)));
    }
}

ss::future<iobuf> tx_range_manifest::serialize_buf() const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialize_ostream(os);
    if (!os.good()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "could not serialize tx range manifest {}",
          get_manifest_path()));
    }
    co_return serialized;
}

remote_manifest_path tx_range_manifest::get_manifest_path() const {
    return generate_remote_tx_path(_path);
}

void tx_range_manifest::serialize_ostream(std::ostream& out) const {
    json::OStreamWrapper wrapper(out);
    json::Writer<json::OStreamWrapper> w(wrapper);
    w.StartObject();
    w.Key("version");
    w.Int(static_cast<int>(tx_range_manifest_version::current_version));
    w.Key("compat_version");
    w.Int(static_cast<int>(tx_range_manifest_version::compat_version));
    w.Key("ranges");
    w.StartArray();
    for (const auto& tx : _ranges) {
        w.StartObject();
        w.Key("pid.id");
        w.Int64(tx.pid.id);
        w.Key("pid.epoch");
        w.Int(tx.pid.epoch);
        w.Key("first");
        w.Int64(tx.first());
        w.Key("last");
        w.Int64(tx.last());
        w.EndObject();
    }
    w.EndArray();
    w.EndObject();
}

size_t tx_range_manifest::estimate_serialized_size() const {
    constexpr auto total_keys_size_per_range = 36;
    constexpr auto avg_value_bytes = 40;
    return _ranges.size() * (total_keys_size_per_range + avg_value_bytes);
}
} // namespace cloud_storage
