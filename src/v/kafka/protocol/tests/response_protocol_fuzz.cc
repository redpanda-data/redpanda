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

#include "kafka/server/handlers/handlers.h"
#include "protocol_utils.h"

#include <vassert.h>

struct handler_interface {
    handler_interface() = default;
    handler_interface(const handler_interface&) = delete;
    handler_interface& operator=(const handler_interface&) = delete;
    handler_interface(handler_interface&&) noexcept = delete;
    handler_interface& operator=(handler_interface&&) noexcept = delete;
    virtual ~handler_interface() = default;

    virtual kafka::api_version min_supported() const = 0;
    virtual kafka::api_version max_supported() const = 0;
    virtual void operator()(iobuf, kafka::api_version) const = 0;
    virtual size_t generate_new_random_message(
      uint8_t* data, size_t max_size, kafka::api_version version) const = 0;
};

template<typename T>
struct handler_base final : public handler_interface {
    kafka::api_version min_supported() const override {
        return T::min_supported;
    }
    kafka::api_version max_supported() const override {
        return T::max_supported;
    }
    void operator()(iobuf data, kafka::api_version version) const override {
        decltype(T::api::response_type::data) r;
#if 1
        if constexpr (kafka::HasPrimitiveDecode<decltype(r)>) {
            r.decode(std::move(data), version);
        } else {
            kafka::request_reader reader(std::move(data));
            r.decode(reader, version);
        }
#else
        kafka::request_reader reader(std::move(data));
        r.decode(reader, version);
#endif

        auto _fmt = fmt::format("{}", r);

        iobuf write_buf;
        kafka::response_writer writer(write_buf);
        r.encode(writer, version);
        // vassert(write_buf == data, "Encode/decode does not match!");
    }
    size_t generate_new_random_message(
      uint8_t* data,
      size_t max_size,
      kafka::api_version version) const override {
        bytes result;
        do {
            result = invoke_franz_harness(
              T::api::key, version, kafka::is_kafka_request::no);
        } while (result.size() >= max_size);

        ::memcpy(data, result.data(), result.size());
        return result.size();
    }
};

template<typename H>
struct handler_holder {
    static const inline handler_base<H> instance{};
};

template<typename... Ts>
constexpr auto make_lut(kafka::type_list<Ts...>) {
    constexpr int max_index = std::max({Ts::api::key...});

    std::array<const handler_interface*, max_index + 1> lut{};
    ((lut[Ts::api::key] = &handler_holder<Ts>::instance), ...);

    return lut;
}

template<typename T>
T read(const uint8_t*& data, size_t& size) {
    typename T::type ret;
    std::memcpy(&ret, data, sizeof(T));
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    data += sizeof(T);
    size -= sizeof(T);
    return T(ret);
}

template<typename T>
T read(uint8_t*& data, size_t& size) {
    typename T::type ret;
    std::memcpy(&ret, data, sizeof(T));
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    data += sizeof(T);
    size -= sizeof(T);
    return T(ret);
}

template<typename T>
T read_no_update(const uint8_t* data) {
    typename T::type ret;
    std::memcpy(&ret, data, sizeof(T));
    return T(ret);
}

template<typename T>
void write(uint8_t*& data, const T& val, size_t& size) {
    std::memcpy(data, &val, sizeof(T));
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    data += sizeof(T);
    size += sizeof(T);
}

template<>
void write(uint8_t*& data, const iobuf& val, size_t& size) {
    for (auto& f : val) {
        if (f.is_empty()) {
            continue;
        }
        ::memcpy(data, f.get(), f.size());
        size += f.size();
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        data += f.size();
    }
}

template<typename T>
void write_no_update(uint8_t*& data, const T& val) {
    std::memcpy(data, &val, sizeof(T));
}

static constexpr auto ok = std::to_array<std::string_view>({
  "Null topics received for version 0 of metadata request",
  "Cannot decode string as UTF8",
  "sstring overflow",
});

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < (sizeof(kafka::api_key) + sizeof(kafka::api_version))) {
        return -1;
    }

    const auto key = kafka::api_key(read<kafka::api_key>(data, size));
    const auto ver = kafka::api_version(read<kafka::api_version>(data, size));

    static constexpr auto lut = make_lut(kafka::request_types{});
    if (key < kafka::api_key(0) || key >= kafka::api_key(lut.size())) {
        return -1;
    }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
    auto handler = lut[key];
    if (!handler) {
        return -1;
    }

    if (ver < handler->min_supported() || ver > handler->max_supported()) {
        return -1;
    }

    iobuf buf;
    buf.append(data, size);

    try {
        handler->operator()(std::move(buf), ver);
    } catch (const std::out_of_range&) {
        return -1;
    } catch (const std::bad_alloc&) {
        return -1;
    } catch (const std::runtime_error& e) {
        for (const auto& s : ok) {
            if (e.what() == s) {
                return -1;
            }
        }
        throw;
    }

    return 0;
}

#ifdef USE_CUSTOM_MUTATOR
// Forward-declare the libFuzzer's mutator callback.
extern "C" size_t LLVMFuzzerMutate(uint8_t* Data, size_t Size, size_t MaxSize);

extern "C" size_t LLVMFuzzerCustomMutator(
  uint8_t* data, size_t size, size_t max_size, unsigned int seed) {
    static constexpr size_t minimum_size = sizeof(kafka::api_key)
                                           + sizeof(kafka::api_version);
    // If immediately fed data that's too small, mutate the data until we have
    // something of appropriate length
    while (size < minimum_size) {
        size = LLVMFuzzerMutate(data, size, max_size);
    }

    static constexpr auto lut = make_lut(kafka::request_types{});
    auto key = kafka::api_key(read_no_update<kafka::api_key>(data));
    auto key_invalid = [](const kafka::api_key& key) -> bool {
        return key < kafka::api_key(0) || key >= kafka::api_key(lut.size());
    };
    auto key_valid = [](const kafka::api_key& key) -> bool {
        return key >= kafka::api_key(0) && key < kafka::api_key(lut.size());
    };
    // Now if we receive a key that's invalid, continue to mutate the data (and
    // maintain minimum size) until we have a good one
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
    while (key_invalid(key) || (key_valid(key) && !lut[key])) {
        do {
            size = LLVMFuzzerMutate(data, size, max_size);
        } while (size < minimum_size);
        key = kafka::api_key(read_no_update<kafka::api_key>(data));
    }
    // Now we have a good key, consume the data pointer
    read<kafka::api_key>(data, size);
    // Grab the version
    auto ver = kafka::api_version(read_no_update<kafka::api_version>(data));

    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
    auto handler = lut[key];
    assert(handler);

    // If the version is invalid, update the version to the max supported and
    // write it into the buffer
    if (ver < handler->min_supported() || ver > handler->max_supported()) {
        ver = handler->max_supported();
        write_no_update(data, ver);
    }
    auto ver_data_ptr = data;
    // now consume the version data
    read<kafka::api_version>(data, size);

    iobuf buf;
    buf.append(data, size);

    try {
        handler->operator()(std::move(buf), ver);
    } catch (const std::exception&) {
        // Pretty darn close in this situation!  Let's use the same handler
        // and form a new message
        auto new_message_size = handler->generate_new_random_message(
          data, max_size, ver);
        iobuf test_buf;
        test_buf.append(data, new_message_size);
        handler->operator()(std::move(test_buf), ver);
        return new_message_size + minimum_size;
    }

    // If the decode was successful, increment (or wrap around) the version
    if (ver == handler->max_supported()) {
        ver = handler->min_supported();
    } else {
        ver++;
    }
    write_no_update(ver_data_ptr, ver);
    return size + minimum_size;
}
#endif
