/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/values.h"

#include "bytes/hash.h"
#include "bytes/iobuf_parser.h"

#include <boost/container_hash/hash_fwd.hpp>
#include <fmt/format.h>

namespace iceberg {

namespace {

struct primitive_copying_visitor {
    template<typename PrimitiveT>
    primitive_value operator()(const PrimitiveT& v) const {
        return v;
    }
    primitive_value operator()(const string_value& v) const {
        return string_value{v.val.copy()};
    }
    primitive_value operator()(const fixed_value& v) const {
        return fixed_value{v.val.copy()};
    }
    primitive_value operator()(const binary_value& v) const {
        return binary_value{v.val.copy()};
    }
};

struct primitive_hashing_visitor {
    size_t operator()(const boolean_value& v) const {
        return std::hash<bool>()(v.val);
    }
    size_t operator()(const int_value& v) const {
        return std::hash<int>()(v.val);
    }
    size_t operator()(const long_value& v) const {
        return std::hash<int64_t>()(v.val);
    }
    size_t operator()(const float_value& v) const {
        return std::hash<float>()(v.val);
    }
    size_t operator()(const double_value& v) const {
        return std::hash<double>()(v.val);
    }
    size_t operator()(const date_value& v) const {
        return std::hash<int32_t>()(v.val);
    }
    size_t operator()(const time_value& v) const {
        return std::hash<int64_t>()(v.val);
    }
    size_t operator()(const timestamp_value& v) const {
        return std::hash<int64_t>()(v.val);
    }
    size_t operator()(const timestamptz_value& v) const {
        return std::hash<int64_t>()(v.val);
    }
    size_t operator()(const string_value& v) const {
        return std::hash<iobuf>()(v.val);
    }
    size_t operator()(const uuid_value& v) const {
        return absl::Hash<uuid_t>()(v.val);
    }
    size_t operator()(const fixed_value& v) const {
        return std::hash<iobuf>()(v.val);
    }
    size_t operator()(const binary_value& v) const {
        return std::hash<iobuf>()(v.val);
    }
    size_t operator()(const decimal_value& v) const {
        return absl::Hash<absl::int128>()(v.val);
    }
};

struct hashing_visitor {
    size_t operator()(const primitive_value& v) const {
        return std::visit(primitive_hashing_visitor{}, v);
    }

    size_t operator()(const std::unique_ptr<struct_value>& v) const {
        if (!v) {
            return 0;
        }
        return value_hash(*v);
    }
    size_t operator()(const std::unique_ptr<list_value>& v) const {
        if (!v) {
            return 0;
        }
        size_t h = 0;
        for (const auto& e : v->elements) {
            if (!e) {
                continue;
            }
            boost::hash_combine(h, std::hash<value>()(*e));
        }
        return h;
    }
    size_t operator()(const std::unique_ptr<map_value>& v) const {
        if (!v) {
            return 0;
        }
        size_t h = 0;
        for (const auto& kv : v->kvs) {
            boost::hash_combine(h, std::hash<value>()(kv.key));
            if (kv.val) {
                boost::hash_combine(h, std::hash<value>()(*kv.val));
            }
        }
        return h;
    }
};

fmt::iterator format_val_ptr(const std::optional<value>& v, fmt::iterator it) {
    if (v) {
        return fmt::format_to(it, "{}", *v);
    }
    return fmt::format_to(it, "none");
}

struct primitive_value_comparison_visitor {
    template<typename T, typename U>
    bool operator()(const T&, const U&) const {
        static_assert(!std::is_same<T, U>::value);
        return false;
    }
    template<typename T>
    requires requires(T t) { t.val; }
    bool operator()(const T& lhs, const T& rhs) const {
        return lhs.val == rhs.val;
    }
};

struct primitive_value_lt_visitor {
    template<typename T, typename U>
    bool operator()(const T& t, const U& u) const {
        static_assert(!std::is_same_v<T, U>);
        throw std::invalid_argument(
          fmt::format("Cannot evaluate {} < {}", t, u));
    }
    template<typename T>
    requires requires(T t) { t.val; }
    bool operator()(const T& lhs, const T& rhs) const {
        return lhs.val < rhs.val;
    }
};

struct copying_visitor {
    value operator()(const primitive_value& v) const {
        return std::visit(primitive_copying_visitor{}, v);
    }

    value operator()(const std::unique_ptr<struct_value>& v) const {
        auto ret = std::make_unique<struct_value>();
        ret->fields.reserve(v->fields.size());
        for (const auto& f : v->fields) {
            if (!f) {
                ret->fields.push_back(std::nullopt);
                continue;
            }
            ret->fields.push_back(make_copy(*f));
        }
        return ret;
    }
    value operator()(const std::unique_ptr<list_value>& v) const {
        auto ret = std::make_unique<list_value>();
        ret->elements.reserve(v->elements.size());
        for (const auto& e : v->elements) {
            if (!e) {
                ret->elements.push_back(std::nullopt);
                continue;
            }
            ret->elements.push_back(make_copy(*e));
        }
        return ret;
    }
    value operator()(const std::unique_ptr<map_value>& v) const {
        auto ret = std::make_unique<map_value>();
        ret->kvs.reserve(v->kvs.size());
        for (const auto& kv : v->kvs) {
            iceberg::kv_value kv_copy;
            kv_copy.key = make_copy(kv.key);
            if (!kv.val) {
                kv_copy.val = std::nullopt;
            } else {
                kv_copy.val = make_copy(*kv.val);
            }
            ret->kvs.push_back(std::move(kv_copy));
        }
        return ret;
    }
};

} // namespace

primitive_value make_copy(const primitive_value& v) {
    return std::visit(primitive_copying_visitor{}, v);
}

value make_copy(const value& v) { return std::visit(copying_visitor{}, v); }

bool operator==(const primitive_value& lhs, const primitive_value& rhs) {
    return std::visit(primitive_value_comparison_visitor{}, lhs, rhs);
}

bool operator<(const primitive_value& lhs, const primitive_value& rhs) {
    return std::visit(primitive_value_lt_visitor{}, lhs, rhs);
}

bool operator==(const struct_value& lhs, const struct_value& rhs) {
    if (lhs.fields.size() != rhs.fields.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.fields.size(); i++) {
        auto has_lhs = lhs.fields[i] != std::nullopt;
        auto has_rhs = rhs.fields[i] != std::nullopt;
        if (has_lhs != has_rhs) {
            return false;
        }
        if (!has_lhs) {
            // Both are null.
            continue;
        }
        if (*lhs.fields[i] != *rhs.fields[i]) {
            return false;
        }
    }
    return true;
}
bool operator==(
  const std::unique_ptr<struct_value>& lhs,
  const std::unique_ptr<struct_value>& rhs) {
    if ((lhs == nullptr) != (rhs == nullptr)) {
        return false;
    }
    if (lhs == nullptr) {
        // Both null.
        return true;
    }
    return *lhs == *rhs;
}

bool operator==(const list_value& lhs, const list_value& rhs) {
    if (lhs.elements.size() != rhs.elements.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.elements.size(); i++) {
        auto has_lhs = lhs.elements[i] != std::nullopt;
        auto has_rhs = rhs.elements[i] != std::nullopt;
        if (has_lhs != has_rhs) {
            return false;
        }
        if (!has_lhs) {
            // Both are null.
            continue;
        }
        if (*lhs.elements[i] != *rhs.elements[i]) {
            return false;
        }
    }
    return true;
}
bool operator==(
  const std::unique_ptr<list_value>& lhs,
  const std::unique_ptr<list_value>& rhs) {
    if ((lhs == nullptr) != (rhs == nullptr)) {
        return false;
    }
    if (lhs == nullptr) {
        // Both null.
        return true;
    }
    return *lhs == *rhs;
}

bool operator==(const kv_value& lhs, const kv_value& rhs) {
    auto has_lhs_val = lhs.val != std::nullopt;
    auto has_rhs_val = rhs.val != std::nullopt;
    if (has_lhs_val != has_rhs_val) {
        return false;
    }
    if (lhs.key != rhs.key) {
        return false;
    }
    if (has_lhs_val && *lhs.val != *rhs.val) {
        return false;
    }
    return true;
}

bool operator==(const map_value& lhs, const map_value& rhs) {
    if (lhs.kvs.size() != rhs.kvs.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.kvs.size(); i++) {
        if (lhs.kvs[i] != rhs.kvs[i]) {
            return false;
        }
    }
    return true;
}
bool operator==(
  const std::unique_ptr<map_value>& lhs,
  const std::unique_ptr<map_value>& rhs) {
    if ((lhs == nullptr) != (rhs == nullptr)) {
        return false;
    }
    if (lhs == nullptr) {
        // Both null.
        return true;
    }
    return *lhs == *rhs;
}

struct comparison_visitor {
    explicit comparison_visitor(const value& lhs)
      : lhs_(lhs) {}
    const value& lhs_;

    bool operator()(const primitive_value& rhs) {
        return std::get<primitive_value>(lhs_) == rhs;
    }
    bool operator()(const std::unique_ptr<list_value>& rhs) {
        return std::get<std::unique_ptr<list_value>>(lhs_) == rhs;
    }
    bool operator()(const std::unique_ptr<struct_value>& rhs) {
        return std::get<std::unique_ptr<struct_value>>(lhs_) == rhs;
    }
    bool operator()(const std::unique_ptr<map_value>& rhs) {
        return std::get<std::unique_ptr<map_value>>(lhs_) == rhs;
    }
};

bool operator==(const value& lhs, const value& rhs) {
    if (lhs.index() != rhs.index()) {
        return false;
    }
    return std::visit(comparison_visitor{lhs}, rhs);
}

fmt::iterator string_value::format_to(fmt::iterator it) const {
    iobuf_const_parser buf_parser{val};
    static constexpr auto max_len = 16;
    auto size_bytes = val.size_bytes();
    if (size_bytes > max_len) {
        return fmt::format_to(
          it, "string(\"{}...\")", buf_parser.read_string(max_len));
    }
    return fmt::format_to(
      it, "string(\"{}\")", buf_parser.read_string(size_bytes));
}
fmt::iterator fixed_value::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "fixed(size_bytes={})", val.size_bytes());
}
fmt::iterator binary_value::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "binary(size_bytes={})", val.size_bytes());
}
namespace {
struct primitive_format_visitor {
    fmt::iterator it;

    template<typename T>
    fmt::iterator operator()(const T& v) {
        return fmt::format_to(it, "{}", v);
    }
};
} // namespace

fmt::iterator format_to(const primitive_value& v, fmt::iterator it) {
    return std::visit(primitive_format_visitor{it}, v);
}

fmt::iterator list_value::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "list{{");
    static constexpr size_t max_to_log = 5;
    size_t logged = 0;
    for (const auto& e : elements) {
        if (logged == max_to_log) {
            it = fmt::format_to(it, "...");
            break;
        }
        it = format_val_ptr(e, it);
        it = fmt::format_to(it, ", ");
        logged++;
    }
    return fmt::format_to(it, "}}");
}
fmt::iterator map_value::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "map{{");
    static constexpr size_t max_to_log = 5;
    size_t logged = 0;
    for (const auto& kv : kvs) {
        if (logged == max_to_log) {
            it = fmt::format_to(it, "...");
            break;
        }
        it = fmt::format_to(it, "(k={}, v=", kv.key);
        it = format_val_ptr(kv.val, it);
        it = fmt::format_to(it, "), ");
        logged++;
    }
    return fmt::format_to(it, "}}");
}
fmt::iterator struct_value::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "struct{{");
    static constexpr size_t max_to_log = 5;
    size_t logged = 0;
    for (const auto& f : fields) {
        if (logged == max_to_log) {
            it = fmt::format_to(it, "...");
            break;
        }
        it = format_val_ptr(f, it);
        it = fmt::format_to(it, ", ");
        logged++;
    }
    return fmt::format_to(it, "}}");
}

namespace {
struct value_format_visitor {
    fmt::iterator it;

    fmt::iterator operator()(const primitive_value& v) {
        return format_to(v, it);
    }

    template<typename T>
    fmt::iterator operator()(const T& v) {
        return fmt::format_to(it, "{}", v);
    }
};
} // namespace

fmt::iterator format_to(const value& v, fmt::iterator it) {
    return std::visit(value_format_visitor{it}, v);
}

size_t value_hash(const struct_value& v) {
    size_t h = 0;
    for (const auto& f : v.fields) {
        if (!f) {
            continue;
        }
        boost::hash_combine(h, std::hash<value>()(*f));
    }
    return h;
}

size_t value_hash(const value& v) { return std::visit(hashing_visitor{}, v); }

} // namespace iceberg
