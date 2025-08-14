/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/partition_spec_parser.h"

#include "base/vassert.h"
#include "container/chunked_vector.h"
#include "iceberg/transform.h"
#include "iceberg/unresolved_partition_spec.h"
#include "utils/fixed_string.h"
#include "utils/variant.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <fmt/format.h>

#include <charconv>
#include <cstddef>
#include <optional>
#include <string_view>
#include <unordered_set>

namespace datalake {

namespace {

using source = std::string_view;
using formatted_error = ss::sstring;

class parser_base {
protected:
    explicit parser_base(source original)
      : _original(original) {}

    using position = source::const_iterator;
    using expectation = std::string_view;
    using expectations = std::unordered_set<expectation>;

    template<typename T>
    struct parse_success {
        T val;
        position next;
    };

    template<typename T>
    class parse_result {
        std::optional<parse_success<T>> res;

        parse_result() = default;

    public:
        // success
        parse_result(T&& val, position next)
          : res(std::in_place, std::forward<T>(val), next) {}

        // success
        parse_result(
          T&& val,
          position next,
          parser_base& self,
          expectation exp,
          position err_pos)
          : res(std::in_place, std::forward<T>(val), next) {
            self.log_failure(exp, err_pos);
        }

        // failure
        parse_result(parser_base& self, expectation exp, position err_pos) {
            self.log_failure(exp, err_pos);
        }

        // failure
        template<typename U>
        parse_result(parse_result<U>&& other_failure) {
            vassert(
              !other_failure,
              "failure should either be explicit or based on another "
              "failure");
        }

        // failure
        static parse_result<T> dummy_failure() { return {}; }

        T value() && {
            vassert(res, "attempted to treat error as success");
            return std::move(*res).val;
        }

        T& mutable_value() & {
            vassert(res, "attempted to treat error as success");
            return res->val;
        }

        position after() const {
            vassert(res, "attempted to treat error as success");
            return res->next;
        }

        operator bool() const { return res.has_value(); }

        /* rule of 5: movable not copiable */
        parse_result(const parse_result&) = delete;
        parse_result& operator=(const parse_result&) = delete;
        parse_result(parse_result<T>&& other) = default;
        parse_result& operator=(parse_result&&) = default;
        ~parse_result() = default;
    };

    void log_failure(expectation exp, position err_pos) {
        if (err_pos > _farthest_failure) {
            _farthest_failure = err_pos;
            _failed_expectations = {std::move(exp)};
        } else if (err_pos == _farthest_failure) {
            _failed_expectations.insert(std::move(exp));
        }
    }

    formatted_error format_error() {
        return fmt::format(
          "col {}: expected {} (got instead: \"{}\")",
          _farthest_failure - _original.cbegin(),
          fmt::join(_failed_expectations, " or "),
          source{_farthest_failure, _original.cend()});
    }

    position end() { return _original.end(); }

    const char* to_char_ptr(position pos) {
        return _original.data() + (pos - _original.begin());
    }

    const char* to_position(const char* ptr) {
        return _original.begin() + (ptr - _original.data());
    }

private:
    source _original;
    position _farthest_failure = _original.begin();
    expectations _failed_expectations;
};

class partition_spec_parser : parser_base {
    using self = partition_spec_parser;

public:
    static checked<iceberg::unresolved_partition_spec, formatted_error>
    parse(source unparsed) {
        self parser{unparsed};
        auto pr = parser.parse_partition_spec(unparsed.begin());
        if (!pr) {
            return parser.format_error();
        }
        return std::move(pr).value();
    }

private:
    explicit partition_spec_parser(source original)
      : parser_base(original) {}

    bool is_ws(position pos) {
        return *pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r';
    }

    void skip_ws(position& pos) {
        while (pos != end() && is_ws(pos)) {
            ++pos;
        }
    }

    struct upper {
        [[nodiscard]] constexpr char operator()(char c) const noexcept {
            return std::toupper(c);
        };
    };

    parse_result<std::monostate> expect_end(position pos) {
        if (pos == end()) {
            return {{}, pos};
        }
        return {*this, "end of input", pos};
    }

    template<fixed_string expected, bool case_sensitive = true>
    parse_result<std::monostate> expect(position pos) {
        source unparsed{pos, end()};
        constexpr std::conditional_t<case_sensitive, std::identity, upper> proj;
        if (!std::ranges::starts_with(
              unparsed, std::string_view{expected}, {}, proj, proj)) {
            static constexpr auto err = fixed_string{"\""} + expected
                                        + fixed_string{"\""};
            return {*this, err, pos};
        }
        pos += std::string_view{expected}.size();
        return {std::monostate{}, pos};
    }

    template<fixed_string expected, bool case_sensitive = true>
    parse_result<std::monostate> expect_with_ws(position pos) {
        skip_ws(pos);

        auto res = expect<expected, case_sensitive>(pos);
        if (!res) {
            return res;
        }
        pos = res.after();
        skip_ws(pos);

        return {std::monostate{}, pos};
    }

    parse_result<ss::sstring> parse_simple_identifier(position pos) {
        // first symbol cannot be a digit
        constexpr static auto expected1 = "letter or \"_\"";
        if (pos == end() || (*pos != '_' && !std::isalpha(*pos))) {
            return {*this, expected1, pos};
        }
        // remaining symbols
        position cur = pos + 1;
        constexpr static auto expected = "alphanumeric or \"_\"";
        while (cur != end() && (*cur == '_' || std::isalnum(*cur))) {
            ++cur;
        }
        ss::sstring id{pos, cur};
        for (char& c : id) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        return {std::move(id), cur, *this, expected, cur};
    }

    template<char quotation_mark>
    parse_result<ss::sstring> parse_quoted_w_doubling(position pos) {
        constexpr char quotation_mark_as_array[2]{quotation_mark, '\0'};
        skip_ws(pos);
        auto o_q_res = expect<quotation_mark_as_array>(pos);
        if (!o_q_res) {
            return o_q_res;
        }

        pos = o_q_res.after();
        position cur = pos;
        size_t cnt_dbl_backticks = 0;
        while (cur != end()) {
            if (*cur == quotation_mark) {
                ++cur;
                auto dbl_q_res = expect<quotation_mark_as_array>(cur);
                if (!dbl_q_res) {
                    // it was a single quote: finalize!
                    position reread_cur = pos;
                    auto reread_end = cur - 1; // excluding the closing tick

                    size_t dst_len = reread_end - reread_cur
                                     - cnt_dbl_backticks;
                    ss::sstring res{ss::sstring::initialized_later{}, dst_len};
                    auto write_cur = res.begin();

                    while (reread_cur != reread_end) {
                        *write_cur = *reread_cur;
                        if (*reread_cur == quotation_mark) {
                            ++reread_cur; // double to single
                        }
                        ++reread_cur;
                        ++write_cur;
                    }
                    vassert(
                      write_cur == res.end(), "pointer arithmetics error");
                    skip_ws(cur);
                    return {std::move(res), cur};
                }
                ++cnt_dbl_backticks;
            }
            ++cur;
        }
        // reached end of input inside quotes
        return expect_with_ws<quotation_mark_as_array>(cur);
    }

    parse_result<ss::sstring> parse_identifier(position pos) {
        auto res = parse_simple_identifier(pos);
        if (!res) {
            res = parse_quoted_w_doubling<'`'>(pos);
        }
        return res;
    }

    template<
      typename Elem,
      template<typename...> class Container,
      fixed_string Delim>
    parse_result<Container<Elem>> parse_separated_list(
      parse_result<Elem> (self::*elem_parser)(position), position pos) {
        Container<Elem> result;
        while (true) {
            if (!result.empty()) {
                auto delim_res = expect_with_ws<Delim>(pos);
                if (!delim_res) {
                    return {std::move(result), pos};
                }
                pos = delim_res.after();
            }

            auto elem_res = (this->*elem_parser)(pos);
            if (!elem_res) {
                return elem_res;
            }
            pos = elem_res.after();
            result.push_back(std::move(elem_res).value());
        }
    }

    parse_result<iceberg::unresolved_partition_spec::column_reference>
    parse_column_reference(position pos) {
        return parse_separated_list<ss::sstring, std::vector, ".">(
          &self::parse_identifier, pos);
    }

    parse_result<iceberg::unresolved_partition_spec::field>
    parse_column_reference_expr(position pos) {
        auto res = parse_column_reference(pos);
        if (!res) {
            return res;
        }
        pos = res.after();
        return {{.source_name = std::move(res).value()}, pos};
    };

    using transform_ident = variant_of_identities<iceberg::transform>;

    parse_result<transform_ident> parse_transform_name(position pos) {
        auto tagged_res = parse_result<transform_ident>::dummy_failure();

        std::apply(
          [this, &tagged_res, pos](auto... transform_id) {
              (
                [this, &tagged_res, pos, transform_id] {
                    if (tagged_res) {
                        return;
                    }

                    using transform_type = decltype(transform_id)::type;

                    auto literal_res
                      = expect_with_ws<transform_type::key, false>(pos);
                    if (literal_res) {
                        // success, skip further attempts
                        tagged_res = {transform_id, literal_res.after()};
                    }
                }(),
                ...);
          },
          tuple_of_identities<iceberg::transform>{});

        return tagged_res;
    }

    parse_result<uint32_t> parse_uint32(position pos) {
        uint32_t num;
        auto res = std::from_chars(to_char_ptr(pos), to_char_ptr(end()), num);
        if (res.ec == std::errc{}) {
            return {std::move(num), to_position(res.ptr)};
        }
        return {*this, "integer between 0 and 4294967295", pos};
    }

    parse_result<
      std::pair<uint32_t, iceberg::unresolved_partition_spec::column_reference>>
    parse_transform_arguments(position pos) {
        auto num_res = parse_uint32(pos);
        if (!num_res) {
            return num_res;
        }
        auto comma_res = expect_with_ws<",">(num_res.after());
        if (!comma_res) {
            return comma_res;
        }
        auto col_ref_res = parse_column_reference(comma_res.after());
        if (!col_ref_res) {
            return col_ref_res;
        }
        pos = col_ref_res.after();
        return {
          {std::move(num_res).value(), std::move(col_ref_res).value()}, pos};
    }

    template<typename T>
    parse_result<T> parse_in_brackets(
      parse_result<T> (self::*inner_parser)(position pos), position pos) {
        auto o_br_res = expect_with_ws<"(">(pos);
        if (!o_br_res) {
            return o_br_res;
        }
        auto main_res = (this->*inner_parser)(o_br_res.after());
        if (!main_res) {
            return main_res;
        }
        auto cl_br_res = expect_with_ws<")">(main_res.after());
        if (!cl_br_res) {
            return cl_br_res;
        }
        return {std::move(main_res).value(), cl_br_res.after()};
    }

    template<typename T>
    constexpr static auto transform_idx = iceberg::transform{T{}}.index();

    parse_result<iceberg::unresolved_partition_spec::field>
    parse_transform_expr(position pos) {
        auto transform_name_res = parse_transform_name(pos);
        if (!transform_name_res) {
            return transform_name_res;
        }
        pos = transform_name_res.after();
        auto transform_ident_val = std::move(transform_name_res).value();

        auto transform = std::visit(
          [](auto identity) {
              return iceberg::transform{
                std::in_place_type_t<typename decltype(identity)::type>{}};
          },
          transform_ident_val);

        switch (transform_ident_val.index()) {
        case transform_idx<iceberg::identity_transform>:
        case transform_idx<iceberg::year_transform>:
        case transform_idx<iceberg::month_transform>:
        case transform_idx<iceberg::day_transform>:
        case transform_idx<iceberg::hour_transform>:
        case transform_idx<iceberg::void_transform>: {
            auto cr_res = parse_in_brackets(&self::parse_column_reference, pos);
            if (!cr_res) {
                return cr_res;
            }
            pos = cr_res.after();
            return {
              {.source_name = std::move(cr_res).value(),
               .transform = std::move(transform)},
              pos};
        }
        case transform_idx<iceberg::bucket_transform>:
        case transform_idx<iceberg::truncate_transform>: {
            auto args_res = parse_in_brackets(
              &self::parse_transform_arguments, pos);
            if (!args_res) {
                return args_res;
            }
            pos = args_res.after();
            auto args_res_value = std::move(args_res).value();
            uint32_t param1 = args_res_value.first;

            ss::visit(
              transform,
              [&param1](iceberg::bucket_transform& parametrized_transform) {
                  parametrized_transform.n = param1;
              },
              [&param1](iceberg::truncate_transform& parametrized_transform) {
                  parametrized_transform.length = param1;
              },
              [](auto) { vassert(false, "unexpected transform"); });
            return {
              {.source_name = args_res_value.second,
               .transform = std::move(transform)},
              pos};
        }
        default:
            vassert(false, "parse_transform_name returned unknown option");
        }
    }

    parse_result<ss::sstring> parse_alias(position pos) {
        auto as_res = expect_with_ws<"AS", false>(pos);
        if (!as_res) {
            return as_res;
        }
        return parse_identifier(as_res.after());
    }

    parse_result<iceberg::unresolved_partition_spec::field>
    parse_partition_field(position pos) {
        auto res = parse_transform_expr(pos);
        if (!res) {
            res = parse_column_reference_expr(pos);
            if (!res) {
                return res;
            }
        }
        auto alias_res = parse_alias(res.after());
        if (alias_res) {
            pos = alias_res.after();
            res.mutable_value().name = std::move(alias_res).value();
            return {std::move(res).value(), pos};
        } else {
            res.mutable_value().autogenerate_name();
            return res;
        }
    }

    parse_result<iceberg::unresolved_partition_spec>
    parse_partition_spec_wo_brackets(position pos) {
        auto fields_res = parse_separated_list<
          iceberg::unresolved_partition_spec::field,
          chunked_vector,
          ",">(&self::parse_partition_field, pos);
        if (!fields_res) {
            // empty list is allowed
            return {{}, pos};
        }
        pos = fields_res.after();
        return {{std::move(fields_res).value()}, pos};
    }

    parse_result<iceberg::unresolved_partition_spec>
    parse_partition_spec(position pos) {
        skip_ws(pos);

        auto res = parse_in_brackets(
          &self::parse_partition_spec_wo_brackets, pos);
        if (!res) {
            res = parse_partition_spec_wo_brackets(pos);
        }
        if (!res) {
            return res;
        }

        pos = res.after();
        skip_ws(pos);

        auto end_res = expect_end(pos);
        if (!end_res) {
            return end_res;
        }

        return {std::move(res).value(), end_res.after()};
    }
};

} // namespace

checked<iceberg::unresolved_partition_spec, ss::sstring>
parse_partition_spec(const std::string_view& str) {
    return partition_spec_parser::parse(str);
}
} // namespace datalake
