/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/compatibility_types.h"
#include "iceberg/datatypes.h"
#include "iceberg/simulate_evolution.h"

#include <gtest/gtest.h>

using namespace iceberg;

namespace {

nested_field_ptr
make_field(ss::sstring name, field_required req, field_type type) {
    return nested_field::create(0, std::move(name), req, std::move(type));
}

nested_field_ptr make_optional(ss::sstring name, field_type type) {
    return make_field(std::move(name), field_required::no, std::move(type));
}

nested_field_ptr make_required(ss::sstring name, field_type type) {
    return make_field(std::move(name), field_required::yes, std::move(type));
}

template<typename... Fields>
struct_type make_struct(Fields&&... fields) {
    struct_type s;
    (s.fields.push_back(std::forward<Fields>(fields)), ...);
    return s;
}

} // namespace

TEST(SimulateEvolutionTest, SingleVersion) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", string_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_value()) << "expected success for single version";
    EXPECT_EQ(res.value().fields.size(), 2);
}

TEST(SimulateEvolutionTest, AddOptionalField) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(make_optional("a", int_type{})));
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", string_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_value()) << "expected success adding optional field";
    EXPECT_EQ(res.value().fields.size(), 2);
}

TEST(SimulateEvolutionTest, TypePromotionIntToLong) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(make_optional("a", int_type{})));
    seq.push_back(make_struct(make_optional("a", long_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_value()) << "expected success for int->long promotion";

    ASSERT_EQ(res.value().fields.size(), 1);
    auto& ft = res.value().fields[0]->type;
    ASSERT_TRUE(std::holds_alternative<primitive_type>(ft));
    auto& pt = std::get<primitive_type>(ft);
    EXPECT_TRUE(std::holds_alternative<long_type>(pt));
}

TEST(SimulateEvolutionTest, TypePromotionFloatToDouble) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(make_optional("a", float_type{})));
    seq.push_back(make_struct(make_optional("a", double_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_value())
      << "expected success for float->double promotion";

    ASSERT_EQ(res.value().fields.size(), 1);
    auto& ft = res.value().fields[0]->type;
    ASSERT_TRUE(std::holds_alternative<primitive_type>(ft));
    auto& pt = std::get<primitive_type>(ft);
    EXPECT_TRUE(std::holds_alternative<double_type>(pt));
}

TEST(SimulateEvolutionTest, DropField) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", string_type{})));
    seq.push_back(make_struct(make_optional("a", int_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_value()) << "expected success when dropping a field";
    // Iceberg never truly drops fields from the accumulated schema.
    EXPECT_EQ(res.value().fields.size(), 2);
}

TEST(SimulateEvolutionTest, SubsetWriterNoChange) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", string_type{})));
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", string_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_value()) << "expected success for identical schemas";
    EXPECT_EQ(res.value().fields.size(), 2);
}

TEST(SimulateEvolutionTest, DropAndReintroduceSameType) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", string_type{})));
    seq.push_back(make_struct(make_optional("a", int_type{})));
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", string_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_value())
      << "expected success reintroducing dropped field with same type";
}

TEST(SimulateEvolutionTest, DropAndReintroduceDifferentType) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", string_type{})));
    seq.push_back(make_struct(make_optional("a", int_type{})));
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", int_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_error())
      << "expected failure reintroducing field with different type";
    EXPECT_EQ(res.error().errc, schema_evolution_errc::incompatible);
    EXPECT_EQ(res.error().step, 2);
}

TEST(SimulateEvolutionTest, IncompatibleTypeChange) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(make_optional("a", string_type{})));
    seq.push_back(make_struct(make_optional("a", int_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_error())
      << "expected failure for incompatible type change";
    EXPECT_EQ(res.error().errc, schema_evolution_errc::incompatible);
    EXPECT_EQ(res.error().step, 1);
}

TEST(SimulateEvolutionTest, WriterWithNarrowerTypeAccepted) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(make_optional("a", int_type{})));
    seq.push_back(make_struct(make_optional("a", long_type{}))); // promote
    seq.push_back(make_struct(
      make_optional("a", int_type{}))); // writer uses int, table stays long

    auto res = simulate_evolution(std::move(seq));
    // This should PASS: the table stays at long, and int writes are fine.
    // try_fill_field_ids handles int->long promotion for writing.
    ASSERT_TRUE(res.has_value()) << "int writing to long column should succeed";
}

TEST(SimulateEvolutionTest, NestedStructAddField) {
    auto inner_v1 = make_struct(make_optional("x", int_type{}));
    auto inner_v2 = make_struct(
      make_optional("x", int_type{}), make_optional("y", string_type{}));

    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(make_optional("inner", std::move(inner_v1))));
    seq.push_back(make_struct(make_optional("inner", std::move(inner_v2))));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_value())
      << "expected success adding field to nested struct";
}

TEST(SimulateEvolutionTest, ListElementTypePromotion) {
    auto list_v1 = list_type::create(0, field_required::yes, int_type{});
    auto list_v2 = list_type::create(0, field_required::yes, long_type{});

    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(make_optional("items", std::move(list_v1))));
    seq.push_back(make_struct(make_optional("items", std::move(list_v2))));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_value())
      << "expected success for list element type promotion";
}

TEST(SimulateEvolutionTest, MapKeyChangeRejected) {
    auto map_v1 = map_type::create(
      0, string_type{}, 0, field_required::no, int_type{});
    auto map_v2 = map_type::create(
      0, int_type{}, 0, field_required::no, int_type{});

    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(make_optional("kv", std::move(map_v1))));
    seq.push_back(make_struct(make_optional("kv", std::move(map_v2))));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_error()) << "expected failure for map key type change";
}

TEST(SimulateEvolutionTest, FieldIdsMonotonicallyIncreasing) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", string_type{})));
    seq.push_back(make_struct(
      make_optional("a", int_type{}),
      make_optional("b", string_type{}),
      make_optional("c", long_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_value()) << "expected success for additive evolution";

    std::set<int32_t> ids;
    for (auto& f : res.value().fields) {
        EXPECT_NE(f->id, nested_field::id_t{0})
          << "field " << f->name << " has placeholder ID 0";
        ids.insert(f->id());
    }
    EXPECT_EQ(ids.size(), res.value().fields.size())
      << "field IDs must be unique";
}

TEST(SimulateEvolutionTest, FiveVersionEvolution) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(make_optional("a", int_type{})));
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", string_type{})));
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", string_type{})));
    seq.push_back(make_struct(
      make_optional("a", int_type{}),
      make_optional("b", string_type{}),
      make_optional("c", long_type{})));
    seq.push_back(make_struct(
      make_optional("a", int_type{}),
      make_optional("b", string_type{}),
      make_optional("c", long_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_value()) << "expected success for 5-step evolution";
    EXPECT_EQ(res.value().fields.size(), 3);
}

TEST(SimulateEvolutionTest, ErrorAtMiddleStep) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(make_optional("a", int_type{})));
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", string_type{})));
    // Incompatible: change b from string to int
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_optional("b", int_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_error()) << "expected failure at middle step";
    EXPECT_EQ(res.error().step, 2);
}

TEST(SimulateEvolutionTest, EmptySequenceReturnsError) {
    chunked_vector<struct_type> seq;
    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_error()) << "expected failure for empty sequence";
}

TEST(SimulateEvolutionTest, EmptyStructs) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct());
    seq.push_back(make_struct());

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_value()) << "expected success for two empty structs";
}

TEST(SimulateEvolutionTest, NewRequiredFieldRejected) {
    chunked_vector<struct_type> seq;
    seq.push_back(make_struct(make_optional("a", int_type{})));
    seq.push_back(make_struct(
      make_optional("a", int_type{}), make_required("b", string_type{})));

    auto res = simulate_evolution(std::move(seq));
    ASSERT_TRUE(res.has_error()) << "expected failure for new required field";
    EXPECT_EQ(res.error().errc, schema_evolution_errc::new_required_field);
    EXPECT_EQ(res.error().step, 1);
}
