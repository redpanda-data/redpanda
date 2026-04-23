/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "config/replicas_preference.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/named_type.h"
#include "serde/rw/sstring.h"
#include "serde/rw/vector.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <sstream>
#include <stdexcept>

namespace {

using rp = config::replicas_preference;
using rp_t = config::replicas_preference::type_t;

TEST(ReplicasPreference, ParseNone) {
    for (std::string_view input : {"none", "", "   ", "\t  \n"}) {
        SCOPED_TRACE(fmt::format("input: '{}'", input));
        auto p = rp::parse(input);
        EXPECT_EQ(p.type, rp_t::none);
        EXPECT_TRUE(p.rack_ids.empty());
        EXPECT_EQ(p.num_groups(), 0);
        EXPECT_FALSE(p.group_index_for(model::rack_id{"A"}).has_value());
    }
}

TEST(ReplicasPreference, ParseSingleRack) {
    auto p = rp::parse("racks: A");
    EXPECT_EQ(p.type, rp_t::racks);
    EXPECT_EQ(p.num_groups(), 1);
    EXPECT_EQ(p.group_index_for(model::rack_id{"A"}).value(), 0U);
    EXPECT_FALSE(p.group_index_for(model::rack_id{"Z"}).has_value());
}

TEST(ReplicasPreference, ParseMultipleRacksEachItsOwnGroup) {
    auto p = rp::parse("racks: A, B, C");
    EXPECT_EQ(p.num_groups(), 3);
    EXPECT_EQ(p.group_index_for(model::rack_id{"A"}).value(), 0U);
    EXPECT_EQ(p.group_index_for(model::rack_id{"B"}).value(), 1U);
    EXPECT_EQ(p.group_index_for(model::rack_id{"C"}).value(), 2U);
    EXPECT_FALSE(p.group_index_for(model::rack_id{"D"}).has_value());
}

TEST(ReplicasPreference, ParseGroups) {
    // Mixed single rack + multi-rack group + single rack.
    {
        auto p = rp::parse("racks: A, {B, C}, D");
        EXPECT_EQ(p.num_groups(), 3);
        EXPECT_EQ(p.group_index_for(model::rack_id{"A"}).value(), 0U);
        EXPECT_EQ(p.group_index_for(model::rack_id{"B"}).value(), 1U);
        EXPECT_EQ(p.group_index_for(model::rack_id{"C"}).value(), 1U);
        EXPECT_EQ(p.group_index_for(model::rack_id{"D"}).value(), 2U);
    }
    // Multiple multi-rack groups.
    {
        auto p = rp::parse("racks: {A, B}, {C, D}, E");
        EXPECT_EQ(p.num_groups(), 3);
        EXPECT_EQ(p.group_index_for(model::rack_id{"A"}).value(), 0U);
        EXPECT_EQ(p.group_index_for(model::rack_id{"B"}).value(), 0U);
        EXPECT_EQ(p.group_index_for(model::rack_id{"C"}).value(), 1U);
        EXPECT_EQ(p.group_index_for(model::rack_id{"D"}).value(), 1U);
        EXPECT_EQ(p.group_index_for(model::rack_id{"E"}).value(), 2U);
    }
    // Explicit single-element group (equivalent to a bare rack).
    {
        auto p = rp::parse("racks: {A}, B");
        EXPECT_EQ(p.num_groups(), 2);
        EXPECT_EQ(p.group_index_for(model::rack_id{"A"}).value(), 0U);
        EXPECT_EQ(p.group_index_for(model::rack_id{"B"}).value(), 1U);
    }
}

TEST(ReplicasPreference, ParseToleratesWhitespace) {
    // Extra internal whitespace and surrounding whitespace.
    auto p1 = rp::parse("  racks:  A ,  { B , C } ,  D  ");
    // No whitespace at all.
    auto p2 = rp::parse("racks:A,{B,C},D");
    // Canonical spacing.
    auto p3 = rp::parse("racks: A, {B, C}, D");
    EXPECT_EQ(p1, p2);
    EXPECT_EQ(p1, p3);
}

TEST(ReplicasPreference, ParseErrors) {
    for (std::string_view bad : {
           "invalid",          // no prefix
           "racks: {}",        // empty braces
           "racks: {{A}}",     // nested braces
           "racks: {A, B",     // unclosed brace
           "racks: A}",        // unexpected close
           "racks: A, A",      // duplicate
           "racks: A, {B, A}", // duplicate across groups
           "racks: A,,B",      // empty token
           "racks: ",          // no racks
           "racks: A,",        // trailing comma
         }) {
        SCOPED_TRACE(fmt::format("input: '{}'", bad));
        EXPECT_THROW(rp::parse(bad), std::runtime_error);
    }
}

TEST(ReplicasPreference, FormatRoundtrip) {
    // parse(input) == parse(format(parse(input))), and format is stable.
    for (std::string_view input : {
           "none",
           "racks: A",
           "racks: A, B, C",
           "racks: A, {B, C}, D",
           "racks: {A, B}, {C, D}, E",
           "racks: {A}, B",
         }) {
        SCOPED_TRACE(fmt::format("input: '{}'", input));
        auto orig = rp::parse(input);
        auto str = fmt::format("{}", orig);
        auto reparsed = rp::parse(str);
        EXPECT_EQ(orig, reparsed);
        EXPECT_EQ(str, fmt::format("{}", reparsed));
    }
}

TEST(ReplicasPreference, FormatExact) {
    EXPECT_EQ(fmt::format("{}", rp{}), "none");
    EXPECT_EQ(fmt::format("{}", rp::parse("racks: A")), "racks: A");
    EXPECT_EQ(
      fmt::format("{}", rp::parse("racks: A, {B, C}, D")),
      "racks: A, {B, C}, D");
    EXPECT_EQ(
      fmt::format("{}", rp::parse("racks: {A, B}, {C, D}, E")),
      "racks: {A, B}, {C, D}, E");
}

TEST(ReplicasPreference, SerdeRoundtrip) {
    for (std::string_view input : {
           "none",
           "racks: A",
           "racks: A, B, C",
           "racks: A, {B, C}, D",
           "racks: {A, B}, {C, D}, E",
         }) {
        SCOPED_TRACE(fmt::format("input: '{}'", input));
        auto orig = rp::parse(input);

        iobuf buf;
        serde::write(buf, orig);

        auto parser = iobuf_parser(std::move(buf));
        auto decoded = serde::read<rp>(parser);

        EXPECT_EQ(orig, decoded);
    }
}

TEST(ReplicasPreference, Inequality) {
    auto a = rp::parse("racks: A, B, C");
    EXPECT_NE(a, rp::parse("racks: A, B"));      // fewer racks
    EXPECT_NE(a, rp::parse("racks: A, {B, C}")); // different grouping
    EXPECT_NE(a, rp{});                          // vs. none
}

// Intra-group rack order must not affect equality or serialization:
// "racks: A, {C, B}" is equivalent to "racks: A, {B, C}", and the
// canonical form is the lexicographically sorted one.
TEST(ReplicasPreference, CanonicalOrderingEquality) {
    EXPECT_EQ(rp::parse("racks: A, {B, C}"), rp::parse("racks: A, {C, B}"));
    EXPECT_EQ(
      rp::parse("racks: {A, B}, {C, D}"), rp::parse("racks: {B, A}, {D, C}"));
}

TEST(ReplicasPreference, CanonicalSerialization) {
    // Both orderings serialize to the sorted canonical form.
    EXPECT_EQ(
      fmt::format("{}", rp::parse("racks: A, {C, B}")), "racks: A, {B, C}");
    EXPECT_EQ(
      fmt::format("{}", rp::parse("racks: {B, A}, {D, C}")),
      "racks: {A, B}, {C, D}");
}

TEST(ReplicasPreference, IstreamOperator) {
    auto orig = rp::parse("racks: A, {B, C}, D");
    std::istringstream is(fmt::format("{}", orig));

    rp parsed;
    is >> parsed;
    EXPECT_EQ(orig, parsed);
}

} // namespace
