/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "iceberg/compatibility_types.h"
#include "iceberg/datatypes.h"
#include "iceberg/field_name_comparison.h"
#include "iceberg/partition.h"
#include "iceberg/values.h"

namespace iceberg {

/**
   check_types - Performs a basic type check between two Iceberg field types,
   enforcing the Primitive Type Promotion policy laid out in
   https://iceberg.apache.org/spec/#schema-evolution

   - For non-primitive types, checks strict equality - i.e. struct == struct,
     list != map
   - Unwraps and compares input types, returns the result. Does not account for
     nesting.

   @param src  - The type of some field in an existing schema
   @param dest - The type of some field in a new schema, possibly compatible
                 with src.
   @return The result of the type check:
             - type_promoted::yes - if src -> dest is a valid type promotion
               e.g. int -> long
             - type_promoted::unless_partition - if src -> dest is a valid type
               promotion but would change the value of a partition transform
               involving src
             - type_promoted::no  - if src == dest
             - compat_errc::mismatch - if src != dest but src -> dest is not
               permitted e.g. int -> string or struct -> list
 */
type_check_result
check_types(const iceberg::field_type& src, const iceberg::field_type& dest);

// Promote primitive value variant c++ type to the c++ variant type determined
// by `dest`. Trivial promotion to the same type is allowed (e.g. int_value ->
// int_value if `dest` is int_type), as well as promotions allowed by the v2
// spec Primitive Type Promotion policy (int -> long, float -> double, decimal
// -> decimal). Note: underlying c++ type for decimal values with any precision
// is the same, so the promotion is trivial in this case as well.
primitive_value
promote_primitive_value_type(primitive_value value, const primitive_type& dest);

/**
 * annotate_schema_transform - Answers whether a one schema can "evolve" into
 * another, structurally.
 *
 * Traverse the schemas in lockstep, in a recursive depth-first fashion, marking
 * nested fields in one of the following ways:
 *   - for a field from the source schema:
 *     - the field does not exist in the dest schema (i.e. drop column)
 *     - a backwards compatible field exists in the dest schema (update column)
 *   - for a field from the dest schema:
 *     - the field receives its ID from a compatible field in the source schema
 *     - the field does not appear in the source schema (i.e. add column)
 *
 * Considerations:
 *   - This function modifies nested_field metadata, but ID updates are deferred
 *     to validate_schema_transform (see below). This is mainly to allow
 *     constness throughout the traversal, ensuring that we do not make
 *     inadvertent structural changes to either input struct.
 *
 * @param source - the schema we want to evolve _from_ (probably the current
 *                 schema for some table)
 * @param dest   - the proposed schema (probably extracted from some incoming
 *                 record)
 * @param spec  - a partition spec, resolved to the source schema. for
 *                 evaluating allowability of field removals.
 *
 * @return schema_transform_state (indicating success), or an error code
 */
schema_transform_result annotate_schema_transform(
  const struct_type& source,
  const struct_type& dest,
  const partition_spec& spec,
  field_name_comparison norm);

/**
 * validate_schema_transform - Finish evaluating backwards compatibility of
 * some schema with another. Makes any required changes to 'dest' and enforces
 * rules around field nullability ("requiredness"").
 *
 * Visit each field in the destination struct again,
 * this time checking nullability invariants and assigning final column IDs to
 * fields mapped from the source structs.
 *
 * Preconditions:
 *   - 'dest' has been fed though 'annotate_schema_transform' already, along
 *     with a source schema.
 *
 * @param annotate_res - the result of the annotation we're validating
 * @param dest - the proposed schema (probably extracted from some incoming
 *               record)
 * @param spec - a partition specification against which the fields
 *               of dest are compared to enforce that type promotions don't
 *               interfere with existing partition fields.
 *
 * @return schema_transform_state (indicating success), or an error code
 */
schema_transform_result validate_schema_transform(
  const schema_transform_result& annotate_res,
  struct_type& dest,
  const partition_spec& spec);

/**
 * evolve_schema - Prepares dest for insertion into table metadata.
 *
 * Annotate dest with source metadata, evaluate the annotations, and
 * return whether the table schema needs an update.
 *
 * Preconditions:
 *   - Both input structs are un-annotated. That is, none of their
 *     fields have the optional ::meta filled in.
 *
 * Postconditions:
 *   - All fields in dest are either assigned a unique ID carried over
 *     from source, or are marked as "new" (i.e. needing a unique ID
 *     assigned before insertion into metadata)
 *   - Each field in source is annotated with metadata indicating whether
 *     it has a compatible counterpart in dest.
 *
 * @param source - The source (i.e original) schema
 * @param dest   - the proposed schema (probably extracted from some incoming
 *                 record)
 * @param spec   - The current partition spec for the target table.
 *                 Used to enforce that type promotions don't interfere with
 *                 partition fields.
 *
 * @return Whether the schema changed from source->dest, or an error
 */
schema_evolution_result evolve_schema(
  const struct_type& source,
  struct_type& dest,
  const partition_spec& spec,
  field_name_comparison norm);

/**
 * Fill all writer struct field IDs from host struct by name while following
 * Iceberg schema evolution rules for types:
 * - writer_struct_type fields must exist in the host struct (by name)
 * - writer_struct_type can be a subset of host_struct_type (not all
 * host_struct_type fields required to be present)
 * - Field types must be identical or promotable (e.g., int -> bigint)
 * - Required fields in host_struct_type cannot be optional (i.e. contain nulls)
 * in the writer_struct_type.
 *
 * Notation:
 * - host_struct_type: the table's existing schema (the one we want to write
 * data to).
 * - writer_struct_type: schema derived from the incoming record/write.
 *
 * On success, destination fields are assigned the corresponding source field
 * IDs. On failure, destination struct is in an undefined state and should be
 * thrown away.
 */
ids_filled try_fill_field_ids(
  const struct_type& host_struct_type,
  struct_type& writer_struct_type,
  field_name_comparison norm);

using schema_merge_result = checked<void, schema_evolution_errc>;

/**
 * Merge the writer struct type into host struct using Iceberg schema evolution
 * rules such that on successful result the host struct can accept writes using
 * the writer struct.
 *
 * Notation:
 * - writer_struct_type: schema derived from the incoming record/write.
 * - host_struct_type: the table's existing schema (the one we are merging
 * into).
 *
 * Conflict resolution:
 * - Non-structural metadata (e.g., field comments, field order):
 * `host_struct_type` wins.
 * - Structural incompatibilities: return an error.
 *
 * On error, `host_struct_type` may be partially updated; discard it in that
 * case.
 */
schema_merge_result merge_struct_types(
  const struct_type& writer_struct_type,
  struct_type& host_struct_type,
  field_name_comparison norm);

} // namespace iceberg
