# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import random

import jinja2
from avro.schema import parse
from rptest.tests.datalake.schemas.data_type_generator import DataTypeGenerator
from rptest.tests.datalake.schemas.data_types import (
    ALL_COMPLEX_DATA_TYPES,
    ALL_PRIMITIVE_DATA_TYPES,
    ALL_PRIMITIVE_DATA_TYPES_AND_ENUM,
    GenericDataType,
    GenericDouble,
    GenericEnum,
    GenericFloat,
    GenericInt,
    GenericLong,
    GenericPrimitive,
    GenericString,
    ProducerType,
)


class SchemaGenerator:
    def __init__(
        self,
        producer_type: ProducerType,
        complex_types: list[GenericDataType] = ALL_COMPLEX_DATA_TYPES,
        complex_type_probabilities: list[float] | None = None,
        primitive_types: list[GenericDataType] = ALL_PRIMITIVE_DATA_TYPES,
        primitive_type_probabilities: list[float] | None = None,
        max_depth: int = 5,
        max_fields: int = 10,
        max_fields_per_type: int = 10,
    ):
        if complex_type_probabilities is None:
            complex_type_probabilities = [1.0 / len(complex_types)] * len(complex_types)
        if primitive_type_probabilities is None:
            primitive_type_probabilities = [1.0 / len(primitive_types)] * len(
                primitive_types
            )

        assert len(complex_types) == len(complex_type_probabilities), (
            f"complex_type_probabilities should be of length {len(complex_types)}, is of length {len(complex_type_probabilities)}"
        )
        assert len(primitive_types) == len(primitive_type_probabilities), (
            f"primitive_type_probabilities should be of length {len(primitive_types)}, is of length {len(primitive_type_probabilities)}"
        )

        self.producer_type = producer_type
        self.max_depth = max_depth
        self.complex_types = complex_types
        self.complex_type_probabilities = complex_type_probabilities
        self.primitive_types = primitive_types
        self.primitive_type_probabilities = primitive_type_probabilities
        self.max_fields = max_fields
        self.max_fields_per_type = max_fields_per_type
        self.num_fields = 0
        self.current_depth = 0
        self.fields = dict()

    def num_fields_left(self) -> int:
        return self.max_fields - self.num_fields

    def num_sub_fields_for_field(self) -> int:
        return min(self.num_fields_left(), random.randint(1, self.max_fields_per_type))

    def can_recurse(self) -> bool:
        return self.current_depth < self.max_depth

    def get_random_data_type(self, num_sub_fields) -> GenericDataType:
        data_type = GenericPrimitive
        if self.can_recurse() and num_sub_fields > 1:
            data_type = random.choices(
                self.complex_types, weights=self.complex_type_probabilities
            )[0]
        if data_type is GenericPrimitive:
            data_type = random.choices(
                self.primitive_types, weights=self.primitive_type_probabilities
            )[0]
        return data_type

    def _make_avro_schema(self):
        fields = []
        template = {}
        while self.num_fields < self.max_fields:
            field, field_temp = DataTypeGenerator.generate_field(
                self, self.num_sub_fields_for_field()
            )
            fields.append(field)
            template.update(field_temp)
        raw_schema = dict()
        raw_schema["name"] = "schema"
        raw_schema["type"] = "record"
        raw_schema["fields"] = fields

        # Validate the schema with the avro library.
        schema = parse(json.dumps(raw_schema))
        return schema, jinja2.Template(json.dumps(template))

    def _make_proto_schema(self):
        pass

    def generate_schema_fields(self):
        if self.producer_type == ProducerType.AVRO:
            return self._make_avro_schema()
        else:
            return self._make_proto_schema()

    @staticmethod
    def make_random_record(
        schema, schema_fields: dict, schema_template, validate: bool = False
    ):
        field_values = {}
        for k, v in schema_fields.items():
            t, maybe_syms = v
            if t in ALL_PRIMITIVE_DATA_TYPES_AND_ENUM:
                if t == GenericEnum:
                    field_values[k] = t.random(maybe_syms)
                elif t == GenericString:
                    field_values[k] = t.random(random.randint(1, 100))
                elif t in [GenericInt, GenericLong, GenericFloat, GenericDouble]:
                    field_values[k] = t.random(0, 100)
                else:
                    field_values[k] = t.random()

        tmpl = schema_template.render(field_values)

        # Jinja template renders all values as strings. Have to fix the types here.
        def fix_types(obj):
            if isinstance(obj, str):
                try:
                    return int(obj)
                except Exception:
                    try:
                        return float(obj)
                    except Exception:
                        return obj
            elif isinstance(obj, dict):
                return {k: fix_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [fix_types(v) for v in obj]
            else:
                return obj

        ret = json.loads(tmpl, object_hook=fix_types)

        if validate:
            # Validate the generated record with the schema
            schema.validate(ret)

        return ret
