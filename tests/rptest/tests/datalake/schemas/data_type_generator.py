# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random

from rptest.tests.datalake.schemas.data_types import (
    ALL_PRIMITIVE_DATA_TYPES,
    GenericArray,
    GenericDataType,
    GenericEnum,
    GenericMap,
    GenericOptional,
    GenericRecord,
    GenericUnion,
)


class DataTypeGenerator:
    """
    TODO(willem): Split apart avro and protobuf schema generation, leave this class generic.
    """

    @staticmethod
    def generate_random_data_type(
        schema_generator, data_type: GenericDataType, num_sub_fields
    ):
        if data_type in ALL_PRIMITIVE_DATA_TYPES:
            return DataTypeGenerator.generate_primitive_type(
                schema_generator, data_type
            )
        elif data_type == GenericRecord:
            return DataTypeGenerator.generate_random_record(
                schema_generator, num_sub_fields
            )
        elif data_type == GenericArray:
            return DataTypeGenerator.generate_random_array(
                schema_generator, num_sub_fields
            )
        elif data_type == GenericMap:
            return DataTypeGenerator.generate_random_map(
                schema_generator, num_sub_fields
            )
        elif data_type == GenericEnum:
            return DataTypeGenerator.generate_random_enum(
                schema_generator, num_sub_fields
            )
        elif data_type == GenericUnion:
            return DataTypeGenerator.generate_random_union(
                schema_generator, num_sub_fields
            )
        elif data_type == GenericOptional:
            return DataTypeGenerator.generate_random_optional(
                schema_generator, num_sub_fields
            )
        else:
            raise NotImplementedError(f"Unknown data type {data_type.name()}")

    @staticmethod
    def generate_field(schema_generator, num_sub_fields):
        schema_generator.current_depth += 1
        num_sub_fields -= 1
        schema_generator.num_fields += 1
        data_type = schema_generator.get_random_data_type(num_sub_fields)
        field, template = DataTypeGenerator.generate_random_data_type(
            schema_generator, data_type, num_sub_fields
        )
        schema_generator.current_depth -= 1
        if data_type == GenericEnum:
            schema_generator.fields[field["name"]] = (
                data_type,
                field["type"]["symbols"],
            )
        else:
            schema_generator.fields[field["name"]] = (data_type, None)
        return field, template

    @staticmethod
    def generate_primitive_type(schema_generator, data_type: GenericDataType) -> dict:
        ret = dict()
        template = dict()
        field_name = f"field_{schema_generator.num_fields}"
        template[field_name] = "{{ %s }}" % f"{field_name}"
        ret["name"] = field_name
        ret["type"] = data_type.to_producer_type(schema_generator.producer_type)
        return ret, template

    @staticmethod
    def generate_random_record(schema_generator, num_sub_fields) -> dict:
        ret = dict()
        template = dict()
        field_name = f"field_{schema_generator.num_fields}"
        template = {}
        template[field_name] = {}
        ret["name"] = field_name
        ret["type"] = dict()
        ret["type"]["name"] = f"{field_name}_record"
        ret["type"]["type"] = GenericRecord.to_producer_type(
            schema_generator.producer_type
        )
        ret["type"]["fields"] = []
        while num_sub_fields > 0:
            next_num_sub_fields = random.randint(1, num_sub_fields)
            num_sub_fields -= next_num_sub_fields

            field = dict()
            field["name"] = f"field_{schema_generator.num_fields}"
            sub_field, field_temp = DataTypeGenerator.generate_field(
                schema_generator, next_num_sub_fields
            )
            field.update(sub_field)
            template[field_name].update(field_temp)

            ret["type"]["fields"].append(field)
        return ret, template

    @staticmethod
    def generate_random_array(schema_generator, num_sub_fields) -> dict:
        ret = dict()
        template = dict()
        field_name = f"field_{schema_generator.num_fields}"
        ret["name"] = field_name
        ret["type"] = dict()
        ret["type"]["type"] = GenericArray.to_producer_type(
            schema_generator.producer_type
        )
        field, field_temp = DataTypeGenerator.generate_field(
            schema_generator, num_sub_fields
        )
        ret["type"]["items"] = field["type"]
        template[field_name] = [v for v in field_temp.values()]
        return ret, template

    @staticmethod
    def generate_random_map(schema_generator, num_sub_fields) -> dict:
        ret = dict()
        template = dict()
        field_name = f"field_{schema_generator.num_fields}"
        template[field_name] = {}
        ret["name"] = field_name
        ret["type"] = dict()
        ret["type"]["type"] = GenericMap.to_producer_type(
            schema_generator.producer_type
        )
        field, field_temp = DataTypeGenerator.generate_field(
            schema_generator, num_sub_fields
        )
        ret["type"]["values"] = field["type"]
        template[field_name].update(field_temp)
        return ret, template

    @staticmethod
    def generate_random_enum(schema_generator, num_sub_fields) -> dict:
        ret = dict()
        template = dict()
        field_name = f"field_{schema_generator.num_fields}"
        template[field_name] = "{{ %s }}" % f"{field_name}"
        ret["name"] = field_name
        ret["type"] = dict()
        ret["type"]["name"] = f"{field_name}_enum"
        ret["type"]["type"] = GenericEnum.to_producer_type(
            schema_generator.producer_type
        )
        ret["type"]["symbols"] = [
            GenericEnum.make_valid_enum_name(10, schema_generator.producer_type)
            for _ in range(0, num_sub_fields)
        ]

        return ret, template

    @staticmethod
    def generate_random_union(schema_generator, num_sub_fields) -> dict:
        # TODO(willem): Implement union generation
        pass

    @staticmethod
    def generate_random_optional(schema_generator, num_sub_fields) -> dict:
        # TODO(willem): Implement optional generation
        pass
