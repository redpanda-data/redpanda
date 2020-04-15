#
# Code generator for kafka messages
# =================================
#
# Message schemas are taken from the 2.4 branch.
#
# Kafka reference on schema:
#     https://github.com/apache/kafka/blob/2.4/clients/src/main/resources/common/message/README.md
#
# TODO:
#   - The current version does not handle flexible versions. It will be some
#   time before we encounter clients requiring this, but in principle this
#   generator should be extensible (see type maps below). Note that when the
#   time comes to support flexible versions, there is a filter in the templates
#   within this file that ignores such fields, and that filter should be
#   removed.
#
#   - Handle ignorable fields. Currently we handle nullable fields properly. The
#   ignorable flag on a field doesn't change the wire protocol, but gives
#   instruction on how things should behave when there is missing data.
#
#   - Auto generate output operator
#
import io
import json
import functools
import pathlib
import re
import sys
import textwrap
import jsonschema
import jinja2

# Type overrides
# ==============
#
# The following four mappings:
#
#    - path_type_map
#    - entity_type_map
#    - field_name_type_map
#    - basic_type_map
#
# control how the types in a kafka message schema translate to redpanda types.
# the mappings are in order of preference. that is, a match in path_type_map
# will override a match in the entity_type_map.

# nested dictionary path within a json document
path_type_map = {}

# a few kafka field types specify an entity type
entity_type_map = dict()

# mapping specified as a combination of native type and field name
field_name_type_map = {}

# primitive types
basic_type_map = dict(
    string=("ss::sstring", "read_string()", "read_nullable_string()"),
    int8=("int8_t", "read_int8()"),
    int16=("int16_t", "read_int16()"),
    int32=("int32_t", "read_int32()"),
    int64=("int64_t", "read_int64()"),
)

# a listing of expected struct types
STRUCT_TYPES = []

SCALAR_TYPES = list(basic_type_map.keys())
ENTITY_TYPES = list(entity_type_map.keys())


class VersionRange:
    """
    A version range is fundamentally a range [min, max] but there are several
    different ways in the kafka schema format to specify the bounds.
    """
    def __init__(self, spec):
        self.min, self.max = self._parse(spec)

    def _parse(self, spec):
        match = re.match("^(?P<min>\d+)$", spec)
        if match:
            min = int(match.group("min"))
            return min, min

        match = re.match("^(?P<min>\d+)\+$", spec)
        if match:
            min = int(match.group("min"))
            return min, None

        match = re.match("^(?P<min>\d+)\-(?P<max>\d+)$", spec)
        if match:
            min = int(match.group("min"))
            max = int(match.group("max"))
            return min, max

    def guard(self):
        """
        Generate the C++ bounds check.
        """
        if self.min == self.max:
            cond = f"version == api_version({self.min})"
        else:
            cond = []
            if self.min > 0:
                cond.append(f"version >= api_version({self.min})")
            if self.max != None:
                cond.append(f"version <= api_version({self.max})")
            cond = " && ".join(cond)
        return cond

    def __repr__(self):
        max = "+inf)" if self.max is None else f"{self.max}]"
        return f"[{self.min}, {max}"


def snake_case(name):
    """Convert camel to snake case"""
    assert name[0].isupper(), name
    return name[0].lower() + "".join([f"_{c.lower()}" \
            if c.isupper() else c for c in name[1:]])


class FieldType:
    ARRAY_RE = re.compile("^\[\](?P<type>.+)$")

    def __init__(self, name):
        self._name = name

    @staticmethod
    def create(field, path):
        """
        FieldType factory based on Kafka field type name:

            int32 -> Int32Type
            []int32 -> ArrayType(Int32Type)
            []FooType -> ArrayType(StructType)

        Verifies that structs are only stored in arrays and that there are no array
        of array types like [][]FooType.
        """
        type_name = field["type"]

        match = FieldType.ARRAY_RE.match(type_name)
        is_array = match is not None
        if is_array:
            type_name = match.group("type")
            # we do not assume 2d arrays
            assert FieldType.ARRAY_RE.match(type_name) is None

        if type_name in SCALAR_TYPES:
            t = ScalarType(type_name)
        else:
            assert is_array
            path = path + (field["name"], )
            t = StructType(type_name, field["fields"], path)

        if is_array:
            return ArrayType(t)

        return t

    @property
    def is_struct(self):
        return False

    @property
    def name(self):
        return self._name


class ScalarType(FieldType):
    def __init__(self, name):
        super().__init__(name)


class StructType(FieldType):
    def __init__(self, name, fields, path=()):
        super().__init__(snake_case(name))
        self.fields = [Field.create(f, path) for f in fields]

    @property
    def is_struct(self):
        return True

    def structs(self):
        """
        Return all struct types reachable from this struct.
        """
        res = []
        for field in self.fields:
            t = field.type()
            if isinstance(t, ArrayType):
                t = t.value_type()  # unwrap value type
            if isinstance(t, StructType):
                res += t.structs()
                res.append(t)
        return res


class ArrayType(FieldType):
    def __init__(self, value_type):
        # the name of the ArrayType is its value type
        super().__init__(value_type._name)
        self._value_type = value_type

    def value_type(self):
        return self._value_type


class Field:
    def __init__(self, field, field_type, path):
        self._field = field
        self._type = field_type
        self._path = path + (self._field["name"], )
        self._versions = VersionRange(self._field["versions"])
        self._nullable_versions = self._field.get("nullableVersions", None)
        if self._nullable_versions is not None:
            self._nullable_versions = VersionRange(self._nullable_versions)
        assert len(self._path)

    @staticmethod
    def create(field, path):
        field_type = FieldType.create(field, path)
        return Field(field, field_type, path)

    def type(self):
        return self._type

    def nullable(self):
        return self._nullable_versions is not None

    def versions(self):
        return self._versions

    def about(self):
        return self._field["about"]

    def _redpanda_path_type(self):
        """
        Resolve a redpanda field path type override.
        """
        d = path_type_map
        for p in self._path:
            d = d.get(p, None)
            if d is None:
                break
        if isinstance(d, tuple):
            return d
        return None

    def _redpanda_type(self):
        """
        Resolve a redpanda type override.
        Lookup occurs from most to least specific.
        """
        # path overrides
        path_type = self._redpanda_path_type()
        if path_type:
            return path_type[0]

        # entity type overrides
        et = self._field.get("entityType", None)
        if et in entity_type_map:
            return entity_type_map[et][0]

        tn = self._type.name
        fn = self._field["name"]

        # type/name overrides
        if (tn, fn) in field_name_type_map:
            return field_name_type_map[(tn, fn)]

        # fundamental type overrides
        if tn in basic_type_map:
            return basic_type_map[tn][0]

        return tn

    def _redpanda_decoder(self):
        """
        Resolve a redpanda type override.
        Lookup occurs from most to least specific.
        """
        # path overrides
        path_type = self._redpanda_path_type()
        if path_type:
            return basic_type_map[path_type[1]], path_type[0]

        # entity type overrides
        et = self._field.get("entityType", None)
        if et in entity_type_map:
            m = entity_type_map[et]
            return basic_type_map[m[1]], m[0]

        tn = self._type.name
        fn = self._field["name"]

        # type/name overrides
        if (tn, fn) in field_name_type_map:
            return basic_type_map[tn], field_name_type_map[(tn, fn)]

        # fundamental type overrides
        if tn in basic_type_map:
            return basic_type_map[tn], None

        raise Exception(f"No decoder for {(tn, fn)}")

    @property
    def decoder(self):
        """
        There are two cases:

            1a. plain native: read_int32()
            1b. nullable native: read_nullable_string()
            1c. named types: named_type(read_int32())

            2a. optional named types:
                auto tmp = read_nullable_string()
                if (tmp) {
                  named_type(*tmp)
                }
        """
        plain_decoder, named_type = self._redpanda_decoder()
        if self.nullable():
            return plain_decoder[2], named_type
        return plain_decoder[1], named_type

    @property
    def is_array(self):
        return isinstance(self._type, ArrayType)

    @property
    def type_name(self):
        name = self._redpanda_type()
        if isinstance(self._type, ArrayType):
            name = f"std::vector<{name}>"
        if self.nullable():
            return f"std::optional<{name}>"
        return name

    @property
    def value_type(self):
        assert self.is_array
        return self._redpanda_type()

    @property
    def name(self):
        return snake_case(self._field["name"])


HEADER_TEMPLATE = """
#pragma once
#include "kafka/errors.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <cstdint>
#include <optional>
#include <vector>

{% macro render_struct(struct) %}
{{ render_struct_comment(struct) }}
struct {{ struct.name }} {
{%- for field in struct.fields %}
    {{ field.type_name }} {{ field.name }};
{%- endfor %}
{% endmacro %}

namespace kafka {

class request_reader;
class response_writer;
class request_context;
class response;

{% for struct in struct.structs() %}
{{ render_struct(struct) }}
};
{% endfor %}

{{ render_struct(struct) }}
{%- if op_type == "request" %}
    void encode(response_writer& writer, api_version version);
    void decode(request_reader& reader, api_version version);
{%- else %}
    void encode(const request_context& ctx, response& resp);
    void decode(iobuf buf, api_version version);
{%- endif %}
};

}
"""

SOURCE_TEMPLATE = """
#include "kafka/requests/schemata/{{ header }}"

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "kafka/requests/response_writer.h"
#include "seastarx.h"

#include <chrono>

{% macro version_guard(field) %}
{%- set cond = field.versions().guard() %}
{%- if cond %}
if ({{ cond }}) {
{{- caller() | indent }}
}
{%- else %}
{{- caller() }}
{%- endif %}
{%- endmacro %}

{% macro field_encoder(field, obj) %}
{%- if obj %}
{%- set fname = obj + "." + field.name %}
{%- else %}
{%- set fname = field.name %}
{%- endif %}
{%- if field.is_array %}
{%- if field.nullable() %}
writer.write_nullable_array({{ fname }}, [version]({{ field.value_type }}& v, response_writer& writer) {
{%- else %}
writer.write_array({{ fname }}, [version]({{ field.value_type }}& v, response_writer& writer) {
{%- endif %}
{%- if field.type().value_type().is_struct %}
{{- struct_serde(field.type().value_type(), field_encoder, "v") | indent }}
{%- else %}
    writer.write(v);
{%- endif %}
});
{%- else %}
writer.write({{ fname }});
{%- endif %}
{%- endmacro %}

{% macro field_decoder(field, obj) %}
{%- if obj %}
{%- set fname = obj + "." + field.name %}
{%- else %}
{%- set fname = field.name %}
{%- endif %}
{%- if field.is_array %}
{%- if field.nullable() %}
{{ fname }} = reader.read_nullable_array([version](request_reader& reader) {
{%- else %}
{{ fname }} = reader.read_array([version](request_reader& reader) {
{%- endif %}
{%- if field.type().value_type().is_struct %}
    {{ field.type().value_type().name }} v;
{{- struct_serde(field.type().value_type(), field_decoder, "v") | indent }}
    return v;
{%- else %}
{%- set decoder, named_type = field.decoder %}
{%- if named_type == None %}
    return reader.{{ decoder }};
{%- elif field.nullable() %}
    {
        auto tmp = reader.{{ decoder }};
        if (tmp) {
            return {{ named_type }}(std::move(*tmp));
        }
        return std::nullopt;
    }
{%- else %}
    return {{ named_type }}(reader.{{ decoder }});
{%- endif %}
{%- endif %}
});
{%- else %}
{%- set decoder, named_type = field.decoder %}
{%- if named_type == None %}
{{ fname }} = reader.{{ decoder }};
{%- elif field.nullable() %}
{
    auto tmp = reader.{{ decoder }};
    if (tmp) {
        {{ fname }} = {{ named_type }}(std::move(*tmp));
    }
}
{%- else %}
{{ fname }} = {{ named_type }}(reader.{{ decoder }});
{%- endif %}
{%- endif %}
{%- endmacro %}

{% macro struct_serde(struct, field_serde, obj = "") %}
{%- for field in struct.fields %}
{%- call version_guard(field) %}
{{- field_serde(field, obj) }}
{%- endcall %}
{%- endfor %}
{%- endmacro %}

namespace kafka {

{%- if op_type == "request" %}
void {{ struct.name }}::encode(response_writer& writer, api_version version) {
{{- struct_serde(struct, field_encoder) | indent }}
}

void {{ struct.name }}::decode(request_reader& reader, api_version version) {
{{- struct_serde(struct, field_decoder) | indent }}
}
{%- else %}
void {{ struct.name }}::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    auto version = ctx.header().version;

{{- struct_serde(struct, field_encoder) | indent }}
}

void {{ struct.name }}::decode(iobuf buf, api_version version) {
    request_reader reader(std::move(buf));

{{- struct_serde(struct, field_decoder) | indent }}
}
{%- endif %}

}
"""

# This is the schema of the json files from the kafka tree. This isn't strictly
# necessary for the code generator, but it is useful. The schema verification
# performed on our input files from kafka is _very_ strict. Since the json files
# in kafka do not seem to have any sort of formalized structure, verification
# is a check on our assumptions. If verification fails, it should be taken as an
# indication that the generator may need to be updated.
ALLOWED_TYPES = \
    SCALAR_TYPES + \
    [f"[]{t}" for t in SCALAR_TYPES + STRUCT_TYPES]

# yapf: disable
SCHEMA = {
    "definitions": {
        "versions": {
            "oneOf": [
                {
                    "type": "string",
                    "pattern": "^\d+$"
                },
                {
                    "type": "string",
                    "pattern": "^\d+\-\d+$"
                },
                {
                    "type": "string",
                    "pattern": "^\d+\+$"
                },
            ],
        },
        "field": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "type": {
                    "type": "string",
                    "enum": ALLOWED_TYPES,
                },
                "versions": { "$ref": "#/definitions/versions" },
                "nullableVersions": { "$ref": "#/definitions/versions" },
                "entityType": {
                    "type": "string",
                    "enum": ENTITY_TYPES,
                },
                "about": {"type": "string"},
                "default": {
                    "oneOf": [
                        {"type": "integer"},
                        {"type": "string"},
                    ],
                },
                "mapKey": {"type": "boolean"},
                "ignorable": {"type": "boolean"},
                "fields": {
                    "type": "array",
                    "items": { "$ref": "#/definitions/field" },
                },
            },
            "required": [
                "name",
                "type",
                "versions",
            ],
            "additionalProperties": False,
        },
        "fields": {
            "type": "array",
            "items": { "$ref": "#/definitions/field" },
        },
    },
    "type": "object",
    "properties": {
        "apiKey": {"type": "integer"},
        "type": {
            "type": "string",
            "enum": ["request", "response"],
        },
        "name": {"type": "string"},
        "validVersions": { "$ref": "#/definitions/versions" },
        "flexibleVersions": { "$ref": "#/definitions/versions" },
        "fields": { "$ref": "#/definitions/fields" },
    },
    "required": [
        "apiKey",
        "type",
        "name",
        "validVersions",
        "flexibleVersions",
        "fields",
    ],
    "additionalProperties": False,
}
# yapf: enable


# helper called from template to render a nice struct comment
def render_struct_comment(struct):
    indent = " * "
    wrapper = textwrap.TextWrapper(initial_indent=indent,
                                   subsequent_indent=indent,
                                   width=80)
    comment = wrapper.fill(f"The {struct.name} message.") + "\n"
    comment += indent + "\n"

    max_width = 0
    for field in struct.fields:
        max_width = max(len(field.name), max_width)

    for field in struct.fields:
        field_indent = indent + f"{field.name:>{max_width}}: "
        wrapper = textwrap.TextWrapper(initial_indent=field_indent,
                                       subsequent_indent=indent + " " *
                                       (2 + max_width),
                                       width=80)
        about = field.about() + f" Supported versions: {field.versions()}"
        comment += wrapper.fill(about) + "\n"

    return f"/*\n{comment} */"


if __name__ == "__main__":
    assert len(sys.argv) == 3
    outdir = pathlib.Path(sys.argv[1])
    schema_path = pathlib.Path(sys.argv[2])
    src = (outdir / schema_path.name).with_suffix(".cc")
    hdr = (outdir / schema_path.name).with_suffix(".h")

    # remove comments from the json file. comments are a non-standard json
    # extension that is not supported by the python json parser.
    schema = io.StringIO()
    with open(schema_path, "r") as f:
        for line in f.readlines():
            line = re.sub("\/\/.*", "", line)
            if line.strip():
                schema.write(line)

    # parse json and verify its schema.
    msg = json.loads(schema.getvalue())
    jsonschema.validate(instance=msg, schema=SCHEMA)

    # the root struct in the schema corresponds to the root type in redpanda.
    # but its naming in snake case will conflict with our high level request and
    # response types so arrange for a "_data" suffix to be generated.
    type_name = f"{msg['name']}Data"
    struct = StructType(type_name, msg["fields"], (type_name, ))

    # request or response
    op_type = msg["type"]

    with open(hdr, 'w') as f:
        f.write(
            jinja2.Template(HEADER_TEMPLATE).render(
                struct=struct,
                render_struct_comment=render_struct_comment,
                op_type=op_type))

    with open(src, 'w') as f:
        f.write(
            jinja2.Template(SOURCE_TEMPLATE).render(struct=struct,
                                                    header=hdr.name,
                                                    op_type=op_type))
