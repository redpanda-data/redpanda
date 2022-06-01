#!/usr/bin/env python3
import jinja2
import sys
from enum import Enum
from typing import List
from random import randrange


class BasicType(Enum):
    NONE = 0

    # primitives
    INT_8 = 1
    INT_16 = 2
    INT_32 = 3
    INT_64 = 4
    CHAR = 5
    FLOAT = 6
    DOUBLE = 7

    # complex types
    STRING = 100
    IOBUF = 101

    # template<T> types
    VECTOR = 200
    OPTIONAL = 201

    # struct
    STRUCT = 300


class Type:
    """
    >>> str(Type(BasicType.INT_8))
    'std::int8_t'
    >>> str(Type(basic_type = BasicType.VECTOR, template_type = Type(BasicType.STRING)))
    'std::vector<ss::string>'
    >>> str(Type(BasicType.STRUCT, Struct('my_struct')))
    'my_struct'
    """
    def __init__(self, basic_type: BasicType, template_type=None):
        self._basic_type = basic_type
        self._template_type = template_type

    def __str__(self):
        if self._basic_type == BasicType.INT_8:
            return "std::int8_t"
        elif self._basic_type == BasicType.INT_16:
            return "std::int16_t"
        elif self._basic_type == BasicType.INT_32:
            return "std::int32_t"
        elif self._basic_type == BasicType.INT_64:
            return "std::int64_t"
        elif self._basic_type == BasicType.CHAR:
            return "char"
        elif self._basic_type == BasicType.FLOAT:
            return "float"
        elif self._basic_type == BasicType.DOUBLE:
            return "double"
        elif self._basic_type == BasicType.STRING:
            return "ss::sstring"
        elif self._basic_type == BasicType.IOBUF:
            return "iobuf"
        elif self._basic_type == BasicType.VECTOR:
            return "std::vector<{}>".format(self._template_type)
        elif self._basic_type == BasicType.OPTIONAL:
            return "std::optional<{}>".format(self._template_type)
        elif self._basic_type == BasicType.STRUCT:
            return self._template_type._name
        else:
            raise Exception("unknown type")


class Field:
    """
    >>> str(Field("_str_vec", Type(basic_type = BasicType.VECTOR, template_type = Type(BasicType.STRING))))
    'std::vector<ss::sstring> _str_vec;'
    """
    def __init__(self, name: str, field_type: Type):
        self._name = name
        self._type = field_type

    def __str__(self):
        return "{} {};".format(self._type, self._name)


class Struct:
    """
    >>> str(Struct(name = 'my_struct', fields = [[Field('_f1', Type(BasicType.INT_32))]]))
    'struct my_struct : serde::envelope<my_struct, serde::version<10>, serde::compat_version<5>> {\\n  std::int32_t _f1;\\n};'
    """
    def __init__(self,
                 name: str,
                 version: int,
                 compat_version: int,
                 field_generations: List[List[Field]] = []):
        self._name = name
        self._version = version
        self._compat_version = compat_version
        self._field_generations = field_generations

    def __str__(self):
        return jinja2.Template(
            """struct {{ s._name }} : serde::envelope<{{ s._name }}, serde::version<{{ s._version }}>, serde::compat_version<{{ s._compat_version }}>> {
  bool operator==({{ s._name }} const&) const = default;

  template <std::size_t Generation>
  auto get_generation() {
{%- for generation in s._field_generations %}
    if constexpr (Generation == {{ loop.index - 1 }}) {
      return std::tie(
      {%- for field in generation %}
        {{ field._name }}{{ ", " if not loop.last else "" }}
      {%- endfor %}
      );
    }
{%- endfor %}
    static_assert(Generation < {{ s._field_generations | length }});
  }


{% for generation in s._field_generations %}
    {%- for field in generation %}
  {{ field }}
    {%- endfor %}
{%- endfor %}
};
""").render(s=self)


FILE_TEMPLATE = """#include "serde/envelope.h"
#include "serde/serde.h"

template<typename... T>
struct type_list {};


{%- for l in structs_lists %}
{% set outer_loop = loop %}
{%- for structs in l %}

{%- for s in structs %}
{{ s }}
{%- endfor %}

using types_{{ outer_loop.index * 10 + loop.index }} = type_list<
{%- for s in structs %}
  {{ s._name }}{{ ", " if not loop.last else "" }}
{%- endfor %}
>;

{%- endfor %}
{%- endfor %}
 
"""

my_struct = Struct(name='my_struct',
                   field_generations=[[Field('_f1', Type(BasicType.INT_32))]],
                   version=0,
                   compat_version=0)

types = [
    Type(BasicType.INT_8),
    Type(BasicType.INT_16),
    Type(BasicType.INT_32),
    Type(BasicType.INT_64),
    Type(BasicType.CHAR),
    Type(BasicType.FLOAT),
    Type(BasicType.DOUBLE),
    Type(BasicType.STRING),
    Type(BasicType.IOBUF),
    Type(BasicType.VECTOR, Type(BasicType.INT_32)),
    Type(BasicType.VECTOR, Type(BasicType.OPTIONAL, Type(BasicType.STRING))),
    Type(BasicType.OPTIONAL, Type(BasicType.STRING)),
    Type(BasicType.STRUCT, my_struct)
]


def gen_fields(id):
    ids = [randrange(len(types)) for i in range(3)]
    return [
        Field(name="_f{}".format(id + field_idx), field_type=types[type_id])
        for field_idx, type_id in enumerate(ids)
    ]


struct_idx = 0


def gen_struct(version: int, compat_version: int):
    global struct_idx
    base_fields = gen_fields(0)
    extend_fields_1 = gen_fields(len(base_fields))
    extend_fields_2 = gen_fields(len(base_fields) + len(extend_fields_1))
    struct_idx += 1
    return [
        Struct(name="my_struct_{}_v1".format(struct_idx),
               field_generations=[base_fields],
               version=version,
               compat_version=compat_version),
        Struct(name="my_struct_{}_v2".format(struct_idx),
               field_generations=[base_fields, extend_fields_1],
               version=version + 1,
               compat_version=compat_version),
        Struct(
            name="my_struct_{}_v3".format(struct_idx),
            field_generations=[base_fields, extend_fields_1, extend_fields_2],
            version=version + 2,
            compat_version=compat_version)
    ]


def extend_type_list(struct_type: Type):
    types.extend([
        struct_type,
        Type(BasicType.OPTIONAL, struct_type),
        Type(BasicType.VECTOR, struct_type),
        Type(BasicType.OPTIONAL, Type(BasicType.VECTOR, struct_type)),
        Type(BasicType.VECTOR, Type(BasicType.OPTIONAL, struct_type))
    ])


def gen_structs(version, compat_version):
    structs = [[], [], []]
    for i in range(3):
        for j in range(3):
            previous = gen_struct(version, compat_version)
            extend_type_list(Type(BasicType.STRUCT, previous[0]))
            structs[0].append(previous[0])
            structs[1].append(previous[1])
            structs[2].append(previous[2])
    return structs


if __name__ == "__main__":
    assert len(sys.argv) == 2
    out_file = sys.argv[1]
    with open(out_file, "w") as f:
        f.write(
            jinja2.Template(FILE_TEMPLATE).render(
                structs_lists=[[[my_struct]],
                               gen_structs(3, 3),
                               gen_structs(4, 4)]))
