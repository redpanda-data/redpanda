#!/usr/bin/env python3
# Copyright 2020 Vectorized, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
import re
import sys
import os
import logging
import json
from shutil import copy

# 3rd party
import jinja2

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

serializableFunctions = """
{%- macro write_int8(field, propertyPath, buffer, assign) %}
    {% set jsFn = "writeUInt8LE" if 'u' in field.type else "writeInt8LE" -%}
    {{ "writtenBytes += " if assign -}}
    BF.{{jsFn}}({{propertyPath}}, {{buffer}})
{%- endmacro -%}

{%- macro write_int16(field, propertyPath, buffer, assign) %}
    {% set jsFn = "writeUInt16LE" if 'u' in field.type else "writeInt16LE" -%}
    {{ "writtenBytes += " if assign -}}
    BF.{{jsFn}}({{propertyPath}}, {{buffer}})
{%- endmacro -%}

{%- macro write_int32(field, propertyPath, buffer, assign) %}
    {% set jsFn = "writeUInt32LE" if 'u' in field.type else "writeInt32LE" -%}
    {{ "writtenBytes += " if assign -}}
    BF.{{jsFn}}({{propertyPath}}, {{buffer}})
{%- endmacro -%}

{%- macro write_int64(field, propertyPath, buffer, assign) %}
    {% set jsFn = "writeUInt64LE" if 'u' in field.type else "writeInt64LE" -%}
    {{ "writtenBytes += " if assign -}}
    BF.{{jsFn}}({{propertyPath}}, {{buffer}})
{%- endmacro -%}

{%- macro write_string(field, propertyPath, buffer, assign) %}
    {{ "writtenBytes += " if assign -}}
    BF.writeString({{propertyPath}}, {{buffer}})
{%- endmacro -%}

{%- macro write_buffer(field, propertyPath, buffer, assign) %}
    {{ "writtenBytes += " if assign -}}
    BF.writeBuffer({{propertyPath}}, {{buffer}})
{%- endmacro -%}

{%- macro write_boolean(field, propertyPath, buffer, assign) %}
    {{ "writtenBytes += " if assign -}}
    BF.writeBoolean({{propertyPath}}, {{buffer}})
{%- endmacro -%}

{%- macro write_varint(field, propertyPath, buffer, assign) %}
    {{ "writtenBytes += " if assign -}}
    BF.writeVarint({{propertyPath}}, {{buffer}})
{%- endmacro -%}

{%- macro write_array(field, propertyPath, buffer, assign) %}
    {# Remove ">" and "Array<" from type, the result is the array type #}
    {%-set subtype = field.type | get_value_type %}
    {{ "writtenBytes += " if assign -}}
    BF.writeArray({{"false" if field.size else "true"}})({{propertyPath}},
      buffer, (item, auxBuffer) => {{-serialize_by_field
            ({"name": "", 
              "type": subtype},
              "item", "auxBuffer",
               False)
              }}
    )
{%- endmacro -%}

{%- macro write_object(field, propertyPath, buffer, assign) %}
    {{ "writtenBytes += " if assign -}}
    BF.writeObject({{buffer}}, {{field.type}}, {{propertyPath}})
{%- endmacro -%}

{%- macro write_optional(field, propertyPath, buffer, assign) %}
    {# Remove ">" and "Optional<" from type, the result is the optional type #}
    {%-set subtype = field.type | get_value_type %}
    {{ "writtenBytes += " if assign -}}
    BF.writeOptional({{buffer}}, {{propertyPath}}, (item, auxBuffer) => 
        {{-serialize_by_field({"name": "", 
              "type": subtype},
              "item", "auxBuffer",
               False)
         }})
{%- endmacro -%}

{%-macro serialize_by_field(field, pathParameter, inBuffer, assign)-%}
    {%- set buffer = inBuffer | default("buffer", True) -%}
    {%- set offset = inOffset | default("offset", True) -%}
    {%- set assignOffset = assign | default(False, True) -%}
    {%- set path = pathParameter | default("value." + field.name, True) -%}
    {%- set main_type = field.type | get_type -%}

    {%- if 'Optional' in main_type -%}
    {{ write_optional(field, path, buffer, assignOffset)}}
    {%- elif 'Array' in main_type -%}
    {{ write_array(field, path, buffer, assignOffset)}}
    {%- elif "int8" in main_type -%}
    {{ write_int8(field, path, buffer, assignOffset) }}
    {%- elif "int16" in main_type -%}
    {{ write_int16(field, path, buffer, assignOffset) }}
    {%- elif "int32" in main_type -%}
    {{ write_int32(field, path, buffer, assignOffset) }}
    {%- elif "int64" in main_type -%}
    {{ write_int64(field, path, buffer, assignOffset) }}
    {%- elif main_type == "string" -%}
    {{- write_string(field, path, buffer, assignOffset) }}
    {%- elif main_type == "boolean" -%}
    {{ write_boolean(field, path, buffer, assignOffset) }}
    {%- elif main_type == "varint" -%}
    {{ write_varint(field, path, buffer, assignOffset) }}
    {%- elif main_type == "buffer" -%}
    {{- write_buffer(field, path, buffer, assignOffset) }}
    {%- else -%}
    {{ write_object(field, path, buffer, assignOffset) }}
    {%- endif -%}
 {%- endmacro %}
"""

deserializableFunctions = """
{%- macro read_int8(buffer, offset, func, type) -%}
    {%- set jsFn = "readUInt8LE" if 'u' in type else "readInt8LE" -%}
    {%- if func == False -%}
        (() => {
            const [value, newOffset] = BF.{{jsFn}}({{buffer}}, {{offset}});
            {{offset}} = newOffset;
            return value;
        })()
    {%- else -%}
        ({{buffer}}, {{offset}}) => BF.{{jsFn}}({{buffer}}, {{offset}})
    {%- endif -%}
{%- endmacro %}

{%- macro read_int16(buffer, offset, func, type) -%}
    {%- set jsFn = "readUInt16LE" if 'u' in type else "readInt16LE" -%}
    {%- if func == False -%}
        (() => {
            const [value, newOffset] = BF.{{jsFn}}({{buffer}}, {{offset}});
            {{offset}} = newOffset;
            return value;
        })()
    {%- else -%}
        ({{buffer}}, {{offset}}) => BF.{{jsFn}}({{buffer}}, {{offset}})
    {%- endif -%}
{%- endmacro %}

{%- macro read_int32(buffer, offset, func, type) -%}
    {%- set jsFn = "readUInt32LE" if 'u' in type else "readInt32LE" -%}
    {%- if func == False -%}
        (() => {
            const [value, newOffset] = BF.{{jsFn}}({{buffer}}, {{offset}});
            {{offset}} = newOffset;
            return value;
        })()
    {%- else -%}
        ({{buffer}}, {{offset}}) => BF.{{jsFn}}({{buffer}}, {{offset}})
    {%- endif -%}
{%- endmacro %}

{%- macro read_int64(buffer, offset, func, type) -%}
    {%- set jsFn = "readUInt64LE" if 'u' in type else "readInt64LE" -%}
    {%- if func == False -%}
        (() => {
            const [value, newOffset] = BF.{{jsFn}}({{buffer}}, {{offset}});
            {{offset}} = newOffset;
            return value;
        })()
    {%- else -%}
        ({{buffer}}, {{offset}}) => BF.{{jsFn}}({{buffer}}, {{offset}})
    {%- endif -%}
{%- endmacro %}
 
{%- macro read_string(buffer, offset, func) -%}
    {%- if func == False -%}
    (() =>{
        const [value, newOffset] = BF.readString({{buffer}}, {{offset}});
        {{offset}} = newOffset;
        return value;
    })()
    {%- else -%}
        ({{buffer}}, {{offset}}) => BF.readString({{buffer}}, {{offset}})
    {%- endif -%}  
{%- endmacro %}

{%- macro read_buffer(buffer, offset, func) -%}
    {%- if func == False -%}
    (() =>{
        const [value, newOffset] = BF.readBuffer({{buffer}}, {{offset}});
        {{offset}} = newOffset;
        return value;
    })()
    {%- else -%}
        ({{buffer}}, {{offset}}) => BF.readBuffer({{buffer}}, {{offset}})
    {%- endif -%}  
{%- endmacro %}
 
{%- macro read_boolean(buffer, offset, func) -%}
    {%- if func == False -%}
    (() => {  
        const [value, newOffset] = BF.readBoolean({{buffer}}, {{offset}});
        {{offset}} = newOffset;
        return value;
    })()
    {%- else -%}
        ({{buffer}}, {{offset}}) => BF.readBoolean({{buffer}}, {{offset}})
    {%- endif -%}  
{%- endmacro %}
 

{%- macro read_varint(buffer, offset, func) -%}
    {%- if func == False -%}    
    (() => {  
        const [value, newOffset] = BF.readVarint({{buffer}}, {{offset}});
        {{offset}} = newOffset;
        return value;
    })()
    {%- else -%}
        ({{buffer}}, {{offset}}) => BF.readVarint({{buffer}}, {{offset}})
    {%- endif -%}
{%- endmacro %}
 
{%- macro read_object(type, buffer, offset, func) -%}
    {%- if func == False -%}    
    (() => {  
        const [value, newOffset] = 
        BF.readObject({{buffer}}, {{offset}}, {{type}});
        {{offset}} = newOffset;
        return value;
    })()
    {%- else -%}
        ({{buffer}}, {{offset}}) => 
        BF.readObject({{buffer}}, {{offset}}, {{type}})
    {%- endif -%}
{%- endmacro %}

{%- macro read_array(type, buffer, offset, func, size) -%}
    {# Remove ">" and "Array<" from type, the result is the array type #}
    {%-set subtype = type | get_value_type -%}
    {%- if func == False -%}
    (() => {
        const [array, newOffset] = BF.readArray({{size}})({{buffer}}, {{offset}}, 
        {{- deserialize_by_type({"type": subtype}, "auxBuffer", "auxOffset", True) -}})
        offset = newOffset
        return array;
    })()
    {%- else -%}
        ({{buffer}}, {{offset}}) => 
        BF.readArray({{size}})({{buffer}}, {{offset}}, 
            {{- deserialize_by_type({"type": subtype}, "auxBuffer", "auxOffset", True) -}})
    {%- endif -%}
{%- endmacro %}

{%- macro read_optional(type, buffer, offset, func) -%}
    {# Remove ">" and "Optional<" from type, the result is the optional type #}
    {%-set subtype = type | get_value_type -%}
    {%- if func == False -%}
    (() => {
        const [optional, newOffset] = BF.readOptional({{buffer}}, {{offset}},
        {{- deserialize_by_type({"type": subtype}, "auxBuffer", "auxOffset", True) -}})
        offset = newOffset
        return optional;
    })()
    {%- else -%}
    ({{buffer}}, {{offset}}) => 
        BF.readOptional({{buffer}}, {{offset}},
        {{- deserialize_by_type({"type": subtype}, "auxBuffer", "auxOffset", True) -}})
    {%- endif -%}
{%- endmacro %}
 
{%- macro deserialize_by_type(field, inBuffer, inOffset, funcStyle) -%}
    {%- set buffer = inBuffer | default("buffer", True) -%}
    {%- set offset = inOffset | default("offset", True) -%}
    {%- set func = funcStyle | default(False, True) -%}
    {%- set type = field.type -%}
    {%- set main_type = type | get_type -%}
    {%- if 'Optional' in main_type %}
        {{ read_optional(type, buffer, offset, func)}}
    {%- elif 'Array' in main_type %}
        {{ read_array(type, buffer, offset, func, field.size)}}
    {%- elif "int8" in main_type %}
        {{ read_int8(buffer, offset, func, type) }}
    {%- elif "int16" in main_type %}
        {{ read_int16(buffer, offset, func, type) }}
    {%- elif "int32" in main_type %}
        {{ read_int32(buffer, offset, func, type) }}
    {%- elif "int64" in main_type %}
        {{ read_int64(buffer, offset, func, type) }}
    {%- elif main_type == "string" %}
        {{ read_string(buffer, offset, func) }}
    {%- elif main_type == "buffer" %}
        {{ read_buffer(buffer, offset, func) }}
    {%- elif main_type == "boolean" %}
        {{ read_boolean(buffer, offset, func) }}
    {%- elif main_type == "varint" %}
        {{ read_varint(buffer, offset, func) }}
    {%- else %}
        {{ read_object(type, buffer, offset, func) }}
    {%- endif %}
{%- endmacro %}

{%- macro convert_type(type) -%}
    {%- set main_type = type | get_type -%}
    {%- if 'Optional' in main_type-%}
    {%-set subtype = type | get_value_type -%}
    Optional<{{convert_type(subtype)}}>
    {%- elif 'Array' in main_type-%}
    {%-set subtype = type | get_value_type -%}
    Array<{{convert_type(subtype)}}>
    {%- elif main_type == "varint" -%}
    bigint
    {%- elif main_type == "int64" -%}
    bigint
    {%- elif main_type == "uint64" -%}
    bigint
    {%- elif 'int' in main_type -%}
    number
    {%- elif main_type == "buffer" -%}
    Buffer
    {%- else -%}
    {{type}}
    {%- endif -%}
{%- endmacro %}
"""

template = """
// Code generated by v/tools/ts-generator/rpcgen_js.py
// import Buffer Functions
import BF, {Optional} from "./functions";
import { IOBuf } from "../../utilities/IOBuf";
{% for class in classes %}
export class {{class.className}} {
    {# the order of the field definition is important #}
    {%- for field in class.fields %}
    public {{field.name}}: {{convert_type(field.type)}};
    {%- endfor %}
    
   /**
    * transform bytes into a buffer to {{class.className}}
    * @param buffer is the place where the binary data is stored
    * @param offset is the position where the function will start to
    *        read into buffer
    * @return a tuple, where first element is a {{class.className}} and 
    *        second one is the read last position in the buffer
    */
    static fromBytes(buffer: Buffer, offset = 0): [{{class.className}}, number]{
        {%- for field in class.fields %}
            const {{field.name}} = {{ deserialize_by_type(field) }}
        {%- endfor %} 
        
        return [{
        {%- for field in class.fields %}
            {{field.name}},
        {%- endfor -%}        
        }, offset]
    }
   /**
    * transform from {{class.className}} to binary version with Redpanda 
    * standard
    * @param value is a {{class.className}} instance
    * @param buffer is the binary array where the {{class.className}} binary 
    *        will save
    * @param offset is the position where the toBytes function starts to write
    *        in the buffer
    * @return the last position of the offset
    */
    static toBytes(
        value: {{class.className}},
        buffer: IOBuf
    ): number {
        let writtenBytes = 0;
        {% if class.customEncode is defined %}
          writtenBytes += {{class.customEncode}}(value, buffer)
        {%- else -%}
          {%- for field in class.fields %}
            {{- serialize_by_field(field, "", "", True) -}}
          {%- endfor %}
        {%- endif %}

        return writtenBytes
    }    
}
{%- endfor -%}
"""


def get_value_type(type):
    group = re.match('\w+<(.*)>', type)
    return group.group(1)


def get_type(type):
    group = re.search('([^<]*)<', type)
    if group is not None:
        return group.group(1)
    else:
        return type


def read_file(name):
    with open(name, 'r') as f:
        try:
            return json.load(f)
        except:
            logger.error(
                "Error: try to read input file, but there is "
                "a problem with json format ", name)


def create_class(json):
    env = jinja2.Environment(loader=jinja2.BaseLoader)
    env.filters['get_value_type'] = get_value_type
    env.filters['get_type'] = get_type
    tpl = env.from_string(serializableFunctions + deserializableFunctions +
                          template)
    return tpl.render(json)


def write(code_generated, out_path):
    open(out_path, 'w').write(code_generated)


def save_in_file(generated_code, path):
    (dir_name, file_name) = os.path.split(path)
    try:
        os.makedirs(dir_name)
    except FileExistsError:
        pass
    finally:
        write(generated_code, path)
        copy("./functions.ts", os.path.dirname(path))


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(
            description='deserializer and serializer code generator')
        parser.add_argument(
            '--log',
            type=str,
            default='INFO',
            help='info,debug, type log levels. i.e: --log=debug')
        parser.add_argument('--entities-define-file',
                            type=str,
                            required=True,
                            help='input file in .json format for the codegen')
        parser.add_argument('--output-file',
                            type=str,
                            required=True,
                            help='output header file for the codegen')
        return parser

    parser = generate_options()
    options, program_options = parser.parse_known_args()
    logger.info("%s" % options)
    json_file = read_file(options.entities_define_file)
    save_in_file(create_class(json_file), options.output_file)


if __name__ == '__main__':
    main()
