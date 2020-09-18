#!/usr/bin/env python3
import sys
import os
import logging
import json
from shutil import copy

# 3rd party
from jinja2 import Template

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

serializableFunctions = """
{%- macro write_int8(field, propertyPath, buffer, offset, assign) %}
    {% set jsFn = "writeUInt8LE" if 'u' in field.type else "writeInt8LE" -%}
    {{ "offset = " if assign -}} 
    BF.{{jsFn}}({{propertyPath}}, {{buffer}}, {{offset}})
{%- endmacro -%}

{%- macro write_int16(field, propertyPath, buffer, offset, assign) %}
    {% set jsFn = "writeUInt16LE" if 'u' in field.type else "writeInt16LE" -%}
    {{ "offset = " if assign -}} 
    BF.{{jsFn}}({{propertyPath}}, {{buffer}}, {{offset}})
{%- endmacro -%}

{%- macro write_int32(field, propertyPath, buffer, offset, assign) %}
    {% set jsFn = "writeUInt32LE" if 'u' in field.type else "writeInt32LE" -%}
    {{ "offset = " if assign -}} 
    BF.{{jsFn}}({{propertyPath}}, {{buffer}}, {{offset}})
{%- endmacro -%}

{%- macro write_int64(field, propertyPath, buffer, offset, assign) %}
    {% set jsFn = "writeUInt64LE" if 'u' in field.type else "writeInt64LE" -%}
    {{ "offset = " if assign -}} 
    BF.{{jsFn}}({{propertyPath}}, {{buffer}}, {{offset}})
{%- endmacro -%}

{%- macro write_string(field, propertyPath, buffer, offset, assign) %}
    {{ "offset = " if assign -}} 
    BF.writeString({{propertyPath}}, {{buffer}}, {{offset}})
{%- endmacro -%}

{%- macro write_buffer(field, propertyPath, buffer, offset, assign) %}
    {{ "offset = " if assign -}} 
    BF.writeBuffer({{propertyPath}}, {{buffer}}, {{offset}})
{%- endmacro -%}

{%- macro write_boolean(field, propertyPath, buffer, offset, assign) %}
    {{ "offset = " if assign -}} 
    BF.writeBoolean({{propertyPath}}, {{buffer}}, {{offset}})
{%- endmacro -%}

{%- macro write_varint(field, propertyPath, buffer, offset, assign) %}
    {{ "offset = " if assign -}} 
    BF.writeVarint({{propertyPath}}, {{buffer}}, {{offset}})
{%- endmacro -%}

{%- macro write_array(field, propertyPath) %}
    {# Remove ">" and "Array<" from type, the result is the array type #}
    {%-set subtype = field.type | replace(">","")|replace("Array<", "") %}
    offset = 
    BF.writeArray({{propertyPath}}, buffer, offset, 
        (item, auxBuffer, auxOffset) => {{-serialize_by_field
            ({"name": "", 
              "type": subtype},
              "item", "auxBuffer",
              "auxOffset", False)
              }}
    )
{%- endmacro -%}

{%- macro write_object(field, propertyPath, buffer, offset, assign) %}
    {{ "offset = " if assign -}} 
    BF.writeObject({{buffer}}, {{offset}}, {{field.type}}, {{propertyPath}})
{%- endmacro -%}

{%-macro serialize_by_field(field, pathParameter, inBuffer, inOffset, assign)-%}
    {%- set buffer = inBuffer | default("buffer", True) -%}
    {%- set offset = inOffset | default("offset", True) -%}
    {%- set assignOffset = assign | default(False, True) -%}
    {%- set path = pathParameter | default("value." + field.name, True) -%}
    {%- if 'Array' in field.type -%}
    {{ write_array(field, path)}}
    {%- elif "int8" in field.type -%}
    {{ write_int8(field, path, buffer, offset, assignOffset) }}
    {%- elif "int16" in field.type -%}
    {{ write_int16(field, path, buffer, offset, assignOffset) }}
    {%- elif "int32" in field.type -%}
    {{ write_int32(field, path, buffer, offset, assignOffset) }}
    {%- elif "int64" in field.type -%}
    {{ write_int64(field, path, buffer, offset, assignOffset) }}
    {%- elif field.type == "string" -%}
    {{- write_string(field, path, buffer, offset, assignOffset) }}
    {%- elif field.type == "boolean" -%}
    {{ write_boolean(field, path, buffer, offset, assignOffset) }}
    {%- elif field.type == "varint" -%}
    {{ write_varint(field, path, buffer, offset, assignOffset) }}
    {%- elif field.type == "buffer" -%}
    {{- write_buffer(field, path, buffer, offset, assignOffset) }}
    {%- else -%}
    {{ write_object(field, path, buffer, offset, assignOffset) }}
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

{%- macro read_array(type, buffer, offset, func) -%}
    {# Remove ">" and "Array<" from type, the result is the array type #}
    {%-set subtype = type | replace(">","")|replace("Array<", "") -%}
    (() => {
        const [array, newOffset] = BF.readArray({{buffer}}, {{offset}}, 
        {{- deserialize_by_type(subtype, "auxBuffer", "auxOffset", True) -}})
        offset = newOffset
        return array;
    })()
{%- endmacro %}
 
{%- macro deserialize_by_type(type, inBuffer, inOffset, funcStyle) -%}
    {%- set buffer = inBuffer | default("buffer", True) -%}
    {%- set offset = inOffset | default("offset", True) -%}
    {%- set func = funcStyle | default(False, True) -%}
    {%- if 'Array' in type %}
        {{ read_array(type, buffer, offset, func)}}
    {%- elif "int8" in type %}
        {{ read_int8(buffer, offset, func, type) }}
    {%- elif "int16" in type %}
        {{ read_int16(buffer, offset, func, type) }}
    {%- elif "int32" in type %}
        {{ read_int32(buffer, offset, func, type) }}
    {%- elif "int64" in type %}
        {{ read_int64(buffer, offset, func, type) }}
    {%- elif type == "string" %}
        {{ read_string(buffer, offset, func) }}
    {%- elif type == "buffer" %}
        {{ read_buffer(buffer, offset, func) }}
    {%- elif type == "boolean" %}
        {{ read_boolean(buffer, offset, func) }}
    {%- elif type == "varint" %}
        {{ read_varint(buffer, offset, func) }}
    {%- else %}
        {{ read_object(type, buffer, offset, func) }}
    {%- endif %}
{%- endmacro %}

{%- macro convert_type(type) -%}
    {%- if type == "Array<varint>" -%}
    Array<bigint>
    {%- elif type == "varint" -%}
    bigint
    {%- elif type == "Array<int64>" -%}
    Array<bigint>
    {%- elif type == "int64" -%}
    bigint
    {%- elif type == "Array<uint64>" -%}
    Array<bigint>
    {%- elif type == "uint64" -%}
    bigint
    {%- elif 'Array<int' in type -%}
    Array<number>
    {%- elif 'Array<uint' in type -%}
    Array<number>
    {%- elif 'int' in type -%}
    number
    {%- elif type == "buffer" -%}
    Buffer
    {%- elif type == "Array<buffer>" -%}
    Array<Buffer>
    {%- else -%}
    {{type}}
    {%- endif -%}
{%- endmacro %}
"""

template = """
// Code generated by v/tools/ts-generator/rpcgen_js.py
// import Buffer Functions
import BF from "./functions";
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
    * @return a tuple, where first element is a {{class.className}} and second
    *        one is the read last position in the buffer
    */
    static fromBytes(buffer: Buffer, offset = 0): [{{class.className}}, number]{
        return [{
            {%- for field in class.fields %}
                {{field.name}}: {{ deserialize_by_type(field.type) }},
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
        buffer: Buffer, offset = 0
    ): number {
        {%- for field in class.fields %}
            {{- serialize_by_field(field, "", "", "", True) -}}
        {%- endfor %}
        return offset
    }    
}
{%- endfor -%}
"""


def read_file(name):
    with open(name, 'r') as f:
        try:
            return json.load(f)
        except:
            logger.error(
                "Error: try to read input file, but there is "
                "a problem with json format ", name)


def create_class(json):
    tpl = Template(serializableFunctions + deserializableFunctions + template)
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
