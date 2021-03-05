# Typescript v/RPC code generator

`rpcgen_js.py` is typescript code generator. It takes as input, a structured json
file with a class per entry, in order to create the typescript classes (containing
the correct custom serializers and deserializers) for use with Redpanda's binary
data format.

## Class definition
```json
{
  "classes": [
    {
      "className": "ClassExample",
      "fields": [
        {
          "name": "attributeName",
          "type": "int32"
        } 
      ]
    }
  ] 
}
```

## Types Supported
* int8
* int16
* int32
* uint8
* uint16
* uint32
* string
* varint
* Array\<**T**>
* boolean
* Buffer
* Any custom class

## How can I run the generator?

```commandline
python rpcgen_js.py --entities-define-file path_definition_class 
                    --out-file path_destination
```
