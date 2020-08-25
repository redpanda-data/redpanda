#Rpc typescript generator code
`rpcgen_js.py` is typescript code generator, it receives a file with classes 
definition, in order to create a serializable classes with Redpanda binary data
format. those classes are save in file, 

##Class definition
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

##Types Support
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
* Classes definitions, example `ClassExample`

##How can run generator?
```commandline
python rpcgen_js.py --entities-define-file path_definition_class 
                    --out-file path_destination
```