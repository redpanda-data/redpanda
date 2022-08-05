# kafka-request-generator

The kafka-request-generator is a small little go program that generates 
random kafka requests (or responses) and writes their serialized representation to stdout. It depends on 
[franz-go](https://github.com/twmb/franz-go) to do this.


``` sh
➜  workspace/scratch ./kafka-request-generator -h
Usage of ./kafka-request-generator:
  -api int
        API key of request to generate
  -binary-output
        Print result as binary (default true)
  -is-request
        Generate kafka request if true, response if false (default true)
  -version int
        API version of generated request

```

Pass the kafka api request key and the api version of a request you want to 
generate. If its actually a response pass `-is-request=false` to the program. 
It will write binary data to stdout, so if your using this for manual testing 
and would like to see ASCII data on stdout instead for whatever reason, pass 
-binary-output=false to the program to get a hex formatted represetation 
instead.

### Generated requests

The requests generated are requests with fields populated with random data 
based on the type of a given field. For example if a field representing an 
enum is defined as type `int8` in the request schema but there are only 3 
valid values for said enum, all valid values for an `int8` could be possible 
results the generator would populate the field with.

The only assumptions that can be made about the contents of a given field are 
the binary data fields that are part of produce and fetch requests that 
represent record batches. Properly formatted serialized 
`kafka::record_batch`es (or legacy batches for older versions) are written to 
those fields.

### Example

``` sh
➜  workspace/scratch ./kafka-request-generator -api 16 -version 9 -is-request=false -binary-output=false
241a39ed1147021651644c4b73774a50466f5458755648794b4364505a0d6f685269696b4a4b7
75a4c4e206669565267684c6b75747a456e51734c456a4e4f787a7a5852586b4d4f704a000164
10d4ddb038cef005c2fb1c8a1731966e45%
```

## Current usage

Currently this mainprog is invoked within a particular C++ unit test (located 
in src/v/kafka/protocol/tests/protocol_test.cc) for ensuring that redpanda is 
binary compatible with all currently supported api/versions of the kafka 
protocol. Note that this only means that redpanda can correctly serialize / 
deserialize a request to spec, it does not imply that a particular client at 
a version can or cannot expect some sort of behavior from the broker. 
