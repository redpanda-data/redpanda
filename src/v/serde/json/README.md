# Seastar friendly JSON parser

`serde/json` provides a Seastar-friendly streaming JSON parser. Low level
parsing routines (string, numeric) are ported from [RapidJSON] and redesigned
for incremental parsing. As a result, the parser can be used to parse JSON
documents incrementally, i.e. the input buffer can be split into multiple chunks
with arbitrary alignment/boundaries.

StAX (Streaming API for XML) style API is used to parse JSON documents.

```cpp
auto p = serde::json::parser(iobuf);

while (co_await p.next()) {
    switch (p.token()) {
    case serde::json::token::object_start:
        // handle object start
        break;
    // ... handle other token types
    }
}
```

## Known issues

- Numeric precision lower than RapidJSON. While this can be ignored as
  [RFC 8259] does not mandate precision for numbers (section 6), we are still
  below what C++ double type can represent.
- Not sufficiently tested. Notably, no fuzz testing is done yet. The parser
  should be fuzz tested with various JSON documents to ensure it does not
  trip/gets into infinite loops with malformed documents. The parser should also
  be fuzz tested with various alignment/boundaries.
- No UTF-8 validation. The parser does not validate UTF-8 sequences.

## Design

### Push-parsers (e.g. string, numeric sub-parsers)

Push-parsers are our solution for asynchronous incremental parsing.

**Pros:**

- Can be fed with arbitrarily aligned buffers.
- Do not need to perform preemption checks if the buffers are sized properly
  (i.e., not too large).
- Do not require `co_await` for every byte read (e.g., if parsing is implemented
  from `seastar::input_stream`).
- For unicode strings the string parser can do zero-copy parsing (i.e., no need
  to copy the string into a separate buffer but will share the
  `seastar::temporary_buffer`).

**Cons:**

- Increased complexity.

While parsing from a `seastar::input_stream` is not supported yet, it is nice to
have primitives which allow us to implement it in the future without rewriting
the parsers.

## References

- [RFC 8259] - The JavaScript Object Notation (JSON) Data Interchange Format
- [RapidJSON] - A fast JSON parser/generator for C++ with both SAX and DOM style
  API

[rapidjson]: https://github.com/Tencent/rapidjson
[rfc 8259]: https://datatracker.ietf.org/doc/html/rfc8259
