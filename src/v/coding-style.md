# Redpanda Coding Style

The document covers style and convention not enforced by clang-format.

## Naming and source tree layout

Header files have the `.h` extension, source files use the `.cc` extension.

Public header files (meaning other libraries can `#include` them directly), should go
in an `includes` directory within the library they belong to. For example, a library named
`foo` should have public header files within `src/v/foo/include/foo`.

Test files go in the `tests` directory of the library they belong to. That is,
the tests for a feature named `foo` and located in `src/v/foo` will be located
in `src/v/foo/tests`.

Use `snake_case` naming convention by default for files and language identifiers.

## Includes

In any file, to include a header file (one in the `src/v` directory), use an
absolute path with `""` like this:

```c++
#include "utils/vint.h"
```

Header files included from dependencies should use system path convention:

```c++
#include <boost/thing.h>
```

Use `#pragma once` instead of an include guard.

## Access Specifiers

### Ordering

```c++
class type {
public:
protected:
private:
};
```

### Duplicate sections

It's okay to use multiple instances of access specifiers to create logical groupings where needed.

```c++
class type {
public:
   // boiler plate

public:
  // logical grouping of APIs

...
};
```
