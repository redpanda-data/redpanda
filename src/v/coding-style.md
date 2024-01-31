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

## Documentation

The audience when writing comments should be for other members of your team, new members of the 
team getting onboarded, and an oncaller who is trying to re-familiarize themselves with some code.

Here are some generic tips for writing good technical comments:

* If writing something in-depth, start with a summary to prevent overloading someone new.
* Write as close to the source as possible, and try not to duplicate comments, but reference the source.
* If in doubt, write something concise and to the point. Don't add fluff for the sake of it.
* Think about the maintainablility of your comments.

For the curious, here are some useful articles on this subject:

* [Coding Horror - Code tells you how, comments tell you why](https://blog.codinghorror.com/code-tells-you-how-comments-tell-you-why/)
* [Stack Overflow - Best Practices for Writing Code Comments](https://stackoverflow.blog/2021/12/23/best-practices-for-writing-code-comments/)

### Modules

Modules themselves should have a `README.md` file explaining the purpose of the library,
its API surface, and an architectural overview of that library (mentioning major classes),
etc. This will aid people new to the library in understanding the lay of the land. When in doubt
of if information should be placed in a class or in a README, prefer moving the documentation
as close to the source as possible (inside the class documentation). We want to prioritize
documentation not becoming stale - and also not cognitively overload the reader of the library 
README.

### Classes

All classes and structs should be documented. 

Writing points when documenting a given class:
* Lifecycle of the class
* Place within the larger library
* How to use it correctly (concurrency, proper setup, example usage, etc).
* Broad strokes of implmentation details
* Which cores it exists on (if a sharded service)
* If it is a cluster/node/shard view of information

For a struct, it is useful to document how/where it is used, as well as the meaning of it's members.

### Functions

Non trivial free and class functions should be documented. Trivial is ultimately
a judgement call, but a rule of thumb is that getter methods (like `iobuf value() const { return _value; }`)
or other common lifecycle methods like `start` and `stop` could be considered trivial. It's highly
recommended to error on the side of having something rather than nothing when documenting functions however.

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
