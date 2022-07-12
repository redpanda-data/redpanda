# Redpanda Coding Style

This document contains an incomplete description of the coding style used for
redpanda. It provides rationale for the `.clang-format` file we use for automatic
formatting, and it also includes rules that are not automated.

## Declarators

Pointer (`*`) and reference (`&`, `&&`) declarators stick to the type:

```cpp
void foo(std::string&, std::optional<int*>&&);

auto& x = some_var;
auto* y = &some_var;
```

An exception is the array declarator, which appears after the variable name:

```cpp
int a[3] = {1, 2, 3};
```

Note that we should always prefer std::array over C-style array declarations.

## Files

Header files have the `.h` extension, source files use the `.cc` extension. Use
`#pragma once` instead of an include guard.

Test files go in the `tests` directory of the module they belong to. That is,
the tests for a feature named `foo` and located in `src/v/foo` will be located
in `src/v/foo/tests`.

## Whitespace

Use spaces only. Tabs are never used because they differently on each system.

An _indent_ is four spaces. A double indent is eight spaces, a half-indent is
two spaces. A continuation indent is two spaces.

## Naming

We follow the C++ and Boost naming conventions: class names, variables, and
functions are `words_separated_by_whitespace`.

Data members appear at the bottom of the class. Private data members are
prefixed by an underscore:

```c++
class my_class {
public:
    void foo() {
        _a_member = 3;
    }
private:
    int _a_member;
};
```

Think of the leading underscore as a shorthand for `this->`. The leading
underscore is also useful in the context of IDEs, to bring up the private
members of a class.

Template parameters and concepts use `CamelCase`

## Including header files

In any file, to include a header file (one in the `src/v` directory), use an
absolute path with `""` like this:

```c++
#include "utils/vint.h"
```

`v` header files are the first block of includes. Then come header files for 
`seastar`, and other libraries. The last block is comprised of standard
library headers:

```c++
#include "utils/vint.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <boost/algorithm/string/join.hpp>

#include <map>
#include <vector>
```

This helps find missing includes in our headers.

## Braced blocks

All nested scopes are braced, even when the language allows omitting the braces
(such as an if-statement), this makes patches simpler and is more consistent.
The opening brace is merged with the line that opens the scope (class
definition, function definition, if statement, etc.) and the body is indented.

```c++
void a_function() {
    if (some condition) {
        stmt;
    } else {
        stmt;
    }
}
```

An exception is namespaces -- the body is _not_ indented, to prevent files that are almost 100% whitespace left margin.

When making a change, if you need to insert an indentation level, you can temporarily break the rules by inserting a half-indent, so that the patch is easily reviewable:

```c++
void a_function() {
  while (something) {   // new line - half indent
    if (some condition) {
        stmt;
    } else {
        stmt;
    }
  }                      // new line
}
```

A follow-up patch can restore the indents without any functional changes.

## Function parameters

Avoid output parameters; use return values instead.  In/out parameters are tricky, but in some cases they are relatively standard, such as serialization/deserialization.

If a function accepts a lambda or an `std::function`, make it the last argument, so that it can be easily provided inline:

```c++ 
template <typename Func>
void function_accepting_a_lambda(int a, int b, Func func);

int f() {
    return function_accepting_a_lambda(2, 3, [] (int x, int y) {
        return x + y;
    });
}
```

## Complex return types

If a function returns a complicated return type, put its return type on a separate line, otherwise it becomes hard to see where the return type ends and where the function name begins:

```c++
template <typename T1, T2>
template <typename T3, T4>
std::vector<typename a_struct<T1, T2>::some_nested_class<T3, T4>>  // I'm the return type
a_struct<T1, T2>::a_function(T3 a, T4 b) {                         // And I'm the function name
    // ...
}
```

## Whitespace around operators

Whitespace around operators should match their precedence: high precedence = no spaces, low precedency = add spaces:

```c++
     return *a + *b;  // good
     return * a+* b;  // bad
```

Parentheses can be used to make the expression clearer, but spaces should still
be used for readability.

`if`, `while`, `return` (and `template`) are not function calls, so they get a space after the keyword.

## Long lines

If a line becomes excessively long (>80 characters?), or is just complicated,
break it into two or more lines.  The second (and succeeding lines) are
_continuation lines_, and have a half indent:

```c++
    if ((some_condition && some_other_condition)
      || (more complicated stuff here...)   // continuation line, double indent
      || (even more complicated stuff)) {   // another continuation line
        do_something();  // back to single indent
    }
```

Of course, long lines or complex conditions may indicate that refactoring is in order.

## Generic lambdas and types

Generic lambdas (`[] (auto param)`) are discouraged where the type is known. Generic
lambdas reduce the compiler's and other tools' ability to reason about the code.
In case the actual type of `param` doesn't match the programmers expectations,
the compiler will only detect an error in the lambda body, or perhaps
even lower down the stack if more generic functions are called. In the case of an
IDE, most of its functionality is disabled in a generic lambda, since it can't
assume anything about that parameter.

Of course, when there is a need to support multiple types, genericity is the correct
tool. Even then, type parameters should be constrained with concepts, in order to
catch type mismatches early rather than deep in the instantiation chain.

