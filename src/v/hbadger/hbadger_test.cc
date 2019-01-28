#include <iostream>

#include "hbadger.h"

void
foo() {
  HBADGER(module_filesystem, func_foo);
  // HBADGER for this test will *always* throw
  std::cout << "WILL NEVER BE CALLED" << std::endl;
  std::exit(1);
}

int
main(int argc, char **argv) {
  try {
    foo();
  } catch (const std::runtime_error &e) {
    std::cout << e.what() << std::endl;
    const std::string expected =
      "honey_badger_failure{Code: 66, Category: module_filesystem, Details: "
      "func_foo: Generated Failure From Static Function}";
    const std::string got = e.what();
    if (expected == got) { return 0; }
    std::cerr << "Expected != Got. Expected: `" << expected << "` and Got: `"
              << got << "` missmatch." << std::endl;
    return 1;
  }
  std::cerr << "The demo honey badger test should always throw an exception"
            << std::endl;
  return 1;
}
