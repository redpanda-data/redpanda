{ pkgs, ... }:

{
  starship.enable = true;

  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # https://devenv.sh/packages/
  packages = with pkgs; [
    gh
    vim
    git
    curl
    jq
    ninja
    cmake
    ccache
    libkrb5
    clang_15
    lld_15
    xxHash
    snappy
    zstd
    commonsCompress
    rapidjson
    pkgconfig
    boost175
    c-ares
    protobuf
    re2
    ragel
    fmt
    lz4
    crc32c
    gnutls
    croaring
    libyamlcpp
    cryptopp
    libxfs
    abseil-cpp
    libsystemtap
    usrsctp
    valgrind
    avro-cpp
  ];

  # https://devenv.sh/scripts/
  scripts.hello.exec = "echo hello from $GREET";

  enterShell = ''
    hello
    git --version
  '';

  # https://devenv.sh/languages/
  languages.nix.enable = true;
  languages.go.enable = true;
  languages.python.enable = true;
  languages.cplusplus.enable = true;

  # https://devenv.sh/pre-commit-hooks/
  # pre-commit.hooks.shellcheck.enable = true;

  # https://devenv.sh/processes/
  # processes.ping.exec = "ping example.com";

  # See full reference at https://devenv.sh/reference/options/
}
