{
  lib,
  buildGoModule,
  fetchFromGitHub,
  installShellFiles,
}:

let
  version = "0.0.0-dev";
  rev = "ae5f867664a0160428d904a6c9ee835d4f979bd1";
in
buildGoModule {
  pname = "redpanda-rpk";
  inherit version;

  src = fetchFromGitHub {
    owner = "redpanda-data";
    repo = "redpanda";
    inherit rev;
    hash = "sha256-lt0z25GBSq6aqiuE4Cedjpe3rxSa7wmve5tFv1gnzHo=";
  };

  modRoot = "src/go/rpk";
  vendorHash = "sha256-44doWJ3SB0FN0uYVgPEQRfaWhiC78d5+zQhx7K3La+k=";

  ldflags =
    let
      versionPkg = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version";
      containerPkg = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/containerutil";
    in
    [
      "-s" "-w"
      "-X ${versionPkg}.version=${version}"
      "-X ${versionPkg}.rev=${rev}"
      "-X ${containerPkg}.tag=v${version}"
    ];

  nativeBuildInputs = [ installShellFiles ];

  postInstall = ''
    for shell in bash fish zsh; do
      $out/bin/rpk generate shell-completion $shell > rpk.$shell
      installShellCompletion rpk.$shell
    done
  '';

  # Network-dependent tests cannot run in the sandbox
  doCheck = false;

  meta = {
    description = "Redpanda CLI (rpk)";
    homepage = "https://redpanda.com/";
    license = lib.licenses.bsl11;
    platforms = lib.platforms.linux;
    mainProgram = "rpk";
  };
}
