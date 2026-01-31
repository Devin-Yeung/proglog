{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

{
  # https://devenv.sh/packages/
  packages = with pkgs; [
    protobuf
    protoc-gen-go
    protoc-gen-go-grpc
    gotestsum
    just
  ];

  # https://devenv.sh/languages/
  languages.nix.enable = true;

  # https://devenv.sh/basics/
  enterShell = ''
    protoc --version
  '';

  # https://devenv.sh/git-hooks/
  git-hooks = {
    hooks = {
      unit-tests = {
        enable = true;
        name = "go tests";
        entry = "gotestsum --format testname";
        files = "\\.go$";
        pass_filenames = false;
      };
      nixfmt.enable = true;
      yamlfmt = {
        enable = true;
        settings = {
          lint-only = false;
        };
      };
    };
    package = pkgs.prek;
  };

  # See full reference at https://devenv.sh/reference/options/
}
