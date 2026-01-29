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
