{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

{
  # https://devenv.sh/packages/
  packages = with pkgs; [ protobuf ];

  # https://devenv.sh/languages/
  languages.nix.enable = true;

  # https://devenv.sh/basics/
  enterShell = ''
    protobuf --version
  '';

  # https://devenv.sh/git-hooks/
  git-hooks = {
    hooks = {
      nixfmt.enable = true;
    };
    package = pkgs.prek;
  };

  # See full reference at https://devenv.sh/reference/options/
}
