{
  description = "Proglog: A distributed log";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    gomod2nix = {
      url = "github:nix-community/gomod2nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.follows = "flake-utils";
    };
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      gomod2nix,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ ];
        };

        # helper to build Go applications using gomod2nix
        buildGoApplication = gomod2nix.legacyPackages.${system}.buildGoApplication;

        proglog = buildGoApplication {
          pname = "proglog";
          version = "0.1.0";
          src = ./.;
          modules = ./gomod2nix.toml;
          subPackages = [ "cmd/server" ];
          go = pkgs.go_1_25;
          postInstall = ''
            mv $out/bin/server $out/bin/proglog
          '';
        };

        image = pkgs.dockerTools.buildLayeredImage {
          name = "proglog";
          tag = "latest";

          contents = [
            pkgs.grpc-health-probe
          ];

          config = {
            Cmd = [ ];
            WorkingDir = "/tmp";
          };
        };
      in
      {
        checks = {
          inherit
            proglog
            image
            ;
        };

        packages = {
          inherit
            proglog
            image
            ;
          default = proglog;
        };
      }
    );
}
