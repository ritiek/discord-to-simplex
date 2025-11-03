{
  description = "Merge DM export from discord-chat-exporter into SimpleX DB";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";
  };

  outputs = inputs@{ self, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } ({ lib, ... }: {
      systems = [
        "aarch64-linux"
        "x86_64-linux"
        "riscv64-linux"

        "x86_64-darwin"
        "aarch64-darwin"
      ];
      perSystem = { config, self', inputs', pkgs, system, ... }: {
        packages = {
          discord-to-simplex = (pkgs.callPackage ./default.nix { });
          default = config.packages.discord-to-simplex;
        };
        checks =
          let
            packages = lib.mapAttrs' (n: lib.nameValuePair "package-${n}") self'.packages;
            devShells = lib.mapAttrs' (n: lib.nameValuePair "devShell-${n}") self'.devShells;
          in
          {
            cross-build = self'.packages.discord-to-simplex.overrideAttrs (old: {
              nativeBuildInputs = old.nativeBuildInputs ++ [ pkgs.gox ];
              buildPhase = ''
                runHook preBuild
                HOME=$TMPDIR gox -verbose -osarch '!darwin/386' ./cmd/discord-to-simplex/
                runHook postBuild
              '';
              doCheck = false;
              installPhase = ''
                runHook preBuild
                touch $out
                runHook postBuild
              '';
            });
          } // packages // devShells;
      };
    });
}
