# { pkgs ? import <nixpkgs> {}, vendorHash ? "sha256-abcd" }:
{ pkgs ? import <nixpkgs> {}, vendorHash ? null }:
pkgs.buildGoModule {
  pname = "discord-to-simplex";
  version = "0.1.0";

  src = ./.;

  inherit vendorHash;

  # nativeBuildInputs = pkgs.lib.optional (!pkgs.stdenv.isDarwin) [ pkgs.golangci-lint ];
  nativeBuildInputs = with pkgs; [
    golangci-lint
    sqlcipher
    openssl
  ];

  buildInputs = with pkgs; [
    sqlcipher
    openssl
  ];

  checkPhase = ''
    runHook preCheck
    go test ./...
    runHook postCheck
  '';

  shellHook = ''
    unset GOFLAGS
  '';

  doCheck = true;

  meta = with pkgs.lib; {
    description = "Merge DM export from discord-chat-exporter into SimpleX DB";
    homepage = "https://github.com/ritiek/discord-to-simplex";
    license = licenses.mit;
    maintainers = with maintainers; [ ritiek ];
  };
}
