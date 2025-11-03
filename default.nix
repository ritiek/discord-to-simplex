{ pkgs ? import <nixpkgs> {}, vendorHash ? "sha256-vrPw9C/Tm4MeEva8u1HtuOrMNAOnBOnvPJaQvdeEaJ0=" }:
pkgs.buildGoModule {
  pname = "discord-to-simplex";
  version = "0.1.0";

  src = ./.;

  inherit vendorHash;

  nativeBuildInputs = with pkgs; [
    pkg-config
  ];

  buildInputs = with pkgs; [
    sqlcipher
    openssl
    ffmpeg
  ];

  env.CGO_ENABLED = "1";

  meta = with pkgs.lib; {
    description = "Merge DM export from discord-chat-exporter into SimpleX DB";
    homepage = "https://github.com/ritiek/discord-to-simplex";
    license = licenses.mit;
    maintainers = with maintainers; [ ritiek ];
  };
}
