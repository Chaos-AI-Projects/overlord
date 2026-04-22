{
  description = "Overlord — container & package";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    nixpkgs-unstable.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    gws-cli.url = "github:googleworkspace/cli/v0.22.5";
  };

  outputs = { self, nixpkgs, nixpkgs-unstable, gws-cli }:
  let
    system = "x86_64-linux";
    pkgs = nixpkgs.legacyPackages.${system};
    pkgsUnstable = import nixpkgs-unstable {
      inherit system;
      config.allowUnfree = true;
    };
    lib = pkgs.lib;

    pythonPackages = pkgs.python312Packages;

    # --------------- overlord Python package ---------------
    overlord = pythonPackages.buildPythonApplication {
      pname = "overlord";
      version = "0.1.0";
      src = ./.;
      pyproject = true;

      build-system = with pythonPackages; [ setuptools wheel ];
      dependencies = with pythonPackages; [ mcp ];

      doCheck = false; # tests require a running daemon
    };

    gwsCli = gws-cli.packages.${system}.default;

    # --------------- runtime packages ---------------
    runtimePackages = [
      overlord
      gwsCli
      pkgsUnstable.claude-code
      pkgs.bash
      pkgs.coreutils
      pkgs.findutils
      pkgs.gnugrep
      pkgs.gnused
      pkgs.gosu          # drop-root in containers (replaces su)
      pkgs.jq
      pkgs.git
      pkgs.curl
      pkgs.cacert        # TLS certs for curl
      pkgs.procps        # ps, top, etc.
    ];

    binPath = lib.makeBinPath runtimePackages;

    # --------------- entrypoint ---------------
    entrypoint = pkgs.writeShellScript "entrypoint.sh" ''
      set -euo pipefail

      OVERLORD_UID=1000
      OVERLORD_GID=1000

      # Detect the UID/GID of the mounted /home/overlord volume and remap the
      # overlord user/group so files have correct ownership on the host.
      VOLUME_UID=$(stat -c '%u' /home/overlord)
      VOLUME_GID=$(stat -c '%g' /home/overlord)
      if [ "$VOLUME_UID" != "0" ] && [ "$VOLUME_UID" != "$OVERLORD_UID" ]; then
          sed -i "s/^overlord:x:''${OVERLORD_UID}:/overlord:x:''${VOLUME_UID}:/" /etc/passwd
          OVERLORD_UID="$VOLUME_UID"
      fi
      if [ "$VOLUME_GID" != "0" ] && [ "$VOLUME_GID" != "$OVERLORD_GID" ]; then
          sed -i "s/^overlord:x:''${OVERLORD_GID}:/overlord:x:''${VOLUME_GID}:/" /etc/group
          sed -i "s/^\(overlord:x:''${OVERLORD_UID}\):''${OVERLORD_GID}:/\1:''${VOLUME_GID}:/" /etc/passwd
          OVERLORD_GID="$VOLUME_GID"
      fi

      # Ensure overlord user owns their home directory and required subdirs
      chown "''${OVERLORD_UID}:''${OVERLORD_GID}" /home/overlord
      mkdir -p /home/overlord/.local/share /home/overlord/brain
      chown -R "''${OVERLORD_UID}:''${OVERLORD_GID}" /home/overlord/.local /home/overlord/brain

      export HOME=/home/overlord

      # Initialize the vault (scripts, CLAUDE.md) in /home/overlord/brain;
      # the database is created at DEFAULT_DB_PATH (~/.local/share/overlord/overlord.db)
      cd /home/overlord/brain
      gosu overlord overlord init

      # Start the scheduler daemon as overlord (uses same DEFAULT_DB_PATH)
      exec gosu overlord overlord daemon $*
    '';

    # --------------- container image ---------------
    container = pkgs.dockerTools.buildLayeredImage {
      name = "overlord";
      tag = "latest";

      contents = runtimePackages;

      fakeRootCommands = ''
        # User & group database — replaces useradd from shadow-utils.
        mkdir -p ./etc
        cat > ./etc/passwd <<'PASSWD'
root:x:0:0:root:/root:${pkgs.bash}/bin/bash
overlord:x:1000:1000::/home/overlord:${pkgs.bash}/bin/bash
PASSWD
        cat > ./etc/group <<'GROUP'
root:x:0:
overlord:x:1000:
GROUP
        cat > ./etc/shadow <<'SHADOW'
root:!:1::::::
overlord:!:1::::::
SHADOW

        # NSS config so gosu / id can resolve users
        cat > ./etc/nsswitch.conf <<'NSS'
passwd: files
group:  files
shadow: files
NSS

        # Provide /usr/bin/env so #!/usr/bin/env shebangs work
        mkdir -p ./usr/bin
        ln -s ${pkgs.coreutils}/bin/env ./usr/bin/env

        mkdir -p ./home/overlord
        chown 1000:1000 ./home/overlord
        mkdir -p ./tmp
        chmod 1777 ./tmp

      '';
      enableFakechroot = true;

      config = {
        Entrypoint = [ "${entrypoint}" ];
        ExposedPorts."8000/tcp" = {};
        Volumes."/home/overlord" = {};
        Env = [
          "PATH=${binPath}:/usr/bin:/bin"
          "HOME=/home/overlord"
          "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
          "NIX_SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
        ];
        WorkingDir = "/home/overlord";
      };
    };

  in {
    packages.${system} = {
      inherit overlord container;
      default = overlord;
    };
  };
}
