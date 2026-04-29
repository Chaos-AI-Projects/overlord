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
    overlordVersion = "0.1.0+${self.shortRev or "dev"}";

    overlord = pythonPackages.buildPythonApplication {
      pname = "overlord";
      version = overlordVersion;
      src = ./.;
      pyproject = true;

      build-system = with pythonPackages; [ setuptools wheel ];
      dependencies = with pythonPackages; [ mcp ];

      preBuild = ''
        cat > overlord/_version.py <<PYEOF
VERSION = "${overlordVersion}"
PYEOF
      '';

      doCheck = false; # tests require a running daemon
    };

    gwsCli = gws-cli.packages.${system}.default;

    # --------------- runtime packages ---------------
    runtimePackages = [
      overlord
      gwsCli
      pkgsUnstable.claude-code
      (pkgs.python312.withPackages (ps: with ps; [
        pip
        pandas
      ]))
      pkgs.bash
      pkgs.coreutils
      pkgs.findutils
      pkgs.gnugrep
      pkgs.gnused
      pkgs.jq
      pkgs.git
      pkgs.curl
      pkgs.cacert        # TLS certs for curl
      pkgs.procps        # ps, top, etc.
      pkgs.gh            # GitHub CLI
      pkgs.nodejs        # Node.js LTS
      pkgs.which
      pkgs.less
      pkgs.diffutils       # diff, cmp
      pkgs.patch            # patch
      pkgs.gnumake          # make
      pkgs.gawk             # awk
      pkgs.gnutar           # tar
      pkgs.gzip             # gzip
      pkgs.zip              # zip archiving
      pkgs.unzip            # zip extraction
      pkgs.wget             # HTTP downloader
      pkgs.vim              # terminal text editor
      pkgs.tmux             # terminal multiplexer
      pkgs.iproute2         # ss, ip (network diagnostics)
      pkgs.lsof             # file diagnostics
    ];

    binPath = lib.makeBinPath runtimePackages;

    # --------------- entrypoint ---------------
    entrypoint = pkgs.writeShellScript "entrypoint.sh" ''
      set -euo pipefail

      export HOME=/home/overlord
      mkdir -p /home/overlord/.local/share /home/overlord/brain

      # Initialize the vault (scripts, CLAUDE.md) in /home/overlord/brain;
      # the database is created at DEFAULT_DB_PATH (~/.local/share/overlord/overlord.db)
      cd /home/overlord/brain
      overlord init

      # Start the scheduler daemon (uses same DEFAULT_DB_PATH)
      exec overlord daemon $*
    '';

    # --------------- container image ---------------
    container = pkgs.dockerTools.buildLayeredImage {
      name = "overlord";
      tag = "latest";
      maxLayers = 32;

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

        # NSS config so id(1) can resolve users
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
