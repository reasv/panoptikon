{
  description = "Panoptikon: local multimodal media search (package, NixOS module, dev shells)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
  }: let
    # Server wrap and desktop depend on Linux libraries (WebKit, X11, chromium).
    systems = [
      "x86_64-linux"
      "aarch64-linux"
    ];

    # Filter by path relative to the flake root (not bare basename), so a nested
    # `data/` or `target/` under e.g. ui/ is not stripped by accident.
    panoptikonSrc = let
      root = toString self;
      relOf = path: nixpkgs.lib.removePrefix (root + "/") (toString path);
      # Drop whole trees under these root-relative prefixes.
      rootPrefixes = [
        ".git"
        "target"
        "data"
        "runtime"
        "python-legacy"
        "python/.venv"
        "ui/node_modules"
        "ui/.next"
        "panoptikon-desktop/src-tauri/target"
      ];
      hasRootPrefix = rel:
        builtins.any (
          p:
            rel
            == p
            || nixpkgs.lib.hasPrefix (p + "/") rel
        )
        rootPrefixes;
      # Drop any nested component with these names.
      dropComponents = ["node_modules" ".venv" ".next"];
      hasDropComponent = rel: let
        parts = nixpkgs.lib.splitString "/" rel;
      in
        builtins.any (c: builtins.elem c parts) dropComponents;
    in
      nixpkgs.lib.cleanSourceWith {
        src = self;
        filter = path: type: let
          rel = relOf path;
          base = baseNameOf path;
        in
          rel
          != ""
          && !hasRootPrefix rel
          && !hasDropComponent rel
          && !(nixpkgs.lib.hasPrefix "result" base);
      };

    # Prefer monorepo ui/ when the flake source has it. Otherwise the package
    # fetches panoptikon-ui from contrib/package/nix/panoptikon/ui-pin.json
    # (flake input URLs cannot be computed from that file — Nix requires a
    # plain string — so the pin lives in the package, not inputs.ui).
    uiSrc =
      if builtins.pathExists (panoptikonSrc + "/ui/package.json")
      then panoptikonSrc + "/ui"
      else null;

    # Version comes from panoptikon/Cargo.toml inside src (see package default.nix).
    packageOverlay = import ./contrib/package/nix/overlay.nix {
      src = panoptikonSrc;
      inherit uiSrc;
    };
  in
    {
      overlays.default = packageOverlay;
      nixosModules.default = import ./contrib/nixos/modules/panoptikon.nix;
      nixosModules.panoptikon = self.nixosModules.default;
    }
    // flake-utils.lib.eachSystem systems (
      system: let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
          overlays = [packageOverlay];
        };

        lib = pkgs.lib;
        isLinux = pkgs.stdenv.isLinux;
        # ROCm stack (and module gpu=rocm) is x86_64-only; match shells.nix.
        isX86_64 = pkgs.stdenv.hostPlatform.isx86_64;

        mkNixosTest = args:
          pkgs.testers.runNixOSTest {
            imports = [(import ./contrib/nixos/tests/panoptikon.nix args)];
            defaults.imports = [self.nixosModules.default];
          };
      in {
        packages =
          {
            default = pkgs.panoptikon;
            inherit
              (pkgs)
              panoptikon
              panoptikon-cpu
              panoptikon-cuda
              ;
          }
          // lib.optionalAttrs isX86_64 {
            inherit (pkgs) panoptikon-rocm;
          }
          // lib.optionalAttrs isLinux {
            inherit
              (pkgs)
              panoptikon-desktop
              panoptikon-desktop-cpu
              panoptikon-desktop-cuda
              ;
          }
          // lib.optionalAttrs (isLinux && isX86_64) {
            inherit (pkgs) panoptikon-desktop-rocm;
          };

        checks =
          {
            panoptikon = pkgs.panoptikon;
            panoptikon-cli = pkgs.panoptikon.passthru.tests.cli;
            panoptikon-install = pkgs.panoptikon.passthru.tests.install;
            panoptikon-cpu-install = pkgs.panoptikon-cpu.passthru.tests.install;
            panoptikon-cuda-install = pkgs.panoptikon-cuda.passthru.tests.install;
            # Flake formatter (alejandra) must accept all tracked *.nix files.
            alejandra =
              pkgs.runCommand "alejandra-check"
              {
                nativeBuildInputs = [pkgs.alejandra];
                src = panoptikonSrc;
              }
              ''
                cp -a "$src" ./src
                chmod -R u+w ./src
                cd ./src
                alejandra --check .
                touch $out
              '';
          }
          // lib.optionalAttrs isX86_64 {
            panoptikon-rocm-install = pkgs.panoptikon-rocm.passthru.tests.install;
          }
          // lib.optionalAttrs isLinux {
            panoptikon-desktop = pkgs.panoptikon-desktop;
            panoptikon-desktop-install = pkgs.panoptikon-desktop.passthru.tests.install;
            panoptikon-desktop-cpu-install = pkgs.panoptikon-desktop-cpu.passthru.tests.install;
            panoptikon-desktop-cuda-install = pkgs.panoptikon-desktop-cuda.passthru.tests.install;

            panoptikon-nixos = mkNixosTest {};
            panoptikon-nixos-gpu-cpu = mkNixosTest {gpu = "cpu";};
            panoptikon-nixos-gpu-cuda = mkNixosTest {gpu = "cuda";};
          }
          // lib.optionalAttrs (isLinux && isX86_64) {
            panoptikon-desktop-rocm-install = pkgs.panoptikon-desktop-rocm.passthru.tests.install;
            panoptikon-nixos-gpu-rocm = mkNixosTest {gpu = "rocm";};
          };

        devShells = import ./contrib/package/nix/shells.nix {
          inherit pkgs lib;
        };

        formatter = pkgs.alejandra;
      }
    );
}
