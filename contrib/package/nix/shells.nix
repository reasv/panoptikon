# Dev shells for the monorepo (cpu / cuda / rocm).
{
  pkgs,
  lib,
}: let
  isLinux = pkgs.stdenv.isLinux;
  isX86_64 = pkgs.stdenv.hostPlatform.isx86_64;
  python = pkgs.python312;

  commonPackages = with pkgs;
    [
      rustc
      cargo
      rustfmt
      clippy
      pkg-config
      openssl
      nodejs_24
      uv
      git
      ffmpeg
      python
      fontconfig
    ]
    ++ lib.optionals isLinux [
      libGL
      libglvnd
      glib
      zlib
      zstd
      stdenv.cc.cc.lib
      libx11
      libxext
      libxrender
      libsm
      libice
      freetype
      chromium
      dejavu_fonts
      noto-fonts
      webkitgtk_4_1
      gtk3
      libsoup_3
      librsvg
      libayatana-appindicator
    ];

  mkPanoptikonShell = {
    name,
    accelerator,
    extraPackages ? [],
  }: let
    allPackages = commonPackages ++ extraPackages;
    libraryPath = lib.makeLibraryPath allPackages;
  in
    pkgs.mkShell {
      name = "panoptikon-${name}";
      packages = allPackages;
      shellHook = ''
        export PANOPTIKON_NIX_SHELL=${accelerator}
        export UV_PYTHON="${python}/bin/python3.12"
        export UV_PYTHON_DOWNLOADS=never
        export LD_LIBRARY_PATH="${libraryPath}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
        if [ -d /run/opengl-driver/lib ]; then
          export LD_LIBRARY_PATH="/run/opengl-driver/lib:$LD_LIBRARY_PATH"
        fi
        if [ -d /run/opengl-driver-32/lib ]; then
          export LD_LIBRARY_PATH="/run/opengl-driver-32/lib:$LD_LIBRARY_PATH"
        fi
        export XDG_DATA_DIRS="${pkgs.dejavu_fonts}/share:${pkgs.noto-fonts}/share''${XDG_DATA_DIRS:+:$XDG_DATA_DIRS}"

        if _cfg="$("$UV_PYTHON" scripts/generate-nix-dev-config.py)"; then
          export PANOPTIKON_CONFIG_PATH="''${_cfg}"
        else
          echo "warning: failed to generate config/server/nix-dev.toml" >&2
        fi
        unset _cfg

        echo "Panoptikon nix shell: ${accelerator}"
        echo "  rustc/cargo/node/uv/python/fc-match on PATH (format: nix fmt)"
        echo "  format nix:  nix fmt"
        echo "  UI pin sync: scripts/sync-nix-ui-pin.py  # no nix; importNpmLock for npm"
        echo "  PANOPTIKON_CONFIG_PATH=''${PANOPTIKON_CONFIG_PATH:-<unset>}"
        echo "  next: cargo build -p panoptikon && panoptikon setup --accelerator ${accelerator}"
      '';
    };

  cpuShell = mkPanoptikonShell {
    name = "cpu";
    accelerator = "cpu";
  };

  cudaShell =
    if isLinux
    then
      mkPanoptikonShell {
        name = "cuda";
        accelerator = "cuda";
        extraPackages = with pkgs.cudaPackages_12_8; [
          cudatoolkit
          cudnn
          cuda_nvcc
        ];
      }
    else null;

  rocmShell =
    if isLinux && isX86_64
    then
      mkPanoptikonShell {
        name = "rocm";
        accelerator = "rocm";
        extraPackages = import ./rocm-packages.nix {inherit pkgs;};
      }
    else null;
in
  {
    default = cpuShell;
    cpu = cpuShell;
  }
  // lib.optionalAttrs (cudaShell != null) {cuda = cudaShell;}
  // lib.optionalAttrs (rocmShell != null) {rocm = rocmShell;}
