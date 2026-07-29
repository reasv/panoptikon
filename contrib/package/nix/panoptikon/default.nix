{
  lib,
  rustPlatform,
  fetchFromGitHub,
  pkg-config,
  openssl,
  makeWrapper,
  runCommand,
  callPackage,
  nixosTests ? {},
  # Flake passes monorepo `src` (+ optional version). Optional uiSrc overrides pin.
  src ? null,
  # Default: read panoptikon/Cargo.toml from src so package version tracks the repo.
  # Required when src is null (nixpkgs-style fetchFromGitHub).
  version ? null,
  # null → monorepo src/ui if present, else fetchFromGitHub via ui-pin.json.
  uiSrc ? null,
  config,
  cudaSupport ? config.cudaSupport or false,
  rocmSupport ? config.rocmSupport or false,
}:
assert lib.assertMsg (!(cudaSupport && rocmSupport)) ''
  panoptikon: cudaSupport and rocmSupport are mutually exclusive
  (set only one in nixpkgs.config, or use panoptikon-cpu / -cuda / -rocm).
''; let
  pname = "panoptikon";

  # Synced from the ui submodule by scripts/sync-nix-ui-pin.py
  uiPin = builtins.fromJSON (builtins.readFile ./ui-pin.json);

  uiFallback = fetchFromGitHub {
    owner = "reasv";
    repo = "panoptikon-ui";
    rev = uiPin.rev;
    hash = uiPin.hash;
  };

  cargoVersion = srcPath:
    (builtins.fromTOML (builtins.readFile (srcPath + "/panoptikon/Cargo.toml"))).package.version;

  finalVersion =
    if version != null
    then version
    else if src != null
    then cargoVersion src
    else throw "panoptikon: pass version= (or src= with panoptikon/Cargo.toml) so the package tracks the repo version";

  finalSrc =
    if src != null
    then src
    else
      fetchFromGitHub {
        owner = "reasv";
        repo = "panoptikon";
        rev = "v${finalVersion}";
        hash = lib.fakeHash;
        fetchSubmodules = true;
      };

  finalUiSrc =
    if uiSrc != null
    then uiSrc
    else if builtins.pathExists (finalSrc + "/ui/package.json")
    then finalSrc + "/ui"
    else uiFallback;

  ui = callPackage ./ui.nix {
    uiSrc = finalUiSrc;
    version = finalVersion;
  };

  wrap = callPackage ./wrap.nix {
    inherit cudaSupport rocmSupport;
  };
in
  rustPlatform.buildRustPackage (finalAttrs: {
    inherit pname;
    version = finalVersion;
    src = finalSrc;

    cargoLock.lockFile = finalSrc + "/Cargo.lock";

    cargoBuildFlags = [
      "-p"
      "panoptikon"
      "--features"
      "bundled,bundled-ui"
    ];
    doCheck = false;

    nativeBuildInputs = [
      pkg-config
      makeWrapper
    ];

    buildInputs = [
      openssl
    ];

    env = {
      LIBSQLITE3_FLAGS = "-DSQLITE_ENABLE_MATH_FUNCTIONS";
      PANOPTIKON_UI_BUNDLE = "${ui}";
    };

    postPatch = ''
      substituteInPlace Cargo.toml \
        --replace-fail ', "panoptikon-desktop/src-tauri"' ""
    '';

    postInstall = ''
      install -Dm644 config/server/nixos.toml \
        $out/share/panoptikon/nixos.toml
      install -Dm644 config/inference/example.toml \
        $out/share/panoptikon/inference-example.toml

      wrapProgram $out/bin/panoptikon \
        ${lib.escapeShellArgs wrap.wrapArgs}
    '';

    passthru = {
      inherit cudaSupport rocmSupport;
      inherit ui;
      tests =
        {
          cli =
            runCommand "panoptikon-test-cli"
            {
              nativeBuildInputs = [finalAttrs.finalPackage];
              meta.timeout = 60;
            }
            ''
              panoptikon --version | grep -F ${lib.escapeShellArg finalAttrs.version}
              panoptikon --help | grep -q "Panoptikon media indexing"
              panoptikon --help | grep -q -- "--root"
              panoptikon setup --help | grep -q accelerator
              panoptikon setup --help | grep -q if-needed
              # TODO: assert the live `panoptikon accelerator` report once the
              # accelerator CLI subcommand ships on master.
              touch $out
            '';

          install =
            runCommand "panoptikon-test-install"
            {
              meta.timeout = 60;
            }
            ''
              bin=${finalAttrs.finalPackage}/bin/panoptikon
              share=${finalAttrs.finalPackage}/share/panoptikon
              test -x "$bin"
              test -f "$share/nixos.toml"
              test -f "$share/inference-example.toml"
              grep -q 'data_folder' "$share/nixos.toml"
              grep -q UV_PYTHON "$bin"
              grep -q UV_PYTHON_DOWNLOADS "$bin"
              grep -q FONTCONFIG_FILE "$bin"
              ${
                if wrap.useGpu
                then ''grep -q opengl-driver "$bin"''
                else ''! grep -q opengl-driver "$bin"''
              }
              ${
                if rocmSupport
                then ''
                  grep -q '/opt/rocm/lib' "$bin"
                  grep -q current-system/sw/lib "$bin"
                  grep -q PANOPTIKON_ACCELERATOR "$bin"
                  grep -q rocm "$bin"
                ''
                else if cudaSupport
                then ''
                  grep -q PANOPTIKON_ACCELERATOR "$bin"
                  grep -q cuda "$bin"
                  grep -q opengl-driver "$bin"
                  ! grep -q '/opt/rocm/lib' "$bin"
                  ! grep -q 'libamdhip64' "$bin"
                ''
                else ''
                  ! grep -q '/opt/rocm/lib' "$bin"
                  ! grep -q PANOPTIKON_ACCELERATOR "$bin"
                  ! grep -q opengl-driver "$bin"
                ''
              }
              grep -q nodejs "$bin"
              grep -q ffmpeg "$bin"
              grep -q '/bin/uv' "$bin" || grep -q uv- "$bin"

              # TODO: run the live `panoptikon accelerator` report per backend
              # once the accelerator CLI subcommand ships on master.
              touch $out
            '';
        }
        // lib.optionalAttrs (nixosTests ? panoptikon) {
          nixos = nixosTests.panoptikon;
        };
    };

    meta = {
      description = "Local multimodal media search engine (Rust server + AI workers + web UI)";
      longDescription = ''
        Bundled server (features bundled + bundled-ui) with PATH wrap for
        node/ffmpeg/uv/python3.12/fc-match/chromium, UV_PYTHON for Nix CPython,
        and FONTCONFIG_FILE for labels. Always run with --root <writable-dir>.
        Default package follows nixpkgs.config.cudaSupport / rocmSupport (not both).
        Use panoptikon-cpu / -cuda / -rocm to force a backend regardless of config.
      '';
      homepage = "https://github.com/reasv/panoptikon";
      license = lib.licenses.agpl3Plus;
      mainProgram = "panoptikon";
      platforms = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      maintainers = [];
    };
  })
