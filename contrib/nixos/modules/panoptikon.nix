# services.panoptikon: --root stateDir; gpu → package + devices + env.
{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.panoptikon;
  inherit
    (lib)
    mkEnableOption
    mkOption
    mkIf
    types
    literalExpression
    ;

  # Empty host is not treated as loopback (often means all interfaces).
  isLoopback = host: host == "localhost" || host == "::1" || lib.hasPrefix "127." host;

  root = cfg.stateDir;
  serverConfig = "${root}/config/server/default.toml";

  pkgsCuda = pkgs.config.cudaSupport or false;
  pkgsRocm = pkgs.config.rocmSupport or false;

  # Default package from gpu option / nixpkgs config only (no circular package ref).
  defaultPackage =
    if cfg.gpu == "cpu"
    then pkgs.panoptikon-cpu
    else if cfg.gpu == "cuda"
    then pkgs.panoptikon-cuda
    else if cfg.gpu == "rocm"
    then pkgs.panoptikon-rocm
    else pkgs.panoptikon;

  package = cfg.package;
  panoptikonBin = "${package}/bin/panoptikon";

  packageCuda = package.cudaSupport or false;
  packageRocm = package.rocmSupport or false;

  # Effective backend for env/devices: explicit gpu wins; else follow the
  # selected package's wrap flags (so package = pkgs.panoptikon-cuda with
  # gpu = null still gets CUDA devices/env). When both package flags are
  # false (panoptikon-cpu, or packages without passthru), treat as CPU —
  # do **not** fall through to nixpkgs config (that would attach CUDA
  # devices/env to a CPU wrap when cudaSupport is true in nixpkgs).
  # Config-following `pkgs.panoptikon` already embeds config into passthru.
  effectiveGpu =
    if cfg.gpu != null
    then cfg.gpu
    else if packageRocm && !packageCuda
    then "rocm"
    else if packageCuda && !packageRocm
    then "cuda"
    else "cpu";

  useRocm = effectiveGpu == "rocm";
  useCuda = effectiveGpu == "cuda";
  useGpu = effectiveGpu != "cpu";

  rocmRuntimePkgs = import ../../package/nix/rocm-packages.nix {inherit pkgs;};
in {
  options.services.panoptikon = {
    enable = mkEnableOption "Panoptikon multimodal media search server";

    package = mkOption {
      type = types.package;
      default = defaultPackage;
      defaultText = literalExpression ''
        if gpu == "cpu" then pkgs.panoptikon-cpu
        else if gpu == "cuda" then pkgs.panoptikon-cuda
        else if gpu == "rocm" then pkgs.panoptikon-rocm
        else pkgs.panoptikon
      '';
      description = ''
        Panoptikon package. Defaults from {option}`gpu`: forced
        panoptikon-cpu/-cuda/-rocm when set, otherwise pkgs.panoptikon
        (follows nixpkgs GPU config). Override for patches/version only
        when flags still match gpu.
      '';
    };

    gpu = mkOption {
      type = types.nullOr (
        types.enum [
          "cpu"
          "cuda"
          "rocm"
        ]
      );
      default = null;
      description = ''
        When set, force that GPU backend: default package becomes
        panoptikon-cpu / -cuda / -rocm (ignoring nixpkgs.config), and
        service env/devices/HIP follow that backend.

        When null (default), use pkgs.panoptikon, which follows
        nixpkgs.config.cudaSupport / rocmSupport (not both).
      '';
    };

    openFirewall = mkOption {
      type = types.bool;
      default = false;
      description = ''
        Open {option}`services.panoptikon.port` in the firewall.
        Prefer a reverse proxy with authentication for non-loopback access.
      '';
    };

    host = mkOption {
      type = types.str;
      default = "127.0.0.1";
      description = "Bind address (env PANOPTIKON_HOST).";
    };

    port = mkOption {
      type = types.port;
      default = 6342;
      description = "Gateway port (env PANOPTIKON_PORT).";
    };

    rocmOverrideGfx = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "10.3.0";
      description = ''
        Export HSA_OVERRIDE_GFX_VERSION when effective backend is rocm.
      '';
    };

    stateDir = mkOption {
      type = types.path;
      default = "/var/lib/panoptikon";
      description = ''
        Writable `--root` (not under /nix/store). Layout: config/, data/, runtime/.
      '';
    };

    user = mkOption {
      type = types.str;
      default = "panoptikon";
      description = "Service user.";
    };

    group = mkOption {
      type = types.str;
      default = "panoptikon";
      description = "Service group.";
    };

    libraryPaths = mkOption {
      type = types.listOf types.path;
      default = [];
      example = [
        "/mnt/media/photos"
        "/var/lib/immich/library"
      ];
      description = ''
        Extra media trees exposed read-only to the service (also add them in
        scan config). Paths are optional (`-` prefix): a missing mount does
        not prevent the unit from starting.
      '';
    };

    readWritePaths = mkOption {
      type = types.listOf types.path;
      default = [];
      description = "Extra read-write paths.";
    };

    extraEnvironment = mkOption {
      type = types.attrsOf types.str;
      default = {};
      example = {
        LOGLEVEL = "DEBUG";
        RUST_LOG = "info,panoptikon=debug";
      };
      description = "Extra service environment.";
    };

    autoSetup = mkOption {
      type = types.bool;
      default = true;
      description = ''
        preStart: `panoptikon setup --if-needed` (long TimeoutStartSec). First
        sync is multi-GB; rocm also HIP-probes torch after sync.
        PANOPTIKON_AUTO_SETUP still covers a later stale lockfile after start.
        See {option}`setupMustSucceed` for hard-fail vs soft-fail on setup errors.
      '';
    };

    setupMustSucceed = mkOption {
      type = types.bool;
      default = false;
      description = ''
        When {option}`autoSetup` is true and preStart setup fails: if true,
        fail the unit (no HTTP until setup works); if false (default), log a
        warning, set PANOPTIKON_SKIP_IMMEDIATE_AUTO_SETUP so the process does
        not immediately re-run multi-GB setup in ExecStart, and start the
        server so API/UI come up while inference is degraded. Use true for
        appliances that must not run without a venv.
      '';
    };

    extraArgs = mkOption {
      type = types.listOf types.str;
      default = [];
      description = "Extra CLI args after the standard flags.";
    };
  };

  config = mkIf cfg.enable {
    assertions = [
      {
        assertion = !(lib.hasPrefix "/nix/store" cfg.stateDir);
        message = "services.panoptikon.stateDir must not be under /nix/store (immutable).";
      }
      {
        # Both nixpkgs GPU flags only illegal when we would use config-following panoptikon.
        assertion = cfg.gpu != null || !(pkgsCuda && pkgsRocm);
        message = ''
          services.panoptikon: nixpkgs.config.cudaSupport and rocmSupport are both
          true, but services.panoptikon.gpu is unset. Set gpu = "cpu"|"cuda"|"rocm"
          to force a backend, or enable only one config flag.
        '';
      }
      {
        assertion = cfg.rocmOverrideGfx == null || useRocm;
        message = "services.panoptikon.rocmOverrideGfx is only meaningful when the effective backend is rocm.";
      }
      {
        # If user set both gpu and a custom package, flags must agree.
        assertion =
          cfg.gpu
          == null
          || (
            (cfg.gpu == "cpu" && !packageCuda && !packageRocm)
            || (cfg.gpu == "cuda" && packageCuda && !packageRocm)
            || (cfg.gpu == "rocm" && packageRocm && !packageCuda)
          );
        message = ''
          services.panoptikon: package CUDA/ROCm flags do not match gpu = ${toString cfg.gpu}.
          Use the default package for that gpu, or a package with matching passthru flags.
        '';
      }
      {
        # Package wrap must not advertise both backends.
        assertion = !(packageCuda && packageRocm);
        message = ''
          services.panoptikon: package has both cudaSupport and rocmSupport.
          Use panoptikon-cpu, -cuda, or -rocm (or default panoptikon).
        '';
      }
      {
        assertion = effectiveGpu != "rocm" || pkgs.stdenv.hostPlatform.isx86_64;
        message = ''
          services.panoptikon: ROCm is only supported on x86_64-linux.
          Set gpu = "cpu" or "cuda", or use a different package.
        '';
      }
    ];

    users.users.${cfg.user} = {
      isSystemUser = true;
      group = cfg.group;
      home = cfg.stateDir;
      createHome = false;
      description = "Panoptikon service user";
      extraGroups = lib.optionals useGpu [
        "render"
        "video"
      ];
    };
    users.groups.${cfg.group} = {};

    fonts.packages = [
      pkgs.dejavu_fonts
      pkgs.noto-fonts
    ];

    environment.systemPackages = lib.optionals useRocm rocmRuntimePkgs;

    networking.firewall.allowedTCPPorts = mkIf cfg.openFirewall [cfg.port];

    systemd.tmpfiles.rules = [
      "d ${cfg.stateDir} 0750 ${cfg.user} ${cfg.group} -"
    ];

    warnings =
      lib.optional (cfg.openFirewall && !isLoopback cfg.host) ''
        services.panoptikon.openFirewall is enabled while host is "${cfg.host}".
        Prefer a reverse proxy with authentication; not hardened for direct exposure.
      ''
      ++ lib.optional (!isLoopback cfg.host) ''
        services.panoptikon.host is "${cfg.host}" (not loopback). Ensure policies
        match that Host; seeded nixos.toml only allows localhost under allow_all.
      '';

    systemd.services.panoptikon = {
      description = "Panoptikon media search engine";
      after = ["network-online.target"];
      wants = ["network-online.target"];
      wantedBy = ["multi-user.target"];

      environment =
        {
          PANOPTIKON_HOST = cfg.host;
          PANOPTIKON_PORT = toString cfg.port;
          PANOPTIKON_ACCELERATOR = effectiveGpu;
          PANOPTIKON_AUTO_SETUP =
            if cfg.autoSetup
            then "true"
            else "false";
        }
        // lib.optionalAttrs (cfg.rocmOverrideGfx != null) {
          HSA_OVERRIDE_GFX_VERSION = cfg.rocmOverrideGfx;
        }
        // lib.optionalAttrs useRocm {
          ROCM_PATH = "${pkgs.rocmPackages.clr}";
          HIP_PATH = "${pkgs.rocmPackages.clr}";
        }
        // cfg.extraEnvironment;

      path =
        [
          package
          pkgs.coreutils
        ]
        ++ lib.optionals useRocm [
          pkgs.rocmPackages.rocminfo
          pkgs.rocmPackages.rocm-smi
        ];

      preStart = ''
        set -euo pipefail
        root=${lib.escapeShellArg root}
        mkdir -p "$root"/{config/server,config/inference,data,runtime}
        if [ ! -f "$root/config/server/default.toml" ]; then
          cp --no-preserve=mode,ownership \
            ${package}/share/panoptikon/nixos.toml \
            "$root/config/server/default.toml"
        fi
        if [ ! -f "$root/config/inference/example.toml" ]; then
          cp --no-preserve=mode,ownership \
            ${package}/share/panoptikon/inference-example.toml \
            "$root/config/inference/example.toml"
        fi
        ${lib.optionalString cfg.autoSetup (
          if cfg.setupMustSucceed
          then ''
            # Hard-fail: unit will not start until setup succeeds.
            : > "$root/runtime/prestart.env"
            ${panoptikonBin} \
              --root "$root" \
              --config ${lib.escapeShellArg serverConfig} \
              --disable-update-check \
              setup \
              --if-needed \
              --accelerator ${lib.escapeShellArg effectiveGpu}
          ''
          else ''
            # Soft-fail (default): multi-GB torch download must not leave the host
            # without HTTP/API. On failure, write an EnvironmentFile so ExecStart
            # skips a second immediate setup (unit would look active while blocked).
            # Inference stays degraded until setup is re-run successfully.
            : > "$root/runtime/prestart.env"
            if ! ${panoptikonBin} \
                --root "$root" \
                --config ${lib.escapeShellArg serverConfig} \
                --disable-update-check \
                setup \
                --if-needed \
                --accelerator ${lib.escapeShellArg effectiveGpu}
            then
              echo "warning: panoptikon setup failed; starting without a complete managed venv" >&2
              echo "warning: set services.panoptikon.setupMustSucceed = true to fail the unit instead" >&2
              echo "PANOPTIKON_SKIP_IMMEDIATE_AUTO_SETUP=1" > "$root/runtime/prestart.env"
            fi
          ''
        )}
      '';

      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;

        # Written by soft-fail preStart when setup fails (optional '-' if absent).
        EnvironmentFile = lib.mkIf cfg.autoSetup ["-${root}/runtime/prestart.env"];

        ExecStart = lib.escapeShellArgs (
          [
            panoptikonBin
            "--root"
            root
            "--config"
            serverConfig
            "--disable-update-check"
          ]
          ++ cfg.extraArgs
        );

        WorkingDirectory = root;
        Restart = "on-failure";
        RestartSec = "5s";
        TimeoutStartSec =
          if cfg.autoSetup
          then "2h"
          else "5min";

        NoNewPrivileges = true;
        PrivateTmp = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateDevices = !useGpu;
        ProtectKernelTunables = true;
        ProtectKernelModules = true;
        ProtectControlGroups = true;
        LockPersonality = true;
        RestrictSUIDSGID = true;
        RestrictRealtime = true;
        SystemCallArchitectures = "native";

        ReadWritePaths = [root] ++ cfg.readWritePaths;
        # Leading '-' makes the path optional so automount/removable media
        # absence does not fail the unit at start.
        ReadOnlyPaths = map (p: "-${p}") cfg.libraryPaths;

        BindReadOnlyPaths = lib.optionals useGpu ["-/run/opengl-driver"];

        DevicePolicy = "closed";
        DeviceAllow =
          lib.optionals useGpu [
            "char-drm"
            "char-fb"
          ]
          ++ lib.optionals useRocm ["char-kfd"]
          ++ lib.optionals useCuda [
            "char-nvidiactl"
            "char-nvidia-caps"
            "char-nvidia-frontend"
            "char-nvidia-uvm"
          ];
        SupplementaryGroups = lib.optionals useGpu [
          "render"
          "video"
        ];
      };
    };
  };
}
