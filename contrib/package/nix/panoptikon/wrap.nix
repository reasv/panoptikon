# Shared makeWrapper args for the panoptikon server binary.
# GPU host paths and PANOPTIKON_ACCELERATOR only when package flags are set.
{
  lib,
  stdenv,
  nodejs_24,
  ffmpeg,
  uv,
  python312,
  fontconfig,
  chromium,
  makeFontsConf,
  dejavu_fonts,
  openssl,
  libGL,
  libglvnd,
  glib,
  zlib,
  zstd,
  libx11,
  libxext,
  libxrender,
  libsm,
  libice,
  freetype,
  cudaSupport ? false,
  rocmSupport ? false,
}:
assert lib.assertMsg (!(cudaSupport && rocmSupport)) ''
  panoptikon wrap: cudaSupport and rocmSupport are mutually exclusive
''; let
  useGpu = cudaSupport || rocmSupport;

  pythonRuntimeLibs = [
    stdenv.cc.cc.lib
    zlib
    zstd
    openssl
    libGL
    libglvnd
    glib
    libx11
    libxext
    libxrender
    libsm
    libice
    fontconfig
    freetype
  ];

  fontsConf = makeFontsConf {
    fontDirectories = [dejavu_fonts];
  };

  runtimePath = lib.makeBinPath [
    nodejs_24
    ffmpeg
    uv
    python312
    fontconfig.bin
    chromium
  ];

  runtimeLibPath = lib.makeLibraryPath pythonRuntimeLibs;

  gpuLdScript =
    lib.optionalString useGpu ''
      if [ -d /run/opengl-driver/lib ]; then
        export LD_LIBRARY_PATH="/run/opengl-driver/lib''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
      fi
    ''
    + lib.optionalString rocmSupport ''
      if [ -d /opt/rocm/lib ]; then
        export LD_LIBRARY_PATH="/opt/rocm/lib''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
      fi
      if [ -e /run/current-system/sw/lib/libamdhip64.so ] || [ -e /run/current-system/sw/lib/libamdhip64.so.7 ]; then
        export LD_LIBRARY_PATH="/run/current-system/sw/lib''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
      fi
    '';

  # Forced GPU packages pin setup accelerator; CPU leaves it unset (service/config may set).
  gpuEnvArgs =
    lib.optionals rocmSupport [
      "--set"
      "PANOPTIKON_ACCELERATOR"
      "rocm"
    ]
    ++ lib.optionals cudaSupport [
      "--set"
      "PANOPTIKON_ACCELERATOR"
      "cuda"
    ]
    ++ lib.optionals useGpu [
      "--run"
      gpuLdScript
    ];
in {
  inherit
    runtimePath
    runtimeLibPath
    fontsConf
    gpuEnvArgs
    cudaSupport
    rocmSupport
    useGpu
    ;

  # Flattened for: wrapProgram $bin ${lib.escapeShellArgs wrapArgs}
  wrapArgs =
    [
      "--prefix"
      "PATH"
      ":"
      runtimePath
      "--prefix"
      "LD_LIBRARY_PATH"
      ":"
      runtimeLibPath
      "--set"
      "FONTCONFIG_FILE"
      fontsConf
      "--set"
      "UV_PYTHON"
      "${python312}/bin/python3.12"
      "--set"
      "UV_PYTHON_DOWNLOADS"
      "never"
    ]
    ++ gpuEnvArgs;
}
