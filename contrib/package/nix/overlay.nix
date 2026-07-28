# Package overlay. Flake injects monorepo src (+ optional uiSrc).
# Package version is read from panoptikon/Cargo.toml in src (no hardcoded default).
# uiSrc: monorepo ui/ path when present, else null → package uses ui-pin.json.
{
  src,
  uiSrc ? null,
}: final: prev: let
  callServer = args:
    final.callPackage ./panoptikon (
      {
        inherit src uiSrc;
      }
      // args
    );

  callDesktop = args:
    final.callPackage ./panoptikon-desktop (
      {
        inherit src;
      }
      // args
    );

  # Default: follows nixpkgs.config.cudaSupport / rocmSupport.
  panoptikon = callServer {};

  # Forced backends always ignore nixpkgs GPU config.
  panoptikon-cpu = callServer {
    cudaSupport = false;
    rocmSupport = false;
  };
  panoptikon-cuda = callServer {
    cudaSupport = true;
    rocmSupport = false;
  };
  panoptikon-rocm = callServer {
    cudaSupport = false;
    rocmSupport = true;
  };

  panoptikon-desktop = callDesktop {
    inherit panoptikon;
  };
  panoptikon-desktop-cpu = callDesktop {
    panoptikon = panoptikon-cpu;
  };
  panoptikon-desktop-cuda = callDesktop {
    panoptikon = panoptikon-cuda;
  };
  panoptikon-desktop-rocm = callDesktop {
    panoptikon = panoptikon-rocm;
  };
in {
  inherit
    panoptikon
    panoptikon-cpu
    panoptikon-cuda
    panoptikon-rocm
    panoptikon-desktop
    panoptikon-desktop-cpu
    panoptikon-desktop-cuda
    panoptikon-desktop-rocm
    ;
}
