# HIP/HSA (+ deps) for pytorch.org multi-arch rocm7.2 wheels.
# Fat wheels vendor most math libs; host still needs the HIP runtime.
{pkgs}:
(with pkgs.rocmPackages; [
  clr
  rocm-runtime
  rocm-device-libs
  rocminfo
  rocm-smi
])
++ (with pkgs; [
  numactl
  zstd
])
