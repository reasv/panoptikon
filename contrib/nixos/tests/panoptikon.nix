# Parameterized NixOS VM smokes for services.panoptikon.
# Flake injects the module via defaults.imports.
#
# Usage from flake:
#   pkgs.testers.runNixOSTest {
#     imports = [ (import ./panoptikon.nix { gpu = "rocm"; }) ];
#     defaults.imports = [ self.nixosModules.default ];
#   }
#
# Or import without args for default (gpu = null, CPU path).
{
  # null = follow nixpkgs config (test expects clean config → cpu)
  # "cpu" | "cuda" | "rocm" = force via services.panoptikon.gpu
  gpu ? null,
  name ? (
    if gpu == null
    then "panoptikon"
    else "panoptikon-gpu-${gpu}"
  ),
}: let
  effective =
    if gpu == null
    then "cpu"
    else gpu;
  expectRocm = effective == "rocm";
  expectCpu = effective == "cpu";
in {
  inherit name;
  meta.maintainers = [];

  nodes.machine = {
    pkgs,
    config,
    lib,
    ...
  }: {
    services.panoptikon =
      {
        enable = true;
        autoSetup = false;
        host = "127.0.0.1";
        port = 6342;
      }
      // lib.optionalAttrs (gpu != null) {inherit gpu;};
    # Package is on the unit PATH only; add it (and curl) for interactive CLI checks.
    environment.systemPackages = [
      pkgs.curl
      config.services.panoptikon.package
    ];
  };

  testScript = ''
    machine.wait_for_unit("panoptikon.service")
    machine.wait_for_open_port(6342)
    machine.succeed("test -f /var/lib/panoptikon/config/server/default.toml")
    machine.succeed("test -f /var/lib/panoptikon/config/inference/example.toml")

    env = machine.succeed("systemctl show panoptikon.service -p Environment --value")
    assert "PANOPTIKON_ACCELERATOR=${effective}" in env, env

    unit = machine.succeed("systemctl cat panoptikon.service")

    ${
      if expectCpu
      then ''
        assert "char-kfd" not in unit
        assert "char-nvidiactl" not in unit
        assert "ROCM_PATH=" not in env
        assert "HIP_PATH=" not in env
      ''
      else if expectRocm
      then ''
        assert "char-kfd" in unit
        assert "render" in unit
        assert "char-nvidiactl" not in unit
        assert "ROCM_PATH=" in env
        assert "HIP_PATH=" in env
        machine.succeed(
            "systemctl show panoptikon.service -p SupplementaryGroups --value | grep -q render"
        )
        machine.succeed(
            "test -e /run/current-system/sw/lib/libamdhip64.so "
            "|| test -e /run/current-system/sw/lib/libamdhip64.so.7"
        )
      ''
      else ''
        assert "char-nvidiactl" in unit
        assert "char-nvidia-uvm" in unit
        assert "char-kfd" not in unit
        assert "render" in unit
        assert "ROCM_PATH=" not in env
        assert "HIP_PATH=" not in env
        machine.succeed(
            "systemctl show panoptikon.service -p SupplementaryGroups --value | grep -q render"
        )
      ''
    }

    exe = machine.succeed(
        "systemctl cat panoptikon.service | sed -n 's|^ExecStart=\\([^ ]*\\).*|\\1|p' | head -1"
    ).strip()
    assert exe, "missing ExecStart binary"
    machine.succeed(f"test -x '{exe}'")

    ${
      if expectCpu
      then ''
        machine.fail(f"grep -q '/opt/rocm/lib' '{exe}'")
        machine.fail(f"grep -q opengl-driver '{exe}'")
        # Config-following or forced-cpu package: no GPU ACCELERATOR pin in wrap.
        machine.fail(f"grep -q PANOPTIKON_ACCELERATOR '{exe}'")
      ''
      else if expectRocm
      then ''
        machine.succeed(f"grep -q '/opt/rocm/lib' '{exe}'")
        machine.succeed(f"grep -q PANOPTIKON_ACCELERATOR '{exe}'")
        machine.succeed(f"grep -q rocm '{exe}'")
      ''
      else ''
        machine.succeed(f"grep -q PANOPTIKON_ACCELERATOR '{exe}'")
        machine.succeed(f"grep -q cuda '{exe}'")
        machine.succeed(f"grep -q opengl-driver '{exe}'")
        machine.fail(f"grep -q '/opt/rocm/lib' '{exe}'")
      ''
    }

    machine.succeed("systemctl is-active panoptikon.service")
    machine.wait_until_succeeds(
        "curl -fsS http://127.0.0.1:6342/api/client-config | grep -q capabilities",
        timeout=120,
    )

    # TODO: assert the live `panoptikon accelerator` report and its startup
    # journald log line once the accelerator CLI subcommand ships on master.
  '';
}
