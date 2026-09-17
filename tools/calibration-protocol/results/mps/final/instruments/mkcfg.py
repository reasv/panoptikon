import sys, pathlib
R = "/Users/moot/projects/panoptikon-pr27"
src = pathlib.Path(R + "/.mac/server-C1-mac.toml").read_text()
FIX_IMPL = f'"{R}/python/tests/inferio_worker/fixture_impls"'
FIX_CFG = f'"{R}/tools/calibration-protocol/fixtures/registry"'
for accel in ("cpu", "auto"):
    t = src
    old_impl = f'impl_dirs   = ["{R}/python/inferio/impl", "{R}/inferio_custom"]'
    assert old_impl in t, "impl_dirs line not found"
    t = t.replace(old_impl, old_impl[:-1] + f", {FIX_IMPL}]")
    old_cfg = f'config_dirs = ["{R}/python/inferio/config", "{R}/config/inference"]'
    assert old_cfg in t, "config_dirs line not found"
    t = t.replace(old_cfg, old_cfg[:-1] + f", {FIX_CFG}]")
    marker = "[inference_local.python_env]\n"
    assert marker in t
    t = t.replace(marker, marker + f'accelerator = "{accel}"   # CALIB A1: the CPU-priced Mac leg\n', 1)
    out = pathlib.Path(f"{R}/.mac/final/server-C1-a1{accel}.toml")
    out.write_text(t)
    print("wrote", out)
