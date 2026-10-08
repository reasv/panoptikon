"""The calibration tools on ROCm, against fixture sysfs and /proc trees.

`rocm_sysfs.py` must index, key and total GPUs as `rocm.rs` does, or no
reading here joins `/health`; its per-process figure must come from KFD only
where KFD's PIDs are ours. The tools built on it (`vramrec.py`, `hog.py`,
`legs.py`, `selftest.py`, `newrun.py`, `ceiling_probe.py`) and `analyze.py`'s checks of amdgpu
samples are exercised on the same trees.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sys
import types
from pathlib import Path
from unittest import mock

import pytest

HERE = Path(__file__).resolve().parents[1]
MIB = 1024 * 1024
GIB = 1024 * MIB


def _load(name):
    spec = importlib.util.spec_from_file_location(f"_calib_rocm_{name}",
                                                  HERE / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


rocm_sysfs, vramrec, analyze, hog, legs, selftest, newrun, probe = (
    _load(name) for name in
    ("rocm_sysfs", "vramrec", "analyze", "hog", "legs", "selftest", "newrun",
     "ceiling_probe"))


class Host:
    """A fixture host: KFD topology (node 0 a CPU), render nodes, amdgpu PCI
    directories, `/proc/meminfo` and this process's PID namespace link."""

    def __init__(self, root: Path, host_pid_ns=True, kfd_proc=True):
        self.root = root
        self.roots = rocm_sysfs.Roots(kfd=str(root / "kfd"),
                                      pci_devices=str(root / "pci"),
                                      dev_dri=str(root / "dri"),
                                      proc=str(root / "proc"),
                                      cgroup=str(root / "cgroup"))
        for part in ("kfd/topology/nodes/0", "dri", "pci", "proc/self/ns"):
            (root / part).mkdir(parents=True)
        (root / "kfd/topology/nodes/0/properties").write_text(
            "cpu_cores_count 16\nsimd_count 0\n")
        os.symlink(rocm_sysfs.INIT_PID_NS if host_pid_ns else "pid:[4026532999]",
                   root / "proc/self/ns/pid")
        (root / "proc/meminfo").write_text(
            "MemTotal: 134217728 kB\nMemAvailable: 8388608 kB\n"
            "SReclaimable: 1048576 kB\n")
        if kfd_proc:
            (root / "kfd/proc").mkdir()

    def gpu(self, node, location_id, used=GIB, unique_id=0, openable=True,
            gtt=None):
        """A 24 GiB GPU (a 512 MiB carve-out plus `gtt` = (total, used) on an
        APU) at bus `location_id >> 8`, with KFD `gpu_id` 1000 + node."""
        apu = gtt is not None
        props = {"cpu_cores_count": 16 if apu else 0, "simd_count": 96,
                 "gfx_target_version": 110000, "location_id": location_id,
                 "domain": 0, "drm_render_minor": 127 + node,
                 "unique_id": unique_id}
        directory = self.root / f"kfd/topology/nodes/{node}"
        directory.mkdir()
        (directory / "properties").write_text(
            "".join(f"{key} {value}\n" for key, value in props.items()))
        (directory / "gpu_id").write_text(f"{1000 + node}\n")
        if openable:
            (self.root / f"dri/renderD{127 + node}").write_text("")
        pci = self.root / "pci" / rocm_sysfs.format_bdf(0, location_id)
        pci.mkdir()
        counters = {"vram_total": 512 * MIB if apu else 24 * GIB, "vram_used": used}
        if apu:
            counters.update(gtt_total=gtt[0], gtt_used=gtt[1])
        for name, value in counters.items():
            (pci / f"mem_info_{name}").write_text(f"{value}\n")
        return self

    def kfd(self, pid, node, held, pasid=None):
        (self.root / f"kfd/proc/{pid}").mkdir(exist_ok=True)
        (self.root / f"kfd/proc/{pid}/vram_{1000 + node}").write_text(f"{held}\n")
        if pasid is not None:
            (self.root / f"kfd/proc/{pid}/pasid").write_text(f"{pasid}\n")

    def fdinfo(self, pid, fd, text, target="/dev/dri/renderD128"):
        for part in ("fd", "fdinfo"):
            (self.root / f"proc/{pid}/{part}").mkdir(parents=True, exist_ok=True)
        os.symlink(target, self.root / f"proc/{pid}/fd/{fd}")
        (self.root / f"proc/{pid}/fdinfo/{fd}").write_text(text)


BDF_03, BDF_0C = "0000:03:00.0", "0000:0c:00.0"


def _fd(bdf, client, vram_kib, spelling="resident", gtt_kib=None, pasid=None):
    text = (f"pos:\t0\ndrm-driver:\tamdgpu\ndrm-pdev:\t{bdf}\n"
            + (f"drm-client-id:\t{client}\n" if client is not None else "")
            + f"drm-{spelling}-vram:\t{vram_kib} KiB\n")
    text += f"pasid:\t{pasid}\n" if pasid is not None else ""
    return text + (f"drm-{spelling}-gtt:\t{gtt_kib} KiB\n" if gtt_kib else "")


# --- inventory, keys and totals ---------------------------------------------


def test_inventory_indexes_openable_nodes_and_keys_like_rocm_rs(tmp_path):
    """Serial keys, BDF keys, a node this container cannot open, and a shared
    `unique_id` demoted to BDF keys."""
    host = Host(tmp_path / "a")
    host.gpu(1, 0x0300, unique_id=0xDEADBEEF).gpu(2, 0x0800, openable=False)
    host.gpu(3, 0x0C00)
    rows = rocm_sysfs.inventory(host.roots)
    assert [(row.index, row.key, row.bdf, row.gpu_id) for row in rows] == [
        (0, "GPU-00000000deadbeef", BDF_03, 1001),
        (1, f"GPU-BDF-{BDF_0C}", BDF_0C, 1003)]
    assert rocm_sysfs.memory_mb(host.roots, rows[0]) == (24576, 23552)

    twin = Host(tmp_path / "b")
    twin.gpu(1, 0x0300, unique_id=7).gpu(2, 0x0C00, unique_id=7)
    assert [row.key for row in rocm_sysfs.inventory(twin.roots)] == [
        f"GPU-BDF-{BDF_03}", f"GPU-BDF-{BDF_0C}"]
    assert rocm_sysfs.inventory(rocm_sysfs.Roots(kfd=str(tmp_path / "none"))) == []


def test_a_unified_gpu_totals_and_prices_its_gtt(tmp_path):
    """Total = carve-out + GTT; free GTT clamped by MemAvailable less
    SReclaimable (8 - 1 GiB); per process, fdinfo VRAM + GTT even where KFD's
    counter is readable."""
    host = Host(tmp_path).gpu(1, 0x0300, used=256 * MIB,
                              gtt=(64 * GIB, 4 * GIB))
    host.kfd(700, 1, 100 * MIB)
    host.fdinfo(700, 5, _fd(BDF_03, 1, 200 * 1024, gtt_kib=1024 * 1024))
    (gpu,) = rocm_sysfs.inventory(host.roots)
    assert gpu.unified
    assert rocm_sysfs.memory_mb(host.roots, gpu) == (512 + 65536, 256 + 7168)
    # SReclaimable above MemAvailable leaves no free GTT.
    (tmp_path / "proc/meminfo").write_text(
        "MemAvailable: 1048576 kB\nSReclaimable: 2097152 kB\n")
    assert rocm_sysfs.memory_mb(host.roots, gpu) == (512 + 65536, 256 + 0)
    # Without an SReclaimable row, MemAvailable alone.
    (tmp_path / "proc/meminfo").write_text("MemAvailable: 2097152 kB\n")
    assert rocm_sysfs.memory_mb(host.roots, gpu) == (512 + 65536, 256 + 2048)
    assert rocm_sysfs.process_vram_mb(host.roots, [gpu])[gpu.key] == (
        "fdinfo", {700: 1224}, [])
    assert legs.rocm_total_mb(0, host.roots) == 66048


# --- per-process sources ----------------------------------------------------


@pytest.mark.parametrize("host_pid_ns,kfd_proc,gpu_id,expected", [
    (True, True, True, ("kfd", {700: 300}, [])),
    (True, True, False, ("fdinfo", {700: 150, 701: 64, 702: 32}, [])),
    (True, False, True, ("fdinfo", {700: 150, 701: 64, 702: 32}, [])),
    (False, True, True, ("fdinfo", {700: 150, 701: 64, 702: 32}, [])),
])
def test_kfd_where_its_pids_are_ours_else_fdinfo(tmp_path, host_pid_ns,
                                                 kfd_proc, gpu_id, expected):
    host = Host(tmp_path, host_pid_ns, kfd_proc).gpu(1, 0x0300).gpu(2, 0x0C00)
    if not gpu_id:
        (tmp_path / "kfd/topology/nodes/1/gpu_id").unlink()
    if kfd_proc:
        host.kfd(700, 1, 300 * MIB)
    # Two descriptors of one client count once; the older key spelling
    # parses; another GPU's client and a non-DRM descriptor do not count.
    host.fdinfo(700, 3, _fd(BDF_03, 11, 150 * 1024))
    host.fdinfo(700, 4, _fd(BDF_03, 11, 150 * 1024))
    host.fdinfo(700, 5, _fd(BDF_0C, 12, 999 * 1024))
    host.fdinfo(700, 6, _fd(BDF_03, 13, 999 * 1024), target="/tmp/x")
    host.fdinfo(701, 3, _fd(BDF_03, 14, 64 * 1024, spelling="memory"))
    # Without `drm-client-id` the PASID identifies the client; with neither,
    # the record is not counted.
    host.fdinfo(702, 3, _fd(BDF_03, None, 32 * 1024, "memory", pasid=32769))
    host.fdinfo(702, 4, _fd(BDF_03, None, 32 * 1024, "memory", pasid=32769))
    host.fdinfo(702, 5, _fd(BDF_03, None, 999 * 1024, "memory"))
    gpus = rocm_sysfs.inventory(host.roots)
    assert rocm_sysfs.process_vram_mb(host.roots, gpus)[gpus[0].key] == expected


def test_in_a_container_kfd_is_found_by_the_fdinfo_pasid(tmp_path):
    """KFD's directory is named by host PID; the PASID in our PID's fdinfo
    names it. A PID holding memory with no KFD entry sends the GPU to fdinfo."""
    host = Host(tmp_path, host_pid_ns=False).gpu(1, 0x0300)
    host.kfd(4242, 1, 300 * MIB, pasid=32770)
    host.fdinfo(700, 3, _fd(BDF_03, 11, 150 * 1024, pasid=32770))
    (gpu,) = rocm_sysfs.inventory(host.roots)
    assert rocm_sysfs.process_vram_mb(host.roots, [gpu])[gpu.key] == (
        "kfd", {700: 300}, [])
    host.kfd(4243, 1, 500 * MIB, pasid=32771)
    host.fdinfo(701, 3, _fd(BDF_03, 12, 200 * 1024, pasid=32771))
    assert rocm_sysfs.process_vram_mb(host.roots, [gpu])[gpu.key] == (
        "kfd", {700: 300, 701: 500}, [])
    host.fdinfo(702, 3, _fd(BDF_03, 14, 64 * 1024, pasid=99))
    assert rocm_sysfs.process_vram_mb(host.roots, [gpu])[gpu.key] == (
        "fdinfo", {700: 150, 701: 200, 702: 64}, [])


def test_a_descriptor_inherited_across_fork_counts_once(tmp_path):
    """Parent and child both name the parent's PASID: its KFD entry is
    credited to the lower PID only. A child with a PASID of its own is
    credited its own entry, whether or not its parent is read, and when its
    PID is lower than its parent's."""
    host = Host(tmp_path, host_pid_ns=False).gpu(1, 0x0300)
    host.kfd(4242, 1, 300 * MIB, pasid=32770)
    for pid in (700, 701):
        host.fdinfo(pid, 3, _fd(BDF_03, 11, 150 * 1024, pasid=32770))
    (gpu,) = rocm_sysfs.inventory(host.roots)
    assert rocm_sysfs.process_vram_mb(host.roots, [gpu], [701, 700])[gpu.key] == (
        "kfd", {700: 300}, [])
    host.kfd(4243, 1, 500 * MIB, pasid=32771)
    host.fdinfo(701, 12, _fd(BDF_03, 12, 200 * 1024, pasid=32771))
    assert rocm_sysfs.process_vram_mb(host.roots, [gpu], [701, 700])[gpu.key] == (
        "kfd", {700: 300, 701: 500}, [])
    assert rocm_sysfs.process_vram_mb(host.roots, [gpu], [701])[gpu.key] == (
        "kfd", {701: 500}, [])
    host.fdinfo(650, 4, _fd(BDF_03, 12, 200 * 1024, pasid=32771))
    host.fdinfo(650, 5, _fd(BDF_03, 11, 150 * 1024, pasid=32770))
    assert rocm_sysfs.process_vram_mb(host.roots, [gpu], [650, 700])[gpu.key] == (
        "kfd", {650: 500, 700: 300}, [])


def test_one_pasid_on_two_gpus_counts_on_each(tmp_path):
    host = Host(tmp_path, kfd_proc=False).gpu(1, 0x0300).gpu(2, 0x0C00)
    host.fdinfo(700, 3, _fd(BDF_03, None, 100 * 1024, "memory", pasid=32769))
    host.fdinfo(700, 4, _fd(BDF_0C, None, 200 * 1024, "memory", pasid=32769),
                target="/dev/dri/renderD129")
    first, second = rocm_sysfs.inventory(host.roots)
    assert rocm_sysfs.process_vram_mb(host.roots, [first, second]) == {
        first.key: ("fdinfo", {700: 100}, []),
        second.key: ("fdinfo", {700: 200}, [])}


def test_a_pasid_reused_after_the_kfd_list_is_read_is_left_out(tmp_path,
                                                               monkeypatch):
    host = Host(tmp_path, host_pid_ns=False).gpu(1, 0x0300)
    host.kfd(4242, 1, 300 * MIB, pasid=32770)
    host.fdinfo(700, 3, _fd(BDF_03, 11, 150 * 1024, pasid=32770))
    read = rocm_sysfs._drm_fdinfo

    def then_reused(roots, pid):
        texts = read(roots, pid)
        (tmp_path / "kfd/proc/4242/vram_1001").unlink()
        (tmp_path / "kfd/proc/4242/pasid").unlink()
        (tmp_path / "kfd/proc/4242").rmdir()
        host.kfd(5000, 1, 800 * MIB, pasid=32770)
        return texts

    monkeypatch.setattr(rocm_sysfs, "_drm_fdinfo", then_reused)
    (gpu,) = rocm_sysfs.inventory(host.roots)
    assert rocm_sysfs.process_vram_mb(host.roots, [gpu])[gpu.key] == (
        "kfd", {}, [])


def test_a_pid_whose_descriptors_are_unreadable_is_counted(tmp_path, monkeypatch):
    host = Host(tmp_path, host_pid_ns=False).gpu(1, 0x0300)
    host.fdinfo(700, 3, _fd(BDF_03, 11, 150 * 1024))
    (tmp_path / "proc/702/fd").mkdir(parents=True)
    listdir = os.listdir

    def denied(path):
        if str(path).endswith("proc/702/fd"):
            raise PermissionError(13, "denied", path)
        return listdir(path)

    monkeypatch.setattr(rocm_sysfs.os, "listdir", denied)
    (gpu,) = rocm_sysfs.inventory(host.roots)
    assert rocm_sysfs.process_vram_mb(host.roots, [gpu])[gpu.key] == (
        "fdinfo", {700: 150}, [702])


# --- vramrec: the amdgpu oracle ---------------------------------------------


def test_vramrec_rows_take_the_gateways_keys_and_name_their_source(tmp_path):
    host = Host(tmp_path).gpu(1, 0x0300)
    host.kfd(700, 1, 300 * MIB)
    answers = [None, {"gpus": [{"index": 0, "uuid": "GPU-CPU"},
                               {"index": 0, "uuid": "GPU-00ff", "bdf": BDF_03}]}]
    oracle = vramrec.AmdgpuOracle(rocm_sysfs.inventory(host.roots), host.roots,
                                  health_url="http://gw", health_interval=0.0,
                                  fetch=lambda url: answers.pop(0))
    assert oracle.meta[0]["uuid"] == f"GPU-BDF-{BDF_03}"
    sample = vramrec.build_sample(0, oracle, vramrec.ProcCache((), False),
                                  None, 0.0)
    (row,) = sample["gpus"]
    assert (row["uuid"], row["total_mb"], row["used_mb"], row["free_mb"]) == (
        "GPU-00ff", 24576, 1024, 23552)
    assert (row["oracle_source"], row["unreadable_pids"]) == ("amdgpu-kfd", [])
    assert [(proc["pid"], proc["used_mb"]) for proc in row["procs"]] == [(700, 300)]


def test_used_that_moves_during_the_process_scan_is_flagged_and_not_judged(
        tmp_path, monkeypatch):
    host = Host(tmp_path).gpu(1, 0x0300).gpu(2, 0x0C00, used=2 * GIB)
    host.kfd(700, 1, 300 * MIB)
    oracle = vramrec.AmdgpuOracle(rocm_sysfs.inventory(host.roots), host.roots)

    def sample():
        rows = vramrec.build_sample(0, oracle, vramrec.ProcCache((), False),
                                    None, 0.0)["gpus"]
        return [(row["used_mb"], row["skew_mb"]) for row in rows]

    assert sample() == [(1024, 0), (2048, 0)]
    scan = vramrec.rocm_sysfs.process_vram_mb

    def moving(*args):
        (tmp_path / "pci" / BDF_03 / "mem_info_vram_used").write_text(
            f"{6 * GIB}\n")
        (tmp_path / "pci" / BDF_0C / "mem_info_vram_used").write_text(
            f"{GIB // 2}\n")
        return scan(*args)

    monkeypatch.setattr(vramrec.rocm_sysfs, "process_vram_mb", moving)
    assert sample() == [(6144, 5120), (512, 1536)]
    # A failed read before the scan leaves the skew unknown.
    failing, memory = iter([None]), vramrec.rocm_sysfs.memory_mb
    monkeypatch.setattr(vramrec.rocm_sysfs, "memory_mb",
                        lambda *args: next(failing, memory(*args)))
    assert sample() == [(6144, None), (512, 0)]

    # The allowance on this 24 GiB GPU is 1 GiB, on a 96 GiB one 1966 MiB; the
    # measured difference is 3800 MiB with `external_mb` 0, 1800 MiB with 2000
    # and 1200 with 5000.
    ctx = _amdgpu_ctx("amdgpu-kfd", [(900, 1200)])
    gpu = ctx.health_samples[0]["health"]["vram"][0]
    row = ctx.vram_samples[0]["gpus"][0]
    for external, skew, verdict in ((0, 1024, "FAIL"), (2000, 1024, "PASS"),
                                    (5000, 1024, "PASS"), (0, 1025, "SKIP"),
                                    (0, None, "SKIP")):
        gpu["external_mb"], row["skew_mb"] = external, skew
        result = analyze.check_oracle_agreement(ctx)
        assert result.verdict == verdict, (external, skew)
    assert result.numbers["skewed_samples"] == 1
    gpu["total_mb"], gpu["external_mb"], row["skew_mb"] = 98304, 0, 1500
    result = analyze.check_oracle_agreement(ctx)
    assert result.verdict == "FAIL"
    assert result.numbers["worst_sample"]["skew_mb"] == 1500


def test_vramrec_selects_the_amdgpu_oracle_without_nvml(tmp_path, monkeypatch):
    host = Host(tmp_path).gpu(1, 0x0300).gpu(2, 0x0C00)

    class NoNvml:
        available, error, driver_version, nvml_version = False, "none", None, None

        def __init__(self, wanted):
            pass

        def shutdown(self):
            pass

    class FixtureOracle(vramrec.AmdgpuOracle):
        def __init__(self, gpus, **kwargs):
            super().__init__(gpus, host.roots, **kwargs)

    inventory = vramrec.rocm_sysfs.inventory
    monkeypatch.setattr(vramrec, "Nvml", NoNvml)
    monkeypatch.setattr(vramrec, "AmdgpuOracle", FixtureOracle)
    monkeypatch.setattr(vramrec.rocm_sysfs, "inventory",
                        lambda *roots: inventory(host.roots))
    monkeypatch.setattr(vramrec.signal, "signal", lambda *args: None)
    out = tmp_path / "vramrec.jsonl"
    assert vramrec.main(["--out", str(out), "--duration", "0", "--gpu", "1",
                         "--quiet"]) == 0
    header, sample = (json.loads(line) for line in out.read_text().splitlines())
    assert [(gpu["index"], gpu["pci_bus_id"]) for gpu in header["gpus"]] == [
        (1, BDF_0C)]
    assert [(gpu["index"], gpu["oracle_source"]) for gpu in sample["gpus"]] == [
        (1, "amdgpu-kfd")]


# --- analyze: amdgpu samples ------------------------------------------------


def _amdgpu_ctx(source, procs, base=None):
    key = "GPU-00ff"
    vram = {"kind": "sample", "t_wall": 100.0,
            "gpus": [{"index": 0, "uuid": key, "total_mb": 24576,
                      "used_mb": 5000, "free_mb": 19576,
                      "oracle_source": source,
                      "procs": [{"pid": pid, "used_mb": mb, "env": {},
                                 "cmdline": "inferio-worker"}
                                for pid, mb in procs]}]}
    health = {"ok": True, "vram": [{"gpu_uuid": key, "total_mb": 24576,
                                    "external_known": True,
                                    "external_mb": 5000 - sum(mb for _, mb in procs),
                                    "footprints_mb": 1200}]}
    if base is not None:
        # As `/health` reports a ROCm replica: no UUID, the HIP index as `gpu`.
        health["gpus"] = [{"index": 0, "uuid": "CPU"},
                          {"index": 0, "uuid": key, "bdf": BDF_03}]
        health["models"] = [{"inference_id": "tags/wd-vit-tagger-v3",
                             "replicas": [{"gpu": "0", "gpu_uuid": None,
                                           "base_mb": base,
                                           "base_method": "fdinfo"}]}]
    grant = {"ts": "t", "t_wall": 100.0, "message": "issued a memory grant",
             "fields": {"model": "m", "gpu": key, "mb": 20000,
                        "headroom_mb": 20000}}
    ctx = analyze.Context(
        args=types.SimpleNamespace(worker_pattern="inferio", join_tolerance=1.0,
                                   probe=[], base_window=5.0),
        vramrec=[vram], healthrec=[{"kind": "sample", "t_wall": 100.0,
                                    "iso": "t", "health": health}],
        hog=[], log=[grant], before=None, after=None, jobs=None, probes=[])
    return ctx


def test_analyze_checks_amdgpu_samples():
    """An idle amdgpu row is priced; an fdinfo base is judged against KFD and
    only reported against fdinfo; the grant is joined to the sysfs free."""
    assert analyze.check_oracle_agreement(
        _amdgpu_ctx("amdgpu-kfd", [])).verdict == "PASS"
    assert analyze.check_oracle_agreement(
        _amdgpu_ctx("amdgpu-fdinfo", [(900, 1200)])).verdict == "PASS"
    assert analyze.check_footprint_agreement(
        _amdgpu_ctx("amdgpu-fdinfo", [(900, 1200)])).verdict == "INFO"
    judged = analyze.check_base_accuracy(
        _amdgpu_ctx("amdgpu-kfd", [(900, 1200)], base=1180))
    assert judged.verdict == "PASS"
    assert judged.numbers["worst"]["oracle_source"] == "amdgpu-kfd"
    same_counter = analyze.check_base_accuracy(
        _amdgpu_ctx("amdgpu-fdinfo", [(900, 1200)], base=1180))
    assert same_counter.verdict == "INFO"
    assert "one counter read twice" in same_counter.detail
    safety = analyze.check_grant_safety(_amdgpu_ctx("amdgpu-kfd", []))
    assert (safety.verdict, safety.numbers["joined"]) == ("FAIL", 1)


def test_base_accuracy_never_mixes_oracle_sources_in_one_window():
    """One fdinfo fallback sample (base_mb's own counter) must not become the
    window minimum of a replica judged against KFD."""
    ctx = _amdgpu_ctx("amdgpu-kfd", [(900, 1200)], base=1000)
    fallback = json.loads(json.dumps(ctx.vram_samples[0]))
    fallback["t_wall"] = 100.25
    fallback["gpus"][0].update(oracle_source="amdgpu-fdinfo")
    fallback["gpus"][0]["procs"][0]["used_mb"] = 1000
    ctx = analyze.Context(args=ctx.args, vramrec=[ctx.vram_samples[0], fallback],
                          healthrec=ctx.healthrec, hog=[], log=[], before=None,
                          after=None, jobs=None, probes=[])
    verdict = analyze.check_base_accuracy(ctx)
    assert verdict.verdict == "FAIL"
    assert verdict.numbers["worst"]["oracle_source"] == "amdgpu-kfd"
    assert verdict.numbers["worst"]["oracle_pid_mb"] == 1200


def test_healthrec_keeps_the_worker_ram_keys_by_default():
    healthrec = _load("healthrec")
    ram = {"ram_resident_mb": 1381, "ram_mb_per_unit": None,
           "ram_booked_mb": 0, "ram_ceiling_binding": False}
    flat = healthrec.flatten_health(
        {"ok": True, "status_code": 200, "latency_ms": 1, "error": None,
         "payload": {"vram": [{"gpu_uuid": "GPU-0", "workers": [ram]}]}},
        full=False)
    assert {key: flat["workers"][0][key] for key in ram} == ram


def test_only_an_unreadable_worker_leaves_the_amdgpu_row_unpriced():
    ctx = _amdgpu_ctx("amdgpu-fdinfo", [])
    sample = ctx.vram_samples[0]
    sample["gpus"][0]["unreadable_pids"] = [801, 802]
    sample["procs"] = [{"pid": 802, "cmdline": "sshd", "env": {}}]
    assert analyze.check_oracle_agreement(ctx).verdict == "PASS"
    sample["procs"].append({"pid": 801, "cmdline": "inferio-worker", "env": {}})
    assert analyze.check_oracle_agreement(ctx).verdict == "SKIP"


# --- hog: sysfs free, own from fdinfo, the ledger's key ----------------------


def test_hog_on_hip_reads_sysfs_by_torchs_pci_address(tmp_path, monkeypatch,
                                                     capsys):
    host = Host(tmp_path, host_pid_ns=False).gpu(1, 0x0300, unique_id=0xFF)
    host.fdinfo(os.getpid(), 3, _fd(BDF_03, 1, 512 * 1024))
    props = types.SimpleNamespace(pci_domain_id=0, pci_bus_id=3, pci_device_id=0,
                                  uuid="hip-uuid", name="AMD Radeon")
    tensor = types.SimpleNamespace(fill_=lambda value: None)
    cuda = types.SimpleNamespace(
        is_available=lambda: True, device_count=lambda: 1,
        get_device_properties=lambda index: props, synchronize=lambda device: None,
        mem_get_info=lambda index: pytest.fail("mem_get_info read on HIP"))
    monkeypatch.setitem(sys.modules, "torch", types.SimpleNamespace(
        cuda=cuda, version=types.SimpleNamespace(hip="7.2"), uint8="u8",
        __version__="2.11.0+rocm7.2", device=lambda name: name,
        zeros=lambda *args, **kwargs: tensor))
    backend = hog.GpuBackend(0, 128, host.roots)
    assert backend.free_total_mb() == (23552, 24576)
    assert backend.own_mb() == 512
    described = backend.describe()
    assert (described["gpu_uuid"], described["gpu_bdf"], described["free_source"]) == (
        "GPU-00000000000000ff", BDF_03, "amdgpu-sysfs")

    props.pci_bus_id = 9
    cuda.mem_get_info = lambda index: (GIB, 2 * GIB)
    monkeypatch.setattr(hog.GpuBackend, "_nvml", lambda self: (None, None))
    lost = hog.GpuBackend(0, 128, host.roots)
    assert lost.describe()["free_source"] == "torch mem_get_info"
    assert "matches no amdgpu sysfs GPU" in capsys.readouterr().err


# --- legs, selftest, newrun --------------------------------------------------


def test_legs_rocm_configs_name_the_accelerator_and_drop_cudnn(tmp_path):
    import tomllib

    rendered = tomllib.loads(legs.render_config("R1", HERE.parents[1]))
    assert rendered["inference_local"]["python_env"]["accelerator"] == "rocm"
    (tmp_path / "lib" / "python3.12" / "site-packages" / "nvidia" / "cudnn"
     / "lib").mkdir(parents=True)
    python = str(tmp_path / "bin" / "python")
    env = legs.config_env("R1", HERE.parents[1], {}, python)
    assert "LD_LIBRARY_PATH" not in env
    assert env["RUST_LOG"].endswith(",panoptikon::db::batch_auto=debug")
    assert "LD_LIBRARY_PATH" in legs.config_env("C1", HERE.parents[1], {}, python)


@pytest.mark.parametrize("name,variable,pin", [
    ("R7", None, "1"), ("R2", "HIP_VISIBLE_DEVICES", "1"),
    ("R3", "ROCR_VISIBLE_DEVICES", "0")])
def test_legs_pinned_rocm_configs(name, variable, pin):
    import tomllib

    rendered = tomllib.loads(legs.render_config(name, HERE.parents[1]))
    (registry,) = [Path(entry) for entry in
                   rendered["inference_local"]["config_dirs"]
                   if "calibration-protocol" in entry]
    (file,) = registry.glob("*.toml")
    entry = tomllib.loads(file.read_text())["group"]["clip"]["inference_ids"][
        "apple_MobileCLIP-S1"]
    assert entry["config"]["devices"] == [pin]
    env = legs.config_env(name, HERE.parents[1], {})
    assert [key for key in legs.DEVICE_ENV if key in env] == (
        [variable] if variable else [])
    assert env.get(variable) == ("1" if variable else None)


def _clear_visibility(monkeypatch):
    for name in legs.DEVICE_ENV:
        monkeypatch.delenv(name, raising=False)


def test_legs_rocm_refuses_an_inherited_visibility_variable(tmp_path, monkeypatch):
    _clear_visibility(monkeypatch)
    monkeypatch.setenv("ROCR_VISIBLE_DEVICES", "0")
    argv = ["--repo", str(HERE.parents[1]), "--write-config", str(tmp_path),
            "--python", "/opt/venv/bin/python"]
    with pytest.raises(SystemExit, match="ROCR_VISIBLE_DEVICES is set"):
        legs.main(["--config", "R2", *argv])
    assert legs.main(["--config", "R3", *argv]) == 0
    assert legs.main(["--config", "C1", *argv]) == 0
    written = (tmp_path / "server-R3.toml").read_text()
    assert 'python = "/opt/venv/bin/python"' in written


def test_legs_run_refuses_an_inherited_visibility_variable(monkeypatch):
    _clear_visibility(monkeypatch)
    monkeypatch.setenv("ROCR_VISIBLE_DEVICES", "0")
    monkeypatch.setattr(legs, "nvml_total_mb", lambda device: None)
    monkeypatch.setattr(legs.rocm_sysfs, "inventory", lambda *roots: [])
    with pytest.raises(SystemExit, match="ROCR_VISIBLE_DEVICES is set"):
        legs.main(["--scenario", "S14", "--config", "R1", "--repo",
                   str(HERE.parents[1]), "--no-dotenv", "--dry-run"])


def test_legs_write_config_reads_the_repo_dotenv(tmp_path, monkeypatch):
    _clear_visibility(monkeypatch)
    shipped = HERE.parents[1] / "config" / "server" / "default.toml"
    (tmp_path / "config" / "server").mkdir(parents=True)
    (tmp_path / "config" / "server" / "default.toml").write_text(shipped.read_text())
    (tmp_path / ".env").write_text("HIP_VISIBLE_DEVICES=0\n")
    argv = ["--config", "R1", "--repo", str(tmp_path), "--python",
            "/opt/venv/bin/python", "--write-config", str(tmp_path / "out")]
    with pytest.raises(SystemExit, match="HIP_VISIBLE_DEVICES is set"):
        legs.main(argv)
    assert legs.main([*argv, "--no-dotenv"]) == 0


def test_legs_totals_a_rocm_gpu_from_sysfs(tmp_path, monkeypatch, capsys):
    host = Host(tmp_path).gpu(1, 0x0300, unique_id=1).gpu(2, 0x0C00, unique_id=2)
    (tmp_path / "pci" / BDF_0C / "mem_info_vram_total").write_text(f"{16 * GIB}\n")
    inventory, total = legs.rocm_sysfs.inventory, legs.rocm_total_mb
    _clear_visibility(monkeypatch)
    monkeypatch.setattr(legs, "nvml_total_mb", lambda device: None)
    monkeypatch.setattr(legs.rocm_sysfs, "inventory",
                        lambda *roots: inventory(host.roots))
    monkeypatch.setattr(legs, "rocm_total_mb",
                        lambda device: total(device, host.roots))
    assert legs.main(["--scenario", "S2", "--config", "R1", "--repo",
                      str(HERE.parents[1]), "--hog-device", "1",
                      "--no-dotenv", "--dry-run"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert (plan["gpu_total_mb"], plan["gpu_total_mb_source"]) == (
        16384, "amdgpu-sysfs")


def test_the_tools_pin_like_the_spawner(tmp_path, monkeypatch, capsys):
    """selftest and ceiling_probe pin HIP device N in KFD order, an unopenable
    node taking no index; the probe takes that GPU when NVML has none, and
    reads it from sysfs and its own usage from KFD."""
    host = Host(tmp_path).gpu(1, 0x0300, used=2 * GIB).gpu(2, 0x0800,
                                                           openable=False)
    host.gpu(3, 0x0C00, used=128 * MIB, gtt=(GIB, 0))

    def pin(device, environ=None):
        gpu = rocm_sysfs.pinned_gpu(device, environ or {}, host.roots)
        return gpu and rocm_sysfs.pin_env(gpu)

    assert pin(0) == {"HIP_VISIBLE_DEVICES": "0", "PANOPTIKON_DEVICE_PIN": "0"}
    unified = {"HIP_VISIBLE_DEVICES": "1", "PANOPTIKON_DEVICE_PIN": "1",
               "PANOPTIKON_UNIFIED_GPU": BDF_0C}
    assert pin(1) == unified
    assert pin(0, {"HIP_VISIBLE_DEVICES": "1"}) == unified
    assert pin(0, {"HIP_VISIBLE_DEVICES": "0,1"}) is None
    assert pin(0, {"ROCR_VISIBLE_DEVICES": "1"}) is None
    assert pin(0, {"GPU_DEVICE_ORDINAL": "0"}) is None
    assert pin(5) is None

    host.kfd(os.getpid(), 1, 300 * MIB)
    host.kfd(os.getpid() + 1, 1, 700 * MIB)
    rocm = probe.Rocm.pinned(0, {}, host.roots)
    assert rocm.env() == pin(0)
    assert rocm.row() == {
        "index": 0, "uuid": f"GPU-BDF-{BDF_03}", "name": None,
        "total_mb": 24576, "free_mb": 22528, "bdf": BDF_03, "unified": False,
        "backend": "rocm"}
    assert (rocm.free_mb(), rocm.own_mb(), rocm.own_sources) == (
        22528, 300, {"kfd"})
    # Without KFD's per-process directory the next reading is fdinfo's.
    shutil.rmtree(tmp_path / "kfd/proc")
    rocm.own_mb()
    assert rocm.own_sources == {"fdinfo", "kfd"}
    apu = probe.Rocm.pinned(1, {}, host.roots)
    assert apu.env() == unified
    assert (apu.row()["unified"], apu.row()["bdf"]) == (True, BDF_0C)
    assert (apu.row()["total_mb"], apu.free_mb()) == (512 + 1024, 384 + 1024)
    assert probe.Rocm.pinned(0, {"CUDA_VISIBLE_DEVICES": "0"}, host.roots) is None

    # NVML first: a ROCm GPU is taken only where NVML has no GPU N.
    pinned = probe.Rocm.pinned
    _clear_visibility(monkeypatch)
    monkeypatch.setattr(probe.Rocm, "pinned", lambda device, environ: pinned(
        device, environ, host.roots))
    nvml_gpu = {"index": 1, "uuid": "GPU-1"}
    for gpus, backend, device in (([], "rocm", apu.row()),
                                  ([nvml_gpu], "cuda", nvml_gpu)):
        monkeypatch.setattr(probe, "Nvml", lambda: types.SimpleNamespace(
            gpus=lambda: gpus, error=None))
        assert probe.main(["--model", "tags/wd-vit-tagger-v3", "--device", "1",
                           "--dry-run"]) == 0
        plan = json.loads(capsys.readouterr().out)
        assert (plan["backend"], plan["device"]) == (backend, device)


def _rocm_probe(tmp_path, monkeypatch, host, hip="6.4.43482", nvml_gpus=(),
                count=1):
    """A probe run on the fixture `host` with a stand-in torch and an impl that
    allocates nothing: its argv (`--max-batch 1`), the torch to put in
    `sys.modules`, and the list the free readings and torch.cuda calls are
    appended to."""
    pinned = probe.Rocm.pinned
    monkeypatch.setattr(probe.Rocm, "pinned", lambda device, environ: pinned(
        device, environ, host.roots))
    monkeypatch.setattr(probe, "Nvml", lambda: types.SimpleNamespace(
        gpus=lambda: list(nvml_gpus), error=None, handle_for_uuid=lambda uuid: None,
        free_mb=lambda handle: None))
    _clear_visibility(monkeypatch)
    monkeypatch.setattr(sys, "path", [str(HERE.parents[1] / "python"), *sys.path])
    (tmp_path / "item.txt").write_text("a caption")
    (tmp_path / "manifest.json").write_text(
        json.dumps({"items": [{"path": "item.txt", "kind": "text"}]}))
    (tmp_path / "registry.toml").write_text(
        '[group.probe.inference_ids.echo]\nconfig.impl_class = "echo_test"\n')
    zero = lambda *args: 0  # noqa: E731
    calls = []
    free_mb = probe.Rocm.free_mb
    monkeypatch.setattr(probe.Rocm, "free_mb",
                        lambda self: calls.append("free") or free_mb(self))
    cuda = types.SimpleNamespace(
        device_count=lambda: calls.append("count") or (
            count if os.environ.get("HIP_VISIBLE_DEVICES") == "0" else 2),
        get_device_name=lambda index: "AMD Radeon",
        is_available=lambda: True,
        is_initialized=lambda: False,
        synchronize=lambda: calls.append("synchronize"),
        empty_cache=lambda: None,
        reset_peak_memory_stats=lambda: calls.append("reset_peak_memory_stats"),
        memory_reserved=zero,
        memory_allocated=zero,
        max_memory_reserved=zero,
        max_memory_allocated=zero)
    torch = types.SimpleNamespace(
        __version__="2.8.0+rocm6.4", cuda=cuda,
        version=types.SimpleNamespace(hip=hip, cuda=None))
    out = tmp_path / "probe.json"
    argv = ["--model", "probe/echo", "--device", "0", "--repo", str(tmp_path),
            "--registry", str(tmp_path / "registry.toml"),
            "--impl-dir", str(HERE.parents[1] / "python/tests/inferio_worker"
                              "/fixture_impls"),
            "--corpus", str(tmp_path / "manifest.json"), "--max-batch", "1",
            "--out", str(out)]
    return argv, torch, calls


@pytest.mark.parametrize("hip,bdf,nvml_gpus,count,exit_calls", [
    ("6.4.43482", BDF_03, [], 1, None), (None, BDF_03, [], 1, ["free"]),
    ("6.4.43482", BDF_03, [], 2, ["free", "count"]),
    ("6.4.43482", BDF_03, [], 0, ["free", "count"]),
    ("6.4.43482", BDF_03, [{"index": 0, "uuid": "GPU-1"}], 1, []),
    ("6.4.43482", None, [], 1, ["free", "count", "count", "synchronize", "free"]),
    ("6.4.43482", BDF_0C, [], 1, ["free", "count", "count", "synchronize", "free"])],
    ids=["ok", "hip None", "count 2", "count 0", "hip on NVML",
         "BDF mismatch", "another GPU"])
def test_the_probe_measures_the_pinned_rocm_gpu_or_exits(
        tmp_path, monkeypatch, hip, bdf, nvml_gpus, count, exit_calls):
    """A run to the JSON on a fixture ROCm host, with a stand-in torch and an
    impl that allocates nothing. The probe exits before the load unless torch
    is a ROCm build that sees one device, and exits on a ROCm torch pinned to
    an NVML GPU; it exits after the priced load, before any batch, unless the
    model loaded on the pinned GPU (`memory.device_bdf()`). The stand-in torch
    sees one device only once `HIP_VISIBLE_DEVICES` is set. `exit_calls` is the
    free readings and torch.cuda calls made before the exit, None for a full
    run. The discrete GPU's 512 MiB free is below the host-RAM reserve, which
    guards only a unified GPU."""
    host = Host(tmp_path / "host").gpu(1, 0x0300, used=24 * GIB - 512 * MIB)
    host.kfd(os.getpid(), 1, 300 * MIB)
    argv, torch, calls = _rocm_probe(tmp_path, monkeypatch, host, hip,
                                     nvml_gpus, count)
    out = tmp_path / "probe.json"
    with mock.patch.dict(os.environ), mock.patch.dict(sys.modules,
                                                      {"torch": torch}):
        from inferio_worker import memory

        monkeypatch.setattr(memory, "device_bdf", lambda: bdf)
        if exit_calls is not None:
            with pytest.raises(SystemExit):
                probe.main(argv)
            assert calls == exit_calls
            return
        assert probe.main(argv) == 0
        assert (os.environ["HIP_VISIBLE_DEVICES"],
                os.environ["PANOPTIKON_DEVICE_PIN"]) == ("0", "0")
    result = json.loads(out.read_text())
    assert result["backend"] == "rocm"
    device = result["device"]
    assert (device["bdf"], device["hip_visible_devices"],
            device["own_source"]) == (BDF_03, "0", ["kfd"])
    assert None not in (result["load"]["free_before_mb"],
                        result["load"]["base_nvml_mb"],
                        result["batches"][0]["gpu_free_mb"],
                        result["batches"][0]["nvml_own_mb"])
    assert result["gqa_check"] == "not applicable"
    assert [batch["items"] for batch in result["batches"]] == [1]
    assert "ram_floor" not in result


def test_the_probe_starts_no_batch_on_a_unified_gpu_below_the_ram_reserve(
        tmp_path, monkeypatch):
    """On a unified GPU no sweep or bisect batch starts while free memory is
    below the CPU device's reserve, a tenth of the fixture's 128 GiB of RAM.
    Free memory drops once batch 2 has run: the sweep stops before batch 4,
    and the bisect runs no batch."""
    host = Host(tmp_path / "host").gpu(1, 0x0300, gtt=(64 * GIB, 0))
    argv, torch, _ = _rocm_probe(tmp_path, monkeypatch, host)
    argv[argv.index("--max-batch") + 1] = "8"
    ran = []
    build_inputs = probe.build_inputs
    monkeypatch.setattr(probe, "build_inputs", lambda items, count, *rest: (
        ran.append(count) or build_inputs(items, count, *rest)))
    monkeypatch.setattr(probe.Rocm, "free_mb",
                        lambda self: 1000 if 2 in ran else 20000)
    with mock.patch.dict(os.environ), mock.patch.dict(sys.modules,
                                                      {"torch": torch}):
        from inferio_worker import memory

        monkeypatch.setattr(memory, "device_bdf", lambda: BDF_03)
        assert probe.main([*argv, "--bisect-oom"]) == 0
    result = json.loads((tmp_path / "probe.json").read_text())
    assert [batch["items"] for batch in result["batches"]] == [1, 2]
    assert result["bisect"]["trace"] == []
    stops = [{"items": items, "free_mb": 1000} for items in (4, 1)]
    assert result["ram_floor"] == {"floor_mb": 13107, "stopped_before": stops}
    assert [probe.ram_reserve_mb(mb) for mb in (3900, 16384, 32768, 163840)] == [
        975, 2048, 3276, 16384]


@pytest.mark.parametrize("free_after,ran,stops,trace,high", [
    (0, [], [1, 1, 1], [], None),
    (2, [1, 1], [1, 1], [], None),
    (5, [1, 1, 1, 1, 2], [4], [1, 2], 2)], ids=["warmup", "repeat", "bisect"])
def test_the_ram_floor_guards_warmup_and_repeats_and_stops_the_bisect(
        tmp_path, monkeypatch, free_after, ran, stops, trace, high):
    """Free memory on a unified GPU drops below the reserve once `free_after`
    batches have run: one warmup, size 1 twice, then the bisect from 1. No
    warmup or repeat starts below it, and a bisect it stops is
    `stopped_early` with `high_items` the largest size it ran. The reserve is
    a tenth of the 32 GiB cgroup limit, not of the host's 128 GiB."""
    host = Host(tmp_path / "host").gpu(1, 0x0300, gtt=(64 * GIB, 0))
    (tmp_path / "host/cgroup").mkdir()
    (tmp_path / "host/cgroup/memory.max").write_text(f"{32 * GIB}\n")
    argv, torch, _ = _rocm_probe(tmp_path, monkeypatch, host)
    counts = []
    build_inputs = probe.build_inputs
    monkeypatch.setattr(probe, "build_inputs", lambda items, count, *rest: (
        counts.append(count) or build_inputs(items, count, *rest)))
    monkeypatch.setattr(probe.Rocm, "free_mb", lambda self: (
        1000 if len(counts) >= free_after else 20000))
    with mock.patch.dict(os.environ), mock.patch.dict(sys.modules,
                                                      {"torch": torch}):
        from inferio_worker import memory

        monkeypatch.setattr(memory, "device_bdf", lambda: BDF_03)
        assert probe.main([*argv, "--repeats", "2", "--bisect-oom"]) == 0
    result = json.loads((tmp_path / "probe.json").read_text())
    assert counts == ran
    assert result["ram_floor"] == {
        "floor_mb": 3276,
        "stopped_before": [{"items": items, "free_mb": 1000} for items in stops]}
    bisect = result["bisect"]
    assert [step["items"] for step in bisect["trace"]] == trace
    assert (bisect["stopped_early"], bisect["high_items"]) == (True, high)


def test_selftest_reasons_name_what_is_missing(tmp_path, monkeypatch):
    def fdinfo(own_mb, hip=True, bdf=None):
        return types.SimpleNamespace(fdinfo_own_vram_mb=lambda: own_mb,
                                     _torch=lambda: None,
                                     _is_hip=lambda torch: hip,
                                     device_bdf=lambda: bdf)

    host = Host(tmp_path)
    gpu_nodes = selftest.rocm_sysfs.gpu_nodes
    monkeypatch.setattr(selftest.rocm_sysfs, "gpu_nodes",
                        lambda *roots: gpu_nodes(host.roots))
    assert selftest.rocm_reason("tier") == "not a ROCm host"
    assert selftest._fdinfo_reason(fdinfo(None)) == (
        "no DRM fdinfo VRAM figure for this process: not a ROCm host")
    assert selftest._free_tier_reason(fdinfo(None), "amdgpu-sysfs") == (
        "no amdgpu sysfs: not a ROCm host")
    host.gpu(1, 0x0300, openable=False)
    assert selftest.rocm_reason("tier") == (
        "KFD lists a GPU but this process cannot open its render node")
    host.gpu(2, 0x0C00)
    assert selftest.rocm_reason("tier") == "tier"
    assert selftest._fdinfo_reason(fdinfo(None, hip=False)) == (
        "the worker's torch is not a ROCm build")
    assert selftest._free_tier_reason(
        fdinfo(None, hip=False, bdf="0000:01:00.0"), "amdgpu-sysfs") == (
        "the worker's torch is not a ROCm build")
    assert selftest._free_tier_reason(
        fdinfo(None, bdf="0000:01:00.0"), "amdgpu-sysfs") == (
        "amdgpu sysfs present but mem_info_vram_* unreadable")
    assert selftest._fdinfo_reason(fdinfo(None)) == (
        "no DRM fdinfo VRAM figure for this process: "
        "no amdgpu fdinfo record of this device parsed")
    assert selftest._fdinfo_reason(fdinfo(900)) == (
        "fdinfo read 900 MiB; the worker rejected it as implausible")
    assert selftest._free_tier_reason(fdinfo(None), "amdgpu-sysfs") == (
        "no amdgpu sysfs: no GPU resolved for this device")


def test_selftest_reads_free_until_it_settles():
    """On a discrete amdgpu GPU free is reread from sysfs until it holds for
    2 s; a change or a failed read restarts the hold, and a figure that never
    holds stops at the read bound. A unified GPU or any other source is read
    once."""
    reads = iter([1500, None, None] + [2000] * 9)
    memory = types.SimpleNamespace(
        free_total_mb=lambda: (1000, 24576, "amdgpu-sysfs"),
        _free_mb=lambda source: (lambda free: (
            free, None if free is None else source))(next(reads)),
        _unified_gpu=lambda: False)
    sleeps = []
    assert selftest.settled_free_mb(memory, sleeps.append) == (
        2000, "amdgpu-sysfs", 3.0, True)
    assert sleeps == [0.25] * 12
    reads = iter(range(1001, 2000))
    assert selftest.settled_free_mb(memory, lambda s: None) == (
        1040, "amdgpu-sysfs", 10.0, False)
    reads = iter([None] * 9)
    assert selftest.settled_free_mb(memory, lambda s: None, reads=9) == (
        None, None, 2.25, False)
    reads = iter([1000] * 8)
    assert selftest.settled_free_mb(memory, lambda s: None) == (
        1000, "amdgpu-sysfs", 2.0, True)
    for source, unified in (("nvml", False), ("amdgpu-sysfs", True)):
        reads = iter([1000, 2000])
        memory = types.SimpleNamespace(
            free_total_mb=lambda: (next(reads), 4096, source),
            _unified_gpu=lambda: unified)
        assert selftest.settled_free_mb(memory, sleeps.append) == (
            1000, source, None, None)
        assert next(reads) == 2000


def test_newrun_records_the_gpu_nodes(tmp_path):
    host = Host(tmp_path).gpu(1, 0x0300).gpu(2, 0x0C00, openable=False)
    facts = newrun.rocm_facts(host.roots, module=str(tmp_path / "absent"))
    assert facts["amdgpu_version"] is None
    first, second = facts["gpu_nodes"]
    assert (first["bdf"], first["index"], first["gpu_id"], first["openable"]) == (
        BDF_03, 0, 1001, True)
    assert first["mem_info"]["mem_info_vram_total"] == 24 * GIB
    assert (second["index"], second["key"], second["openable"]) == (None, None, False)
    assert newrun.rocm_facts(rocm_sysfs.Roots(kfd=str(tmp_path / "none"))) is None
