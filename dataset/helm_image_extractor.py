#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step1-4 pipeline for Helm repos listed in CSVs:
  Step1: clone repo -> find Helm charts -> helm template to manifests
  Step2: parse manifests -> extract container startup chain -> derive daemon/one-shot entry executables
  Step3: docker pull + extract binaries/scripts from image (EPScan-like, binary-level)
  Step4: map extracted binaries to repo "code entrypoints" by strings-matching candidate main file paths

Outputs:
  - JSONL report per repo
  - CSV summary

Notes:
  - No "conservative syscall inference" is performed here.
  - Entry executables are derived deterministically from the startup chain and scripts (exec/&& structure).
  - Binary->code mapping uses strings against repo candidate main file paths (EPScan core idea).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import yaml  # PyYAML
except Exception:
    print("Missing dependency: pyyaml. Install with: pip install pyyaml", file=sys.stderr)
    raise


# ---------------------------
# Utils: subprocess
# ---------------------------

def run(cmd: List[str], cwd: Optional[Path] = None, timeout: int = 900) -> Tuple[int, str, str]:
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    return p.returncode, p.stdout, p.stderr


def must_have_tool(tool: str) -> None:
    if shutil.which(tool) is None:
        raise RuntimeError(f"Required tool not found in PATH: {tool}")


# ---------------------------
# Step0: read repos from CSV
# ---------------------------

def guess_repo_url_from_row(row: Dict[str, str]) -> Optional[str]:
    candidates = [
        "repo", "repo_url", "repository", "repository_url", "url", "git_url",
        "html_url", "web_url", "clone_url", "http_url", "ssh_url",
        "project_url", "source", "link"
    ]
    for k in candidates:
        if k in row and row[k].strip():
            return row[k].strip()
    for v in row.values():
        if not v:
            continue
        v = v.strip()
        if v.startswith("http://") or v.startswith("https://") or v.startswith("git@"):
            return v
    return None


def safe_repo_dir_name(url: str) -> str:
    u = url.strip().rstrip("/")
    if u.endswith(".git"):
        u = u[:-4]
    parts = re.split(r"[/:]", u)
    parts = [p for p in parts if p]
    tail = "_".join(parts[-2:]) if len(parts) >= 2 else "_".join(parts[-1:])
    tail = re.sub(r"[^A-Za-z0-9._-]+", "_", tail)
    return tail[:180] if tail else "repo"


def read_repo_urls(csv_paths: List[Path], limit: int = 0) -> List[Tuple[str, str]]:
    seen = set()
    out: List[Tuple[str, str]] = []
    for cp in csv_paths:
        with cp.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = guess_repo_url_from_row(row)
                if not url:
                    continue
                if url in seen:
                    continue
                seen.add(url)
                out.append((str(cp), url))
                if limit and len(out) >= limit:
                    return out
    return out


# ---------------------------
# Repo health + clone/skip
# ---------------------------

def git_ok(repo_dir: Path) -> bool:
    return (repo_dir / ".git").exists()


def has_non_git_files(repo_dir: Path) -> bool:
    if not repo_dir.exists() or not repo_dir.is_dir():
        return False
    for p in repo_dir.iterdir():
        if p.name == ".git":
            continue
        return True
    return False


def repo_health_check(repo_dir: Path) -> Tuple[bool, str]:
    if not repo_dir.exists():
        return False, "missing_dir"
    if not git_ok(repo_dir):
        return False, "missing_.git"
    code, out, err = run(["git", "rev-parse", "--verify", "HEAD"], cwd=repo_dir, timeout=120)
    if code != 0:
        return False, f"no_HEAD: {err.strip()[:200]}"
    code, out, err = run(["git", "ls-files"], cwd=repo_dir, timeout=120)
    if code != 0:
        return False, f"ls-files_failed: {err.strip()[:200]}"
    if not out.strip():
        return False, "no_tracked_files"
    if not has_non_git_files(repo_dir):
        return False, "empty_worktree_only_.git"
    return True, "healthy"


def try_repair_repo(repo_dir: Path) -> Tuple[bool, str]:
    if not git_ok(repo_dir):
        return False, "repair_skipped_missing_.git"
    run(["git", "fetch", "--all", "--prune"], cwd=repo_dir, timeout=900)
    run(["git", "reset", "--hard", "HEAD"], cwd=repo_dir, timeout=300)
    code, out, err = run(["git", "symbolic-ref", "refs/remotes/origin/HEAD"], cwd=repo_dir, timeout=120)
    if code == 0 and out.strip():
        ref = out.strip()
        branch = ref.split("/")[-1]
        run(["git", "checkout", "-f", branch], cwd=repo_dir, timeout=300)
        run(["git", "reset", "--hard", f"origin/{branch}"], cwd=repo_dir, timeout=300)
    ok, msg = repo_health_check(repo_dir)
    return ok, f"repaired:{msg}" if ok else f"repair_failed:{msg}"


def clone_repo(repo_url: str, dest: Path) -> Tuple[bool, str]:
    if dest.exists():
        shutil.rmtree(dest, ignore_errors=True)
    dest.parent.mkdir(parents=True, exist_ok=True)
    code, out, err = run(["git", "clone", "--depth", "1", "--no-tags", repo_url, str(dest)], timeout=1200)
    if code != 0:
        return False, f"clone_failed: {err.strip()[:400]}"
    ok, msg = repo_health_check(dest)
    if not ok:
        rok, rmsg = try_repair_repo(dest)
        if not rok:
            return False, f"clone_unhealthy: {msg}; {rmsg}"
        return True, f"cloned_then_{rmsg}"
    return True, "cloned"


def ensure_repo(repo_url: str, dest: Path, skip_if_healthy: bool = True) -> Tuple[bool, str, str]:
    """
    Returns (ok, action, detail)
      action: skipped_existing | repaired_existing | recloned | cloned | failed
    """
    if dest.exists() and git_ok(dest):
        ok, msg = repo_health_check(dest)
        if ok and skip_if_healthy:
            return True, "skipped_existing", msg
        rok, rmsg = try_repair_repo(dest)
        if rok:
            return True, "repaired_existing", rmsg
        ok2, cmsg = clone_repo(repo_url, dest)
        return (ok2, "recloned" if ok2 else "failed", cmsg)

    if dest.exists() and not git_ok(dest):
        shutil.rmtree(dest, ignore_errors=True)

    ok, cmsg = clone_repo(repo_url, dest)
    return (ok, "cloned" if ok else "failed", cmsg)


# ---------------------------
# Step1: find charts + helm template
# ---------------------------

def find_chart_dirs(repo_dir: Path) -> List[Path]:
    charts = [p.parent for p in repo_dir.rglob("Chart.yaml")]
    return sorted(set(charts), key=lambda x: str(x))


def helm_template(chart_dir: Path, out_dir: Path, release: str = "test", namespace: str = "default",
                  values_file: Optional[Path] = None) -> Tuple[bool, Path, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_yaml = out_dir / "rendered.yaml"
    
    # Try to build helm dependencies if Chart.lock or charts/ is missing
    chart_yaml = chart_dir / "Chart.yaml"
    if chart_yaml.exists():
        code, _, err = run(["helm", "dependency", "build", str(chart_dir)], timeout=600)
        if code != 0:
            # Dependency build failed, but continue anyway (chart might not have dependencies)
            pass
    
    # Try 3 strategies (fallback chain)
    strategies = [
        # 1. Default: with provided values file
        (["helm", "template", release, str(chart_dir), "--namespace", namespace] + 
         (["-f", str(values_file)] if values_file and values_file.exists() else [])),
        # 2. Fallback: no values file, plain template
        (["helm", "template", release, str(chart_dir), "--namespace", namespace]),
        # 3. Last resort: set some common empty values to avoid nil pointer
        (["helm", "template", release, str(chart_dir), "--namespace", namespace,
          "--set", "global.restapi.jvm.maxheapmemory=''",
          "--set", "global.restapi.jvm.minheapmemory=''",
          "--set", "Values={}"]),
    ]
    
    for attempt, cmd in enumerate(strategies, 1):
        code, stdout, stderr = run(cmd, timeout=900)
        if code == 0 and stdout.strip():
            out_yaml.write_text(stdout, encoding="utf-8")
            return True, out_yaml, f"ok_strategy_{attempt}"
    
    # All strategies failed
    last_err = stderr.strip()[:300]
    return False, out_yaml, f"helm_template_all_strategies_failed: {last_err}"


# ---------------------------
# YAML parsing helpers (workloads -> podSpec)
# ---------------------------

WORKLOAD_KINDS = {
    ("apps/v1", "Deployment"),
    ("apps/v1", "StatefulSet"),
    ("apps/v1", "DaemonSet"),
    ("batch/v1", "Job"),
    ("batch/v1", "CronJob"),
    ("v1", "Pod"),
}

def iter_yaml_docs(yaml_path: Path) -> Iterable[Dict[str, Any]]:
    text = yaml_path.read_text(encoding="utf-8", errors="ignore")
    for doc in yaml.safe_load_all(text):
        if isinstance(doc, dict):
            yield doc


def get_podspec(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    api = obj.get("apiVersion", "")
    kind = obj.get("kind", "")
    if (api, kind) not in WORKLOAD_KINDS:
        return None

    if kind == "Pod":
        return obj.get("spec", None)

    spec = obj.get("spec", {}) or {}
    if kind == "CronJob":
        # spec.jobTemplate.spec.template.spec
        jt = spec.get("jobTemplate", {}).get("spec", {})
        tpl = jt.get("template", {}).get("spec", {})
        return tpl or None

    # Deployment/StatefulSet/DaemonSet/Job: spec.template.spec
    tpl = spec.get("template", {}).get("spec", {})
    return tpl or None


def obj_meta_name(obj: Dict[str, Any]) -> str:
    md = obj.get("metadata", {}) or {}
    return md.get("name") or md.get("generateName") or ""


# ---------------------------
# Step2: startup chain -> daemon/one-shot executables
# ---------------------------

SHELLS = {"sh", "bash", "/bin/sh", "/bin/bash"}

@dataclass
class ExecEntry:
    exe: str
    kind: str            # daemon | one-shot
    pid1: bool
    always: bool
    evidence: str
    condition: str = ""


def normalize_command(command: Optional[List[str]], args: Optional[List[str]]) -> List[str]:
    """
    K8s semantics:
      - if command is set, it overrides image ENTRYPOINT
      - if args is set, it overrides image CMD
    Here (Step2) we only use manifest command/args; Step3 may enrich from image config.
    """
    cmd = []
    if command:
        cmd += [str(x) for x in command if x is not None]
    if args:
        cmd += [str(x) for x in args if x is not None]
    return cmd


def parse_shell_c_string(cstr: str) -> List[ExecEntry]:
    """
    Deterministic extraction based on structural cues:
      - 'exec <prog> ...' at end => daemon
      - '<prog> ... && exec <next>' => one-shot then daemon
      - 'if ...; then <prog>; fi' => conditional one-shot
    This is intentionally conservative about *what is executed* (only what is syntactically explicit),
    but NOT "syscall conservative inference". It does not guess syscalls.
    """
    entries: List[ExecEntry] = []
    lines = [ln.strip() for ln in cstr.splitlines() if ln.strip() and not ln.strip().startswith("#")]

    # join lines for some patterns
    joined = " ; ".join(lines)

    # Pattern A: if condition one-shot
    # very simple: if ...; then <cmd>; fi
    for m in re.finditer(r"\bif\b.*?\bthen\b\s*([^;]+?)\s*;\s*fi\b", joined):
        cmd = m.group(1).strip()
        prog = cmd.split()[0]
        if prog:
            entries.append(ExecEntry(
                exe=prog, kind="one-shot", pid1=False, always=False,
                evidence=f"if-then: {cmd}", condition="if-then"
            ))

    # Pattern B: chain with && exec
    # e.g. setup && exec /app/server --x
    m = re.search(r"(.+?)\s*&&\s*exec\s+([^\s;]+)", joined)
    if m:
        left = m.group(1).strip()
        right_prog = m.group(2).strip()
        # left-most program as one-shot candidate
        left_prog = left.split()[0] if left else ""
        if left_prog and left_prog not in {"set", "export"}:
            entries.append(ExecEntry(
                exe=left_prog, kind="one-shot", pid1=False, always=True,
                evidence=f"chain: {left} && exec {right_prog}"
            ))
        entries.append(ExecEntry(
            exe=right_prog, kind="daemon", pid1=True, always=True,
            evidence=f"exec: exec {right_prog}"
        ))
        return dedup_entries(entries)

    # Pattern C: final exec
    m = re.search(r"\bexec\s+([^\s;]+)", joined)
    if m:
        prog = m.group(1).strip()
        entries.append(ExecEntry(exe=prog, kind="daemon", pid1=True, always=True, evidence=f"exec: {m.group(0).strip()}"))
        return dedup_entries(entries)

    # Pattern D: otherwise, take first obvious program token on each line (best-effort)
    # Not guessing; only tokens that look like path or bareword.
    for ln in lines:
        toks = ln.split()
        if not toks:
            continue
        prog = toks[0]
        if prog in {"set", "export", "cd"}:
            continue
        # treat as one-shot unless it is the last line and has no trailing separators
        entries.append(ExecEntry(exe=prog, kind="one-shot", pid1=False, always=True, evidence=f"line: {ln}"))

    return dedup_entries(entries)


def dedup_entries(entries: List[ExecEntry]) -> List[ExecEntry]:
    seen = set()
    out = []
    for e in entries:
        key = (e.exe, e.kind, e.pid1, e.always, e.condition)
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def format_entries_for_log(entries: List[ExecEntry], max_items: int = 5) -> str:
    if not entries:
        return "[]"
    parts: List[str] = []
    for e in entries[:max_items]:
        parts.append(f"{e.exe}({e.kind},pid1={str(e.pid1).lower()},always={str(e.always).lower()})")
    if len(entries) > max_items:
        parts.append(f"...+{len(entries) - max_items}")
    return "[" + ", ".join(parts) + "]"


def derive_entries_from_container_spec(container: Dict[str, Any], image: str = "") -> Tuple[List[str], List[ExecEntry]]:
    """
    Step2: derive executables using manifest-only command/args.
    If manifest has no command, try to fetch from image config (Step3 enrichment).
    Step3 will later enrich with image entrypoint/CMD and extract scripts content.
    """
    command = container.get("command")
    args = container.get("args")
    cmdline = normalize_command(command, args)
    if not cmdline:
        # Try to get entrypoint from image config if available
        if image:
            entry, cmd, msg = docker_image_config(image)
            if entry or cmd:
                cmdline = normalize_command(entry, cmd)
            # Debug: log why we couldn't get image config
            # (Note: only log on failure to avoid spam)
        
        if not cmdline:
            return [], []

    exe_entries: List[ExecEntry] = []

    # handle sh -c
    if cmdline and cmdline[0] in SHELLS and len(cmdline) >= 3 and cmdline[1] == "-c":
        cstr = " ".join(cmdline[2:]).strip()
        exe_entries = parse_shell_c_string(cstr)
        return cmdline, exe_entries

    # direct exec
    if cmdline:
        prog = cmdline[0]
        exe_entries = [ExecEntry(exe=prog, kind="daemon", pid1=True, always=True, evidence=f"direct: {' '.join(cmdline)}")]
    return cmdline, exe_entries


# ---------------------------
# Step3: Docker image extract
# ---------------------------

def docker_pull(image: str, timeout: int = 300) -> Tuple[bool, str]:
    code, out, err = run(["docker", "pull", image], timeout=timeout)
    if code != 0:
        return False, err.strip()[:500]
    return True, "ok"


def is_private_registry_image(image: str) -> bool:
    """
    Heuristic: if first path segment contains '.' or ':' and is not docker hub aliases,
    treat it as private/custom registry.
    """
    if not image:
        return False
    # Images without explicit registry prefix (no slash) are docker hub library images.
    if "/" not in image:
        return False

    first = image.split("/", 1)[0]
    docker_hub_aliases = {"docker.io", "index.docker.io", "registry-1.docker.io"}
    if first in docker_hub_aliases:
        return False
    return "." in first or ":" in first


def docker_image_config(image: str) -> Tuple[Optional[List[str]], Optional[List[str]], str]:
    """
    Returns (Entrypoint, Cmd, msg)
    """
    try:
        code, out, err = run(["docker", "image", "inspect", image, "--format", "{{json .Config.Entrypoint}} {{json .Config.Cmd}}"], timeout=120)
        if code != 0:
            return None, None, f"docker_inspect_failed: {err.strip()[:200]}"
        parts = out.strip().split(" ", 1)
        if len(parts) != 2:
            return None, None, f"unexpected_format: {out.strip()[:120]}"
        try:
            entry = json.loads(parts[0])
            cmd = json.loads(parts[1])
            # Handle null or empty values
            if not isinstance(entry, list):
                entry = None
            if not isinstance(cmd, list):
                cmd = None
            return entry, cmd, "ok"
        except json.JSONDecodeError as e:
            return None, None, f"json_parse_failed: {e}"
    except Exception as e:
        return None, None, f"exception: {str(e)[:200]}"


def docker_create(image: str) -> Tuple[Optional[str], str]:
    code, out, err = run(["docker", "create", image], timeout=120)
    if code != 0:
        return None, err.strip()[:300]
    cid = out.strip()
    return cid, "ok"


def docker_rm(cid: str) -> None:
    run(["docker", "rm", "-f", cid], timeout=60)


def docker_cp_from_container(cid: str, src_path: str, dst_path: Path) -> Tuple[bool, str]:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    code, out, err = run(["docker", "cp", f"{cid}:{src_path}", str(dst_path)], timeout=120)
    if code != 0:
        return False, err.strip()[:300]
    return True, "ok"


def docker_resolve_which(image: str, prog: str) -> Optional[str]:
    """
    Resolve a bareword executable name to an absolute path using `which` inside container.
    """
    code, out, err = run(["docker", "run", "--rm", "--entrypoint", "which", image, prog], timeout=120)
    if code != 0:
        return None
    p = out.strip().splitlines()[-1].strip() if out.strip() else ""
    return p if p.startswith("/") else None


def resolve_exe_path(image: str, exe: str) -> Optional[str]:
    if exe.startswith("/"):
        return exe
    # try which
    return docker_resolve_which(image, exe)


# ---------------------------
# Step4: map binary -> code entrypoints (EPScan-like)
# ---------------------------

MAIN_FUNC_RE = re.compile(r"^\s*func\s+main\s*\(\s*\)\s*{", re.MULTILINE)
PKG_MAIN_RE = re.compile(r"^\s*package\s+main\s*$", re.MULTILINE)

def scan_repo_go_entrypoints(repo_dir: Path, max_files: int = 8000) -> List[Path]:
    """
    Lightweight (no CodeQL) scan:
      - file is *.go
      - contains 'package main'
      - contains 'func main() {'
    Returns candidate main source files (Paths).
    """
    out = []
    count = 0
    for p in repo_dir.rglob("*.go"):
        count += 1
        if count > max_files:
            break
        try:
            txt = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if PKG_MAIN_RE.search(txt) and MAIN_FUNC_RE.search(txt):
            out.append(p)
    return sorted(set(out), key=lambda x: str(x))


def strings_match_source_paths(binary_path: Path, repo_dir: Path, candidate_main_files: List[Path]) -> List[str]:
    """
    EPScan core trick: Go binaries embed source file paths; use `strings` to match.
    We match relative paths (repo-relative) and also basename-only as weaker signal.
    """
    if not candidate_main_files:
        return []

    rels = [str(p.relative_to(repo_dir)) for p in candidate_main_files if p.exists()]
    # Escape for regex
    escaped = [re.escape(r) for r in rels]
    if not escaped:
        return []

    # Big regex (chunk if too large)
    matched: List[str] = []
    try:
        # strings -a: scan whole file; -n 6: min len
        code, out, err = run(["strings", "-a", "-n", "6", str(binary_path)], timeout=120)
        if code != 0:
            return []
        for line in out.splitlines():
            # direct contains check is faster than giant regex
            for r in rels:
                if r in line:
                    matched.append(r)
        return sorted(set(matched))
    except Exception:
        return sorted(set(matched))


# ---------------------------
# Reports
# ---------------------------

@dataclass
class ContainerResult:
    workload: str
    kind: str
    container_name: str
    image: str
    manifest_cmdline: str
    derived_entries: List[Dict[str, Any]]
    extracted_binaries: List[str]
    matched_code_entrypoints: List[str]
    notes: str


@dataclass
class RepoResult:
    source_csv: str
    repo_url: str
    local_dir: str
    repo_ok: bool
    repo_action: str
    repo_detail: str
    charts_found: List[str]
    rendered_manifests: List[str]
    containers: List[Dict[str, Any]]
    errors: List[str]


# ---------------------------
# Pipeline per repo
# ---------------------------

def analyze_repo(repo_dir: Path, source_csv: str, repo_url: str,
                 out_dir: Path, namespace: str = "default", no_docker: bool = False,
                 skip_private_registry: bool = True, pull_timeout: int = 300) -> RepoResult:

    charts = find_chart_dirs(repo_dir)
    print(f"  [DEBUG] Found {len(charts)} charts")
    rendered_paths: List[str] = []
    container_results: List[Dict[str, Any]] = []
    errors: List[str] = []

    # Step4 prep: scan repo for Go entrypoints
    candidate_mains = scan_repo_go_entrypoints(repo_dir)

    for chart_dir in charts:
        # Step1: helm template (default values only; you can extend to multiple profiles later)
        chart_out = out_dir / "rendered" / chart_dir.relative_to(repo_dir)
        ok, rendered_yaml, msg = helm_template(chart_dir, chart_out, release="test", namespace=namespace, values_file=None)
        if not ok:
            render_err = f"helm_template_failed: {chart_dir}: {msg}"
            print(f"  [WARN] {render_err}")
            errors.append(render_err)
            continue
        rendered_paths.append(str(rendered_yaml))
        print(f"  [DEBUG] Rendered: {rendered_yaml}")

        # Step2: parse manifests
        obj_count = 0
        podspec_count = 0
        for obj in iter_yaml_docs(rendered_yaml):
            obj_count += 1
            podspec = get_podspec(obj)
            if not podspec:
                continue
            podspec_count += 1
            wname = obj_meta_name(obj)
            wkind = obj.get("kind", "")

            # containers + initContainers
            for section in ["initContainers", "containers"]:
                for c in (podspec.get(section, []) or []):
                    cname = c.get("name", "")
                    image = c.get("image", "")
                    cmdline, entries = derive_entries_from_container_spec(c, image)

                    # Step3 + Step4: extract binaries and match to code entrypoints
                    extracted_bins: List[str] = []
                    matched_entrypoints: List[str] = []
                    notes = ""

                    if image and not no_docker:
                        if skip_private_registry and is_private_registry_image(image):
                            notes = "private_registry_skipped"
                            print(f"    [INFO] skip private image: {image}")
                        else:
                            print(f"    [DEBUG] pulling image: {image}")
                            pull_ok, pull_msg = docker_pull(image, timeout=pull_timeout)
                            if not pull_ok:
                                notes = f"docker_pull_failed: {pull_msg}"
                            else:
                                # Create temp container for docker cp
                                cid, cmsg = docker_create(image)
                                if not cid:
                                    notes = f"docker_create_failed: {cmsg}"
                                else:
                                    try:
                                        # Try to extract each entry executable
                                        for e in entries:
                                            exe = e.exe
                                            exe_path = resolve_exe_path(image, exe)
                                            if not exe_path:
                                                continue
                                            # extract
                                            local_bin = out_dir / "binaries" / safe_repo_dir_name(repo_url) / wkind / wname / section / cname / Path(exe_path).name
                                            okcp, cpmsg = docker_cp_from_container(cid, exe_path, local_bin)
                                            if not okcp:
                                                continue
                                            extracted_bins.append(str(local_bin))
                                            # Step4: strings match to repo main files
                                            matches = strings_match_source_paths(local_bin, repo_dir, candidate_mains)
                                            if matches:
                                                matched_entrypoints.extend(matches)
                                    finally:
                                        docker_rm(cid)
                    elif no_docker:
                        notes = "docker_skipped"

                    container_results.append(asdict(ContainerResult(
                        workload=wname,
                        kind=wkind,
                        container_name=f"{section}:{cname}",
                        image=image,
                        manifest_cmdline=" ".join(cmdline),
                        derived_entries=[asdict(e) for e in entries],
                        extracted_binaries=extracted_bins,
                        matched_code_entrypoints=sorted(set(matched_entrypoints)),
                        notes=notes
                    )))
                    print(
                        f"    [DEBUG] {section}:{cname} | image={image} | "
                        f"entries={len(entries)} {format_entries_for_log(entries)} | "
                        f"bins={len(extracted_bins)} | matches={len(matched_entrypoints)}"
                    )

        if obj_count == 0:
            print(f"    [WARN] No K8s objects found in rendered manifest")
        else:
            print(f"    [DEBUG] Parsed {obj_count} K8s objects, {podspec_count} with podspec")

    return RepoResult(
        source_csv=source_csv,
        repo_url=repo_url,
        local_dir=str(repo_dir),
        repo_ok=True,
        repo_action="",
        repo_detail="",
        charts_found=[str(p.relative_to(repo_dir)) for p in charts],
        rendered_manifests=rendered_paths,
        containers=container_results,
        errors=errors
    )


# ---------------------------
# Main
# ---------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", nargs="+", required=True, help="CSV paths containing repo links")
    ap.add_argument("--workdir", required=True, help="Directory to clone repos into")
    ap.add_argument("--outdir", required=True, help="Directory to write outputs")
    ap.add_argument("--max", type=int, default=0, help="Max repos to process (0 = no limit)")
    ap.add_argument("--skip-existing", action="store_true", help="Skip local repo if healthy")
    ap.add_argument("--namespace", default="default", help="Namespace for helm template rendering")
    ap.add_argument("--no-docker", action="store_true", help="Skip Docker image extraction (Step3)")
    ap.add_argument("--pull-timeout", type=int, default=300, help="Timeout (seconds) for docker pull")
    ap.add_argument("--include-private-registry", action="store_true",
                    help="Include private/custom registry images (default: skip to avoid long waits)")
    args = ap.parse_args()

    # Tools required
    must_have_tool("git")
    must_have_tool("helm")
    if not args.no_docker:
        must_have_tool("docker")
    must_have_tool("strings")

    csv_paths = [Path(p) for p in args.csv]
    for p in csv_paths:
        if not p.exists():
            print(f"[!] CSV not found: {p}", file=sys.stderr)
            return 2

    workdir = Path(args.workdir); workdir.mkdir(parents=True, exist_ok=True)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    repos = read_repo_urls(csv_paths, limit=args.max or 0)
    print(f"[*] Loaded {len(repos)} unique repo URLs")

    jsonl_path = outdir / "step1_4_report.jsonl"
    csv_summary_path = outdir / "step1_4_summary.csv"

    summary_rows: List[Dict[str, Any]] = []

    with jsonl_path.open("w", encoding="utf-8") as jf:
        for idx, (src, url) in enumerate(repos, start=1):
            repo_name = safe_repo_dir_name(url)
            repo_dir = workdir / repo_name
            print(f"[{idx}/{len(repos)}] {url}")

            ok, action, detail = ensure_repo(url, repo_dir, skip_if_healthy=args.skip_existing)
            if not ok:
                rr = RepoResult(
                    source_csv=src, repo_url=url, local_dir=str(repo_dir),
                    repo_ok=False, repo_action=action, repo_detail=detail,
                    charts_found=[], rendered_manifests=[], containers=[], errors=[detail]
                )
                jf.write(json.dumps(asdict(rr), ensure_ascii=False) + "\n")
                summary_rows.append({
                    "repo_url": url,
                    "repo_ok": False,
                    "repo_action": action,
                    "repo_detail": detail,
                    "charts_found": 0,
                    "containers_parsed": 0,
                    "binaries_extracted": 0,
                    "entrypoints_matched": 0,
                    "errors": detail
                })
                continue

            repo_out = outdir / "repos" / repo_name
            repo_out.mkdir(parents=True, exist_ok=True)

            rr = analyze_repo(
                repo_dir,
                src,
                url,
                repo_out,
                namespace=args.namespace,
                no_docker=args.no_docker,
                skip_private_registry=not args.include_private_registry,
                pull_timeout=args.pull_timeout,
            )
            rr.repo_ok = True
            rr.repo_action = action
            rr.repo_detail = detail

            jf.write(json.dumps(asdict(rr), ensure_ascii=False) + "\n")

            binaries_extracted = sum(len(c["extracted_binaries"]) for c in rr.containers)
            entrypoints_matched = sum(1 for c in rr.containers if c["matched_code_entrypoints"])
            summary_rows.append({
                "repo_url": url,
                "repo_ok": True,
                "repo_action": action,
                "repo_detail": detail,
                "charts_found": len(rr.charts_found),
                "containers_parsed": len(rr.containers),
                "binaries_extracted": binaries_extracted,
                "entrypoints_matched": entrypoints_matched,
                "errors": ";".join(rr.errors)
            })
            print(f"  [SUMMARY] charts={len(rr.charts_found)} containers={len(rr.containers)} bins={binaries_extracted} matches={entrypoints_matched}")

    # write summary CSV
    if summary_rows:
        with csv_summary_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
            w.writeheader()
            for r in summary_rows:
                w.writerow(r)

    print(f"[*] Wrote JSONL: {jsonl_path}")
    print(f"[*] Wrote CSV  : {csv_summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
