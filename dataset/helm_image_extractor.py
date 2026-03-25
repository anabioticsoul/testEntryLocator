#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Improved Step1-4 pipeline for Helm repos listed in CSVs:
Step1: clone repo -> find Helm charts -> helm template to manifests
Step2: parse manifests -> resolve container startup chain from K8s command/args + image Entrypoint/Cmd
Step3: docker pull + inspect image -> extract startup scripts/binaries from image (EPScan-inspired)
Step4: map extracted binaries to repo "code entrypoints" by strings-matching candidate main file paths

Key improvements over the previous version:
- Correct K8s command/args overriding semantics using both manifest and image config.
- Resolve startup chains hidden behind wrappers such as sh -c, bash -lc, tini, dumb-init, env, timeout, gosu.
- Extract and parse shell scripts from images recursively to recover final executables.
- Substitute environment variables from image env + manifest env to reduce false misses.
- More robust docker inspect parsing and executable path resolution.
- Richer debug notes so it is obvious where extraction fails.
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
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from collections import Counter

try:
    import yaml
except Exception:
    print("Missing dependency: pyyaml. Install with: pip install pyyaml", file=sys.stderr)
    raise

VERBOSE = False

def log_debug(msg: str) -> None:
    if VERBOSE:
        print(f"[DEBUG] {msg}")

def log_info(msg: str) -> None:
    if VERBOSE:
        print(f"[INFO] {msg}")

def log_warn(msg: str) -> None:
    if VERBOSE:
        print(f"[WARN] {msg}")

# ---------------------------
# Utils: subprocess
# ---------------------------

def run(cmd: List[str], cwd: Optional[Path] = None, timeout: int = 900) -> Tuple[int, str, str]:
    try:
        p = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as e:
        stdout = e.stdout if isinstance(e.stdout, str) else ""
        stderr = e.stderr if isinstance(e.stderr, str) else ""
        if stderr:
            stderr = f"{stderr}\n"
        stderr += f"command_timeout_after_{timeout}s: {' '.join(cmd)}"
        return 124, stdout, stderr


def must_have_tool(tool: str) -> None:
    if shutil.which(tool) is None:
        raise RuntimeError(f"Required tool not found in PATH: {tool}")


# ---------------------------
# Step0: read repos from CSV
# ---------------------------

def guess_repo_url_from_row(row: Dict[str, str]) -> Optional[str]:
    candidates = [
        "repo", "repo_url", "repository", "repository_url", "url", "git_url",
        "html_url", "web_url", "clone_url", "http_url", "ssh_url", "project_url",
        "source", "link"
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
                if not url or url in seen:
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
    code, _, err = run(["git", "rev-parse", "--verify", "HEAD"], cwd=repo_dir, timeout=120)
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
    code, out, _ = run(["git", "symbolic-ref", "refs/remotes/origin/HEAD"], cwd=repo_dir, timeout=120)
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
    code, _, err = run(["git", "clone", "--depth", "1", "--no-tags", repo_url, str(dest)], timeout=1200)
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
    if dest.exists() and git_ok(dest):
        ok, msg = repo_health_check(dest)
        if ok and skip_if_healthy:
            return True, "skipped_existing", msg
        rok, rmsg = try_repair_repo(dest)
        if rok:
            return True, "repaired_existing", rmsg
        ok2, cmsg = clone_repo(repo_url, dest)
        return ok2, ("recloned" if ok2 else "failed"), cmsg
    if dest.exists() and not git_ok(dest):
        shutil.rmtree(dest, ignore_errors=True)
    ok, cmsg = clone_repo(repo_url, dest)
    return ok, ("cloned" if ok else "failed"), cmsg


# ---------------------------
# Step1: find charts + helm template
# ---------------------------

def find_chart_dirs(repo_dir: Path) -> List[Path]:
    charts = [p.parent for p in repo_dir.rglob("Chart.yaml")]
    return sorted(set(charts), key=lambda x: str(x))


def chart_has_dependencies(chart_dir: Path) -> bool:
    chart_yaml = chart_dir / "Chart.yaml"
    if not chart_yaml.exists():
        return False
    try:
        obj = yaml.safe_load(chart_yaml.read_text(encoding="utf-8", errors="ignore")) or {}
        deps = obj.get("dependencies")
        return isinstance(deps, list) and len(deps) > 0
    except Exception:
        return False


def extract_values_paths_from_helm_error(stderr: str) -> List[str]:
    paths: List[str] = []
    if not stderr:
        return paths
    for m in re.finditer(r"<\.Values\.([^>]+)>", stderr):
        raw = m.group(1).strip()
        if not raw:
            continue
        # Keep only helm-safe key path characters.
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "", raw)
        if safe:
            paths.append(safe)
    return list(dict.fromkeys(paths))


def build_nil_pointer_set_flags(value_paths: List[str], max_items: int = 20) -> List[str]:
    flags: List[str] = []
    for p in value_paths[:max_items]:
        flags.extend(["--set-string", f"{p}="])
    return flags


def default_helm_fallback_set_flags() -> List[str]:
    # Common missing keys found in this dataset.
    return [
        "--set-string", "global.restapi.jvm.maxheapmemory=",
        "--set-string", "global.restapi.jvm.minheapmemory=",
    ]


def helm_template(
    chart_dir: Path,
    out_dir: Path,
    release: str = "test",
    namespace: str = "default",
    values_file: Optional[Path] = None,
    template_timeout: int = 240,
    dependency_timeout: int = 90,
    skip_dependency_build: bool = False,
) -> Tuple[bool, Path, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_yaml = out_dir / "rendered.yaml"

    dep_note = ""
    base_cmd = ["helm", "template", release, str(chart_dir), "--namespace", namespace]
    if values_file and values_file.exists():
        base_cmd += ["-f", str(values_file)]

    if not skip_dependency_build and chart_has_dependencies(chart_dir):
        dep_cmd = ["helm", "dependency", "build", "--skip-refresh", str(chart_dir)]
        dep_code, _, dep_err = run(dep_cmd, timeout=dependency_timeout)
        if dep_code != 0:
            dep_note = f"; dependency_build_failed: {dep_err.strip()[:180]}"
            # Retry with refresh once for repos not cached locally.
            dep_cmd_refresh = ["helm", "dependency", "build", str(chart_dir)]
            dep_code2, _, dep_err2 = run(dep_cmd_refresh, timeout=max(dependency_timeout, 120))
            if dep_code2 == 0:
                dep_note += "; dependency_build_retry_ok"
            else:
                dep_note += f"; dependency_build_retry_failed: {dep_err2.strip()[:180]}"

    strategies = [
        base_cmd,
        ["helm", "template", release, str(chart_dir), "--namespace", namespace],
        ["helm", "template", release, str(chart_dir), "--namespace", namespace] + default_helm_fallback_set_flags(),
    ]
    last_err = ""
    nilptr_retry_signatures: Set[str] = set()
    nilptr_accumulated_paths: Set[str] = set()
    chart_name = chart_dir.name
    for attempt, cmd in enumerate(strategies, 1):
        code, stdout, stderr = run(cmd, timeout=template_timeout)
        if code == 0 and stdout.strip():
            out_yaml.write_text(stdout, encoding="utf-8")
            return True, out_yaml, f"ok_strategy_{attempt}{dep_note}"
        last_err = stderr.strip()[:300]

        if "nil pointer evaluating interface" in (stderr or ""):
            all_paths = extract_values_paths_from_helm_error(stderr or "")
            if all_paths:
                preferred_paths = [p for p in all_paths if f".{chart_name}." in f".{p}."]
                value_paths = preferred_paths or all_paths
                for p in value_paths:
                    nilptr_accumulated_paths.add(p)
                merged_paths = sorted(nilptr_accumulated_paths)
                signature = ",".join(merged_paths[:20])
                if signature and signature not in nilptr_retry_signatures:
                    nil_cmd = list(base_cmd) + default_helm_fallback_set_flags() + build_nil_pointer_set_flags(merged_paths)
                    strategies.append(nil_cmd)
                    nilptr_retry_signatures.add(signature)
                    dep_note += f"; nilptr_autoset_paths={','.join(merged_paths[:5])}"

    return False, out_yaml, f"helm_template_all_strategies_failed: {last_err}{dep_note}"


# ---------------------------
# YAML parsing helpers
# ---------------------------
WORKLOAD_KINDS = {
    ("apps/v1", "Deployment"),
    ("apps/v1", "StatefulSet"),
    ("apps/v1", "DaemonSet"),
    ("batch/v1", "Job"),
    ("batch/v1", "CronJob"),
    ("v1", "Pod"),
}

WORKLOAD_KIND_NAMES = {"Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob", "Pod"}


def iter_yaml_docs(yaml_path: Path) -> Iterable[Dict[str, Any]]:
    try:
        text = yaml_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return
    try:
        for doc in yaml.safe_load_all(text):
            if isinstance(doc, dict):
                yield doc
    except Exception:
        return


def get_podspec(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    api = obj.get("apiVersion", "")
    kind = obj.get("kind", "")
    if (api, kind) not in WORKLOAD_KINDS:
        return None
    if kind == "Pod":
        return obj.get("spec")
    spec = obj.get("spec", {}) or {}
    if kind == "CronJob":
        return spec.get("jobTemplate", {}).get("spec", {}).get("template", {}).get("spec")
    return spec.get("template", {}).get("spec")


def get_podspec_loose(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Fallback parser by kind name only, for non-standard/legacy apiVersion manifests."""
    kind = str(obj.get("kind") or "")
    if kind not in WORKLOAD_KIND_NAMES:
        return None
    if kind == "Pod":
        return obj.get("spec")
    spec = obj.get("spec", {}) or {}
    if kind == "CronJob":
        return spec.get("jobTemplate", {}).get("spec", {}).get("template", {}).get("spec")
    return spec.get("template", {}).get("spec")


def find_repo_manifest_yaml_files(repo_dir: Path, max_files: int = 5000) -> List[Path]:
    skip_dir_names = {
        ".git", ".venv", "venv", "node_modules", "vendor", "dist", "build", "target",
        "dataset", "outputs", "binaries", "scripts",
    }
    out: List[Path] = []
    seen: Set[Path] = set()
    patterns = ("*.yaml", "*.yml")
    for pattern in patterns:
        for p in repo_dir.rglob(pattern):
            if len(out) >= max_files:
                return sorted(out, key=lambda x: str(x))
            if p in seen:
                continue
            seen.add(p)
            try:
                rel_parts = p.relative_to(repo_dir).parts[:-1]
            except Exception:
                rel_parts = p.parts[:-1]
            if any(part in skip_dir_names for part in rel_parts):
                continue
            try:
                if p.stat().st_size > 1_500_000:
                    continue
            except Exception:
                continue
            out.append(p)
    return sorted(out, key=lambda x: str(x))


def obj_meta_name(obj: Dict[str, Any]) -> str:
    md = obj.get("metadata", {}) or {}
    return md.get("name") or md.get("generateName") or ""


# ---------------------------
# Step2: startup chain extraction
# ---------------------------
SHELLS = {"sh", "bash", "/bin/sh", "/bin/bash", "/busybox/sh"}
SCRIPT_EXTS = (".sh", ".bash", ".envsh")
WRAPPER_CMDS = {
    "tini", "/sbin/tini", "/usr/bin/tini", "dumb-init", "/usr/bin/dumb-init",
    "env", "/usr/bin/env", "timeout", "/usr/bin/timeout", "gosu", "/usr/sbin/gosu",
    "su-exec", "/sbin/su-exec", "chpst", "chroot", "nice", "ionice", "setsid",
}
SKIP_BUILTINS = {
    "set", "export", "cd", "echo", "printf", "test", "[", "[[", "]", "exec",
    "trap", "wait", "sleep", "true", "false", "mkdir", "rm", "cp", "mv", "cat",
    "sed", "awk", "grep", "find", "ln", "chmod", "chown", "umask", "read", "shift"
}
ENV_REF_RE = re.compile(r"\$(\{?[A-Za-z_][A-Za-z0-9_]*(:?[-?][^}]*)?\}?)")
SCRIPT_PATH_RE = re.compile(r"(?P<path>(?:/[^\s;|&]+|\./[^\s;|&]+|\.\./[^\s;|&]+|[^\s;|&]+\.(?:sh|bash)))")


@dataclass
class ExecEntry:
    exe: str
    kind: str
    pid1: bool
    always: bool
    evidence: str
    condition: str = ""
    source: str = "cmdline"


@dataclass
class StartupResolution:
    final_cmdline: List[str]
    entries: List[ExecEntry]
    scripts_considered: List[str]
    env_keys_used: List[str]
    notes: List[str]


# ---------------------------
# Docker helpers
# ---------------------------


def docker_image_exists_locally(image: str) -> bool:
    code, out, _ = run(["docker", "image", "inspect", image], timeout=60)
    return code == 0 and out.strip().startswith("[")


def docker_pull(image: str, timeout: int = 300) -> Tuple[bool, str]:
    # Try native pull first
    code, _, err = run(["docker", "pull", image], timeout=timeout)
    if code == 0:
        return True, "ok"
    
    err_msg = err.strip()
    # If it fails and mentions ARM64/manifest, try amd64 fallback
    if "arm64" in err_msg.lower() or "manifest" in err_msg.lower() or "not found" in err_msg.lower():
        log_debug(f"native pull failed ({err_msg[:100]}), trying amd64 fallback...")
        # Try with explicit platform
        code2, _, err2 = run(["docker", "pull", "--platform", "linux/amd64", image], timeout=timeout)
        if code2 == 0:
            return True, "ok_amd64_fallback"
        # Also return the amd64 error code if primary error was architecture-related
        if "arm64" in err_msg.lower() or "manifest" in err_msg.lower():
            # For architecture mismatches, we'll skip this image gracefully
            return False, f"architecture_mismatch: {err_msg[:300]}"
    
    return False, err_msg[:500]


def is_private_registry_image(image: str) -> bool:
    if not image:
        return False
    if "/" not in image:
        return False
    first = image.split("/", 1)[0]
    docker_hub_aliases = {"docker.io", "index.docker.io", "registry-1.docker.io"}
    if first in docker_hub_aliases:
        return False
    return "." in first or ":" in first


def docker_image_inspect(image: str) -> Tuple[Dict[str, Any], str]:
    code, out, err = run(["docker", "image", "inspect", image], timeout=120)
    if code != 0:
        return {}, f"docker_inspect_failed: {err.strip()[:200]}"
    try:
        arr = json.loads(out)
        if not arr or not isinstance(arr, list):
            return {}, "docker_inspect_empty"
        cfg = arr[0].get("Config", {}) or {}
        return cfg, "ok"
    except Exception as e:
        return {}, f"docker_inspect_parse_failed: {e}"


def docker_image_config(image: str) -> Tuple[Optional[List[str]], Optional[List[str]], Dict[str, str], str]:
    cfg, msg = docker_image_inspect(image)
    if not cfg:
        return None, None, {}, msg
    entry = cfg.get("Entrypoint")
    cmd = cfg.get("Cmd")
    envs: Dict[str, str] = {}
    for item in cfg.get("Env") or []:
        if not isinstance(item, str) or "=" not in item:
            continue
        k, v = item.split("=", 1)
        envs[k] = v
    if not isinstance(entry, list):
        entry = None
    if not isinstance(cmd, list):
        cmd = None
    return entry, cmd, envs, msg


def docker_create(image: str) -> Tuple[Optional[str], str]:
    code, out, err = run(["docker", "create", image], timeout=120)
    if code != 0:
        return None, err.strip()[:300]
    return out.strip(), "ok"


def docker_rm(cid: str) -> None:
    run(["docker", "rm", "-f", cid], timeout=60)


def docker_cp_from_container(cid: str, src_path: str, dst_path: Path) -> Tuple[bool, str]:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    code, _, err = run(["docker", "cp", f"{cid}:{src_path}", str(dst_path)], timeout=120)
    if code != 0:
        return False, err.strip()[:300]
    return True, "ok"


def docker_exec_sh(image: str, shell_cmd: str, timeout: int = 120) -> Tuple[int, str, str]:
    return run(["docker", "run", "--rm", "--entrypoint", "/bin/sh", image, "-c", shell_cmd], timeout=timeout)


def docker_resolve_executable(image: str, exe: str) -> Optional[str]:
    exe = exe.strip()
    if not exe:
        return None
    if exe.startswith("/"):
        code, out, _ = docker_exec_sh(image, f'test -e "{exe}" && printf "%s" "{exe}"')
        return out.strip() if code == 0 and out.strip().startswith("/") else None
    shell = (
        f'if command -v "{exe}" >/dev/null 2>&1; then command -v "{exe}"; '
        f'elif test -e "./{exe}"; then pwd; printf "/{exe}"; '
        f'elif test -e "{exe}"; then printf "%s" "{exe}"; fi'
    )
    code, out, _ = docker_exec_sh(image, shell)
    path = out.strip().splitlines()[-1].strip() if out.strip() else ""
    return path if path.startswith("/") else None


def docker_resolve_executables_batch(image: str, candidates: List[str], timeout: int = 180) -> Dict[str, str]:
    uniq: List[str] = []
    seen: Set[str] = set()
    for c in candidates:
        s = (c or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        uniq.append(s)
    if not uniq:
        return {}

    script = (
        'for c in "$@"; do '
        '  [ -z "$c" ] && continue; '
        '  case "$c" in '
        '    /*) if [ -e "$c" ]; then printf "%s\\t%s\\n" "$c" "$c"; fi ;; '
        '    *) '
        '      p=$(command -v "$c" 2>/dev/null || true); '
        '      if [ -n "$p" ]; then printf "%s\\t%s\\n" "$c" "$p"; continue; fi; '
        '      for d in /usr/local/bin /usr/bin /bin /usr/sbin /sbin; do '
        '        if [ -e "$d/$c" ]; then printf "%s\\t%s\\n" "$c" "$d/$c"; break; fi; '
        '      done ;; '
        '  esac; '
        'done'
    )
    code, out, _ = run(
        ["docker", "run", "--rm", "--entrypoint", "/bin/sh", image, "-c", script, "_"] + uniq,
        timeout=timeout,
    )
    if code != 0:
        return {}

    mapping: Dict[str, str] = {}
    for line in out.splitlines():
        if "\t" not in line:
            continue
        left, right = line.split("\t", 1)
        left = left.strip()
        right = right.strip()
        if left and right.startswith("/"):
            mapping[left] = right
    return mapping


# ---------------------------
# Command / script parsing helpers
# ---------------------------

def parse_container_env(container: Dict[str, Any], base_env: Dict[str, str]) -> Dict[str, str]:
    env = dict(base_env)
    for item in container.get("env", []) or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        if "value" in item and item.get("value") is not None:
            env[name] = str(item.get("value"))
        else:
            env.setdefault(name, "")
    return env


def substitute_env(text: str, env: Dict[str, str]) -> Tuple[str, Set[str]]:
    used: Set[str] = set()

    def repl(m: re.Match[str]) -> str:
        token = m.group(1)
        raw = token
        token = token.strip("{}")
        token = token.split(":", 1)[0]
        token = token.split("-", 1)[0]
        token = token.split("?", 1)[0]
        used.add(token)
        return env.get(token, m.group(0) if raw.startswith("{") else env.get(token, m.group(0)))

    return ENV_REF_RE.sub(repl, text), used


def is_shell_script_path(token: str) -> bool:
    token = token.strip().strip('"\'')
    if not token:
        return False
    if token.endswith(SCRIPT_EXTS):
        return True
    return token.startswith("/") and ("/bin/" not in token or token.endswith(".sh")) and token.count("/") >= 2 and "." in Path(token).name


def is_executable_like_token(tok: str) -> bool:
    t = (tok or "").strip().strip('"\'')
    if not t:
        return False
    if t in SKIP_BUILTINS:
        return False
    if t.startswith("$") or t.startswith("`"):
        return False
    if t.endswith("()"):
        return False
    if "=" in t and not t.startswith("/"):
        return False
    if any(x in t for x in ["(", ")", "{", "}"]):
        return False
    return True


def select_entries_for_extraction(entries: List[ExecEntry], max_items: int = 24) -> List[ExecEntry]:
    if not entries:
        return []

    def rank(e: ExecEntry) -> Tuple[int, int, int]:
        return (
            0 if e.kind == "daemon" else 1,
            0 if e.pid1 else 1,
            0 if e.exe.startswith("/") else 1,
        )

    selected: List[ExecEntry] = []
    seen_exe: Set[str] = set()
    for e in sorted(entries, key=rank):
        exe = (e.exe or "").strip()
        if not is_executable_like_token(exe):
            continue
        if exe in seen_exe:
            continue
        seen_exe.add(exe)
        selected.append(e)
        if len(selected) >= max_items:
            break
    return selected


def dedup_entries(entries: List[ExecEntry]) -> List[ExecEntry]:
    seen = set()
    out: List[ExecEntry] = []
    for e in entries:
        key = (e.exe, e.kind, e.pid1, e.always, e.condition, e.source)
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def format_entries_for_log(entries: List[ExecEntry], max_items: int = 5) -> str:
    if not entries:
        return "[]"
    parts = []
    for e in entries[:max_items]:
        parts.append(f"{e.exe}({e.kind},pid1={str(e.pid1).lower()},src={e.source})")
    if len(entries) > max_items:
        parts.append(f"...+{len(entries) - max_items}")
    return "[" + ", ".join(parts) + "]"


def normalize_full_command(manifest_command: Optional[List[str]], manifest_args: Optional[List[str]], image_entry: Optional[List[str]], image_cmd: Optional[List[str]]) -> List[str]:
    if manifest_command is not None and len(manifest_command) > 0:
        return [str(x) for x in manifest_command] + ([str(x) for x in manifest_args] if manifest_args else [])
    if manifest_args is not None and len(manifest_args) > 0:
        return ([str(x) for x in image_entry] if image_entry else []) + [str(x) for x in manifest_args]
    return ([str(x) for x in image_entry] if image_entry else []) + ([str(x) for x in image_cmd] if image_cmd else [])


def strip_wrappers(cmdline: List[str]) -> List[str]:
    toks = list(cmdline)
    while toks:
        head = toks[0]
        if head in WRAPPER_CMDS:
            toks = toks[1:]
            while toks and (toks[0].startswith("-") or toks[0] in {"--", "--user", "--chdir"}):
                if toks[0] in {"--user", "--chdir"} and len(toks) >= 2:
                    toks = toks[2:]
                else:
                    toks = toks[1:]
            continue
        if head in SHELLS and len(toks) >= 3 and toks[1] in {"-c", "-ec", "-lc", "-exc", "-elc"}:
            return toks
        break
    return toks


def extract_script_candidates_from_tokens(tokens: List[str]) -> List[str]:
    out: List[str] = []
    for t in tokens:
        tt = t.strip().strip('"\'')
        if is_shell_script_path(tt):
            out.append(tt)
    return list(dict.fromkeys(out))


def parse_exec_from_script_text(text: str, env: Dict[str, str], source: str) -> Tuple[List[ExecEntry], List[str], Set[str]]:
    substituted, used = substitute_env(text, env)
    lines = [ln.strip() for ln in substituted.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    joined = " ; ".join(lines)
    entries: List[ExecEntry] = []
    scripts: List[str] = []

    # explicit script calls inside current script
    for m in SCRIPT_PATH_RE.finditer(joined):
        sp = m.group("path")
        if is_shell_script_path(sp):
            scripts.append(sp)

    # exec <prog>
    for m in re.finditer(r"(?:^|[;&|]\s*)exec\s+([^\s;&|]+)", joined):
        prog = m.group(1).strip().strip('"\'')
        if prog and prog not in SKIP_BUILTINS:
            entries.append(ExecEntry(exe=prog, kind="daemon", pid1=True, always=True, evidence=m.group(0), source=source))

    # A && exec B
    for m in re.finditer(r"([^;&|]+?)\s*&&\s*exec\s+([^\s;&|]+)", joined):
        left = m.group(1).strip()
        right = m.group(2).strip().strip('"\'')
        left_prog = left.split()[0].strip('"\'') if left.split() else ""
        if left_prog and left_prog not in SKIP_BUILTINS:
            entries.append(ExecEntry(exe=left_prog, kind="one-shot", pid1=False, always=True, evidence=left, source=source))
        if right and right not in SKIP_BUILTINS:
            entries.append(ExecEntry(exe=right, kind="daemon", pid1=True, always=True, evidence=m.group(0), source=source))

    # pipelines / command separators - take leftmost executable token conservatively
    for part in re.split(r"[;&|]+", joined):
        part = part.strip()
        if not part:
            continue
        toks = part.split()
        if not toks:
            continue
        prog = toks[0].strip('"\'')
        if prog in SKIP_BUILTINS or prog in {"if", "then", "fi", "for", "do", "done", "while", "case", "esac"}:
            continue
        if is_shell_script_path(prog):
            scripts.append(prog)
            continue
        entries.append(ExecEntry(exe=prog, kind="one-shot", pid1=False, always=True, evidence=part, source=source))

    return dedup_entries(entries), list(dict.fromkeys(scripts)), used


def parse_cmdline_entries(cmdline: List[str], env: Dict[str, str]) -> Tuple[List[ExecEntry], List[str], Set[str], List[str]]:
    notes: List[str] = []
    cmdline = strip_wrappers(cmdline)
    entries: List[ExecEntry] = []
    scripts: List[str] = []
    used_all: Set[str] = set()

    if not cmdline:
        return [], [], set(), ["empty_cmdline_after_wrapper_strip"]

    if cmdline[0] in SHELLS and len(cmdline) >= 3 and cmdline[1] in {"-c", "-ec", "-lc", "-exc", "-elc"}:
        script_text = " ".join(cmdline[2:]).strip()
        script_text, used = substitute_env(script_text, env)
        e, s, used2 = parse_exec_from_script_text(script_text, env, source="shell-c")
        entries.extend(e)
        scripts.extend(s)
        used_all |= used | used2
        notes.append("parsed_shell_c")
        return dedup_entries(entries), list(dict.fromkeys(scripts)), used_all, notes

    scripts.extend(extract_script_candidates_from_tokens(cmdline))
    if scripts:
        notes.append("script_token_detected")

    prog = cmdline[0].strip().strip('"\'')
    if prog and prog not in SKIP_BUILTINS and prog not in SHELLS and not is_shell_script_path(prog):
        entries.append(ExecEntry(exe=prog, kind="daemon", pid1=True, always=True, evidence="direct_cmdline", source="cmdline"))
    elif prog in SHELLS:
        notes.append("shell_without_dash_c")
    elif is_shell_script_path(prog):
        scripts.append(prog)

    return dedup_entries(entries), list(dict.fromkeys(scripts)), used_all, notes


def normalize_script_path(script_path: str, working_dir: str = "/") -> str:
    p = script_path.strip().strip('"\'')
    if not p:
        return p
    if p.startswith("/"):
        return p
    if p.startswith("./"):
        return str(Path(working_dir) / p[2:])
    if p.startswith("../"):
        return str((Path(working_dir) / p).resolve())
    return p


def extract_script_from_image(cid: str, image: str, script_path: str, dst_root: Path) -> Tuple[Optional[Path], str]:
    candidate_paths = []
    if script_path.startswith("/"):
        candidate_paths.append(script_path)
    else:
        resolved = docker_resolve_executable(image, script_path)
        if resolved:
            candidate_paths.append(resolved)
        candidate_paths.append(script_path)
        candidate_paths.append(f"/usr/local/bin/{script_path}")
        candidate_paths.append(f"/usr/bin/{script_path}")
        candidate_paths.append(f"/bin/{script_path}")
        candidate_paths.append(f"/{script_path}")
    seen = set()
    for cand in candidate_paths:
        if not cand or cand in seen:
            continue
        seen.add(cand)
        local = dst_root / Path(cand).name
        ok, msg = docker_cp_from_container(cid, cand, local)
        if ok and local.exists() and local.is_file():
            return local, f"copied:{cand}"
    return None, "script_copy_failed"


def resolve_startup_chain(container: Dict[str, Any], image: str, skip_image_config: bool = False) -> StartupResolution:
    """Resolve startup chain from K8s manifest + image config.
    
    Args:
        container: K8s container spec
        image: Container image name
        skip_image_config: If True, only use manifest command/args, skip image config fetch
    """
    image_entry, image_cmd, image_env = None, None, {}
    
    if image and not skip_image_config:
        image_entry, image_cmd, image_env, _ = docker_image_config(image)
    
    env = parse_container_env(container, image_env)
    final_cmdline = normalize_full_command(container.get("command"), container.get("args"), image_entry, image_cmd)
    notes: List[str] = []
    
    if not final_cmdline:
        msg = "no_final_cmdline"
        if skip_image_config:
            msg = "no_final_cmdline_manifest_only"
        return StartupResolution([], [], [], [], [msg])
    
    entries, scripts, used, parse_notes = parse_cmdline_entries(final_cmdline, env)
    notes.extend(parse_notes)
    return StartupResolution(final_cmdline, entries, scripts, sorted(used), notes)


def enrich_entries_with_scripts(cid: str, image: str, startup: StartupResolution, out_script_dir: Path, max_depth: int = 3) -> StartupResolution:
    queue: List[Tuple[str, int]] = [(s, 0) for s in startup.scripts_considered]
    seen_scripts: Set[str] = set(startup.scripts_considered)
    all_entries = list(startup.entries)
    env = {}
    _, _, image_env, _ = docker_image_config(image)
    env.update(image_env)
    notes = list(startup.notes)
    scripts_used = list(startup.scripts_considered)
    env_used = set(startup.env_keys_used)

    while queue:
        script_ref, depth = queue.pop(0)
        if depth > max_depth:
            notes.append(f"script_depth_limit:{script_ref}")
            continue
        local_script, msg = extract_script_from_image(cid, image, script_ref, out_script_dir)
        if not local_script:
            notes.append(f"script_extract_failed:{script_ref}")
            continue
        try:
            text = local_script.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            notes.append(f"script_read_failed:{script_ref}")
            continue
        entries, more_scripts, used = parse_exec_from_script_text(text, env, source=f"script:{local_script.name}")
        all_entries.extend(entries)
        env_used |= used
        notes.append(f"script_parsed:{script_ref}")
        for s in more_scripts:
            if s not in seen_scripts:
                seen_scripts.add(s)
                scripts_used.append(s)
                queue.append((s, depth + 1))

    return StartupResolution(
        final_cmdline=startup.final_cmdline,
        entries=dedup_entries(all_entries),
        scripts_considered=scripts_used,
        env_keys_used=sorted(env_used),
        notes=notes,
    )


# ---------------------------
# Step4: map binary -> code entrypoints (closer to EPScan)
# ---------------------------
MAIN_FUNC_RE = re.compile(r"^\s*func\s+main\s*\(\s*\)\s*{", re.MULTILINE)
PKG_MAIN_RE = re.compile(r"^\s*package\s+main\s*$", re.MULTILINE)


def scan_repo_go_entrypoints(repo_dir: Path, max_files: int = 8000) -> List[Path]:
    out: List[Path] = []
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


def discover_go_modules(repo_dir: Path, max_files: int = 200) -> List[str]:
    mods: List[str] = []
    count = 0
    for gm in repo_dir.rglob("go.mod"):
        count += 1
        if count > max_files:
            break
        try:
            txt = gm.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        m = re.search(r"(?m)^\s*module\s+([^\s]+)\s*$", txt)
        if m:
            mods.append(m.group(1).strip())
    return sorted(set(mods))


def build_candidate_tokens(repo_dir: Path, candidate_main_files: List[Path], go_modules: List[str]) -> Dict[str, Set[str]]:
    rels: Set[str] = set()
    basenames: Set[str] = set()
    packages: Set[str] = set()
    dirs: Set[str] = set()
    for p in candidate_main_files:
        if not p.exists():
            continue
        try:
            rel = str(p.relative_to(repo_dir)).replace('\\', '/')
        except Exception:
            rel = p.name
        rels.add(rel)
        basenames.add(p.name)
        parent_rel = str(p.parent.relative_to(repo_dir)).replace('\\', '/') if p.parent != repo_dir else ''
        if parent_rel:
            dirs.add(parent_rel)
            parts = [x for x in parent_rel.split('/') if x]
            if parts:
                packages.add('/'.join(parts))
                if go_modules:
                    for mod in go_modules:
                        packages.add(f"{mod}/{parent_rel}")
        if go_modules:
            for mod in go_modules:
                packages.add(f"{mod}/{rel}")
                if parent_rel:
                    packages.add(f"{mod}/{parent_rel}")
    return {
        'rels': rels,
        'basenames': basenames,
        'packages': packages,
        'dirs': dirs,
        'modules': set(go_modules),
    }


def get_binary_build_info(binary_path: Path) -> Dict[str, Any]:
    info: Dict[str, Any] = {'is_go_binary': False, 'go_main': '', 'path': '', 'raw': ''}
    if shutil.which('go') is None:
        return info
    try:
        code, out, err = run(['go', 'version', '-m', str(binary_path)], timeout=120)
    except Exception:
        return info

    stdout = out or ''
    stderr = err or ''
    txt = stdout + ('\n' + stderr if stderr else '')
    info['raw'] = txt[:4000]

    if code != 0 or not stdout.strip():
        return info

    lines = [ln.rstrip() for ln in stdout.splitlines() if ln.strip()]
    if not lines:
        return info

    has_go_metadata = False
    for ln in lines[1:]:
        s = ln.strip()
        if (
            s.startswith('path\t')
            or s.startswith('mod\t')
            or s.startswith('build\t')
            or s.startswith('dep\t')
        ):
            has_go_metadata = True
            break

    if not has_go_metadata:
        return info

    info['is_go_binary'] = True
    for ln in lines:
        s = ln.strip()
        if s.startswith('path\t'):
            info['path'] = s.split('\t', 1)[1].strip()
        elif s.startswith('mod\t'):
            parts = s.split('\t')
            if len(parts) >= 2:
                info['go_main'] = parts[1].strip()
    return info


def collect_binary_strings(binary_path: Path, max_lines: int = 50000) -> List[str]:
    try:
        code, out, _ = run(['strings', '-a', '-n', '6', str(binary_path)], timeout=120)
        if code != 0:
            return []
        lines = out.splitlines()
        return lines[:max_lines]
    except Exception:
        return []


@dataclass
class MatchEvidence:
    matched_entrypoints: List[str]
    score: int
    status: str
    reasons: List[str]
    build_info: Dict[str, Any]
    sample_hits: List[str]


def match_binary_to_entrypoints(binary_path: Path, repo_dir: Path, candidate_main_files: List[Path], go_modules: List[str]) -> MatchEvidence:
    if not candidate_main_files:
        return MatchEvidence([], 0, 'no_candidate_mains', ['repo_has_no_detected_main_go'], {}, [])

    build_info = get_binary_build_info(binary_path)
    lines = collect_binary_strings(binary_path)
    if not lines and not build_info.get('is_go_binary'):
        return MatchEvidence([], 0, 'no_binary_metadata', ['strings_failed_or_empty', 'go_version_m_failed_or_not_go'], build_info, [])

    scores: Counter[str] = Counter()
    reasons_by_rel: Dict[str, Set[str]] = {}
    sample_hits: List[str] = []

    rels = [str(p.relative_to(repo_dir)).replace('\\', '/') for p in candidate_main_files if p.exists()]

    def add_reason(rel: str, pts: int, reason: str, sample: str = '') -> None:
        scores[rel] += pts
        reasons_by_rel.setdefault(rel, set()).add(reason)
        if sample and len(sample_hits) < 12:
            sample_hits.append(sample[:240])

    build_path = (build_info.get('path') or '').strip()
    build_mod = (build_info.get('go_main') or '').strip()
    for rel in rels:
        base = Path(rel).name
        parent = str(Path(rel).parent).replace('\\', '/')
        if build_path:
            if base and build_path.endswith('/' + base):
                add_reason(rel, 4, f'build_path_suffix:{build_path}', build_path)
            if parent and parent != '.' and parent in build_path:
                add_reason(rel, 5, f'build_path_parent:{parent}', build_path)
        if build_mod:
            if any(build_mod == m or build_mod.startswith(m + '/') for m in go_modules):
                add_reason(rel, 3, f'build_mod_same_module:{build_mod}', build_mod)

    for line in lines:
        for rel in rels:
            base = Path(rel).name
            parent = str(Path(rel).parent).replace('\\', '/')
            if rel and rel in line:
                add_reason(rel, 8, f'rel_path:{rel}', line)
            if parent and parent != '.' and f'/{parent}/' in line:
                add_reason(rel, 4, f'parent_dir:{parent}', line)
            if base and (('/' + base) in line or line.endswith(base)):
                add_reason(rel, 1, f'basename:{base}', line)
            for mod in go_modules:
                pkg = f'{mod}/{parent}' if parent and parent != '.' else mod
                if pkg and pkg in line:
                    add_reason(rel, 5, f'module_pkg:{pkg}', line)
                pkg_file = f'{mod}/{rel}'
                if pkg_file in line:
                    add_reason(rel, 6, f'module_file:{pkg_file}', line)

    go_markers_in_strings = False
    if lines:
        probe = '\n'.join(lines[:2000]).lower()
        go_markers_in_strings = (
            'go build id' in probe
            or '/src/runtime/runtime.go' in probe
            or 'gopclntab' in probe
            or 'command-line-arguments' in probe
        )
    has_go_signal = bool(build_info.get('is_go_binary')) or go_markers_in_strings

    if not scores:
        reasons = []
        if has_go_signal:
            reasons.append('go_binary_detected_but_no_repo_path_or_module_evidence')
            status = 'repo_mismatch_or_trimpath'
        else:
            reasons.append('binary_not_identified_as_go_or_metadata_missing')
            status = 'non_go_or_stripped'
        if go_modules:
            reasons.append('repo_go_modules=' + ','.join(go_modules[:5]))
        return MatchEvidence([], 0, status, reasons, build_info, sample_hits)

    top_score = max(scores.values())
    matched = sorted([rel for rel, sc in scores.items() if sc == top_score])

    if top_score >= 8 and has_go_signal:
        status = 'exact_or_strong_match'
    elif top_score >= 4 and has_go_signal:
        status = 'likely_match'
    elif top_score >= 4:
        status = 'path_match_without_go_signal'
    else:
        status = 'weak_match'

    reasons: List[str] = []
    for rel in matched[:5]:
        reasons.extend(sorted(reasons_by_rel.get(rel, set())))
    if not has_go_signal:
        reasons.append('matched_by_path_tokens_without_strong_go_markers')

    return MatchEvidence(matched, top_score, status, reasons[:20], build_info, sample_hits)

def detect_binary_language(binary_path: Path) -> Dict[str, Any]:
    """Best-effort language detection for an extracted binary or script.

    Returns keys: language, is_go, detail, file_output.
    """
    result: Dict[str, Any] = {
        'language': 'Unknown',
        'is_go': False,
        'detail': '',
        'file_output': '',
    }

    # 1) First inspect shebang / file type so interpreter wrappers do not get
    #    misclassified as Go just because another tool returns noisy output.
    file_out = ''
    if shutil.which('file') is not None:
        try:
            code, out, err = run(['file', '-b', str(binary_path)], timeout=60)
            file_out = (out or err or '').strip()
        except Exception:
            file_out = ''
    result['file_output'] = file_out[:400]
    low = file_out.lower()

    try:
        head = binary_path.read_bytes()[:4096]
        head_text = head.decode('utf-8', errors='ignore')
    except Exception:
        head_text = ''
    head_low = head_text.lower()

    if head_text.startswith('#!'):
        if 'python' in head_low:
            result.update({'language': 'Python', 'detail': 'shebang:python'})
            return result
        if 'node' in head_low or 'javascript' in head_low:
            result.update({'language': 'JavaScript', 'detail': 'shebang:node'})
            return result
        if 'ruby' in head_low:
            result.update({'language': 'Ruby', 'detail': 'shebang:ruby'})
            return result
        if 'php' in head_low:
            result.update({'language': 'PHP', 'detail': 'shebang:php'})
            return result
        if 'sh' in head_low or 'bash' in head_low or 'ash' in head_low or 'zsh' in head_low:
            result.update({'language': 'Shell', 'detail': 'shebang:shell'})
            return result

    if 'shell script' in low or 'bourne-again shell script' in low or 'posix shell script' in low:
        result.update({'language': 'Shell', 'detail': file_out[:400]})
        return result
    if 'go buildid=' in low or 'go build id' in low:
        result.update({'language': 'Go', 'is_go': True, 'detail': file_out[:400]})
        return result
    if 'python script' in low or 'python byte-compiled' in low or re.search(r'\bpython\b', low):
        result.update({'language': 'Python', 'detail': file_out[:400]})
        return result
    if 'java class data' in low or 'jar' in low or 'java archive' in low:
        result.update({'language': 'Java', 'detail': file_out[:400]})
        return result
    if 'node.js script' in low or 'javascript' in low:
        result.update({'language': 'JavaScript', 'detail': file_out[:400]})
        return result
    if 'ruby script' in low or re.search(r'\bruby\b', low):
        result.update({'language': 'Ruby', 'detail': file_out[:400]})
        return result
    if 'php script' in low or re.search(r'\bphp\b', low):
        result.update({'language': 'PHP', 'detail': file_out[:400]})
        return result

    # 2) Strong Go signal: go build metadata. Require real metadata, not merely
    #    any output from `go version -m`.
    build_info = get_binary_build_info(binary_path)
    if build_info.get('is_go_binary'):
        detail = build_info.get('path') or build_info.get('go_main') or 'go version -m matched'
        result.update({
            'language': 'Go',
            'is_go': True,
            'detail': str(detail)[:400],
        })
        return result

    # 3) strings-based heuristics as fallback
    lines = collect_binary_strings(binary_path, max_lines=8000)
    joined = ''.join(lines[:2000]).lower()
    if 'go build id' in joined or '/src/runtime/runtime.go' in joined or 'command-line-arguments' in joined or 'gopclntab' in joined:
        result.update({'language': 'Go', 'is_go': True, 'detail': 'strings heuristic: go markers'})
        return result
    if 'python' in joined and ('site-packages' in joined or 'pyc' in joined or '__main__' in joined):
        result.update({'language': 'Python', 'detail': 'strings heuristic: python markers'})
        return result
    if 'openjdk' in joined or 'java/lang/' in joined or 'kotlin/' in joined:
        result.update({'language': 'Java', 'detail': 'strings heuristic: jvm markers'})
        return result
    if 'node_modules' in joined or 'node.js' in joined or 'npm' in joined:
        result.update({'language': 'JavaScript', 'detail': 'strings heuristic: node markers'})
        return result

    if 'elf' in low or 'mach-o' in low or 'pe32' in low or 'executable' in low or 'shared object' in low:
        result.update({'language': 'Native/Unknown', 'detail': file_out[:400]})
    else:
        result.update({'language': 'Unknown', 'detail': (file_out or 'no reliable language markers')[:400]})
    return result


def summarize_binary_language_records(records: List[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []
    for rec in records:
        binary = Path(rec.get("binary", "")).name or rec.get("binary", "")
        lang = rec.get("language", "Unknown")
        is_go = rec.get("is_go", False)
        detail = rec.get("detail", "")
        suffix = f" detail={detail}" if detail else ""
        lines.append(f"{binary}: language={lang} is_go={str(is_go).lower()}{suffix}")
    return lines


def summarize_repo_mismatch(container_result: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    status = container_result.get("match_status", "")
    if status == "repo_mismatch_or_trimpath":
        for rec in container_result.get("binary_languages", []) or []:
            binary = Path(rec.get("binary", "")).name or rec.get("binary", "")
            lines.append(f"{binary}: likely not in this repo")
    return lines


def summarize_entry_matches(container_result: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    bins = [Path(x).name for x in (container_result.get("extracted_binaries", []) or [])]
    entrys = container_result.get("matched_code_entrypoints", []) or []
    if entrys:
        prefix = ", ".join(bins) if bins else container_result.get("container_name", "")
        lines.append(f"{prefix}: entrypoints=" + ", ".join(entrys))
    return lines

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
    final_cmdline: str
    scripts_considered: List[str]
    env_keys_used: List[str]
    derived_entries: List[Dict[str, Any]]
    extracted_binaries: List[str]
    matched_code_entrypoints: List[str]
    candidate_mains_count: int
    repo_go_modules: List[str]
    match_status: str
    match_score: int
    binary_build_info: List[Dict[str, Any]]
    binary_languages: List[Dict[str, Any]]
    match_evidence: List[str]
    sample_hits: List[str]
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
    parsed_objects: int = 0
    podspec_objects: int = 0


# ---------------------------
# Pipeline per repo
# ---------------------------

def manifest_cmdline_only(container: Dict[str, Any]) -> List[str]:
    cmd = [str(x) for x in (container.get("command") or [])]
    args = [str(x) for x in (container.get("args") or [])]
    return cmd + args


def split_notes(notes: str) -> List[str]:
    if not notes:
        return []
    return [x.strip() for x in notes.split(";") if x.strip()]


def collect_zero_reasons_for_container(c: Dict[str, Any], repo: RepoResult) -> List[str]:
    reasons: List[str] = []
    notes = split_notes(str(c.get("notes") or ""))
    bins = c.get("extracted_binaries") or []
    matches = c.get("matched_code_entrypoints") or []
    final_cmd = str(c.get("final_cmdline") or "").strip()
    candidate_mains_count = int(c.get("candidate_mains_count") or 0)

    if not repo.charts_found:
        reasons.append("no_chart")
    if not repo.rendered_manifests and repo.charts_found:
        reasons.append("no_rendered_manifest")
    if repo.rendered_manifests and len(repo.containers) == 0:
        reasons.append("no_podspec_or_no_containers")

    if not final_cmd:
        reasons.append("no_cmd")
    if any(n.startswith("docker_pull_failed:") for n in notes):
        reasons.append("image_unpullable")
    if "private_registry_skipped" in notes:
        reasons.append("image_private_skipped")
    if "docker_skipped" in notes:
        reasons.append("docker_skipped")
    if not bins:
        reasons.append("no_extracted_binaries")
    if not matches:
        reasons.append("no_entrypoint_match")
    if candidate_mains_count == 0:
        reasons.append("no_go_mains")

    status = str(c.get("match_status") or "")
    if status:
        reasons.append(f"match_status:{status}")

    return sorted(set(reasons))


def build_zero_diagnostic_rows(rr: RepoResult) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    if not rr.containers:
        repo_reasons: List[str] = []
        if not rr.repo_ok:
            repo_reasons.append("repo_unavailable")
        if not rr.charts_found:
            repo_reasons.append("no_chart")
        if rr.charts_found and not rr.rendered_manifests:
            repo_reasons.append("no_rendered_manifest")
        if rr.rendered_manifests and rr.podspec_objects == 0:
            repo_reasons.append("no_podspec")
        if rr.rendered_manifests and rr.podspec_objects > 0:
            repo_reasons.append("no_containers")
        if rr.errors:
            repo_reasons.append("render_or_repo_errors")

        rows.append({
            "repo_url": rr.repo_url,
            "workload": "",
            "kind": "",
            "container_name": "",
            "image": "",
            "charts_found": len(rr.charts_found),
            "rendered_manifests": len(rr.rendered_manifests),
            "parsed_objects": rr.parsed_objects,
            "podspec_objects": rr.podspec_objects,
            "binaries_extracted": 0,
            "entrypoints_matched": 0,
            "zero_reasons": ";".join(sorted(set(repo_reasons)) or ["no_container_records"]),
            "notes": rr.repo_detail or ";".join(rr.errors),
        })
        return rows

    for c in rr.containers:
        bins = c.get("extracted_binaries") or []
        matches = c.get("matched_code_entrypoints") or []
        rows.append({
            "repo_url": rr.repo_url,
            "workload": c.get("workload", ""),
            "kind": c.get("kind", ""),
            "container_name": c.get("container_name", ""),
            "image": c.get("image", ""),
            "charts_found": len(rr.charts_found),
            "rendered_manifests": len(rr.rendered_manifests),
            "parsed_objects": rr.parsed_objects,
            "podspec_objects": rr.podspec_objects,
            "binaries_extracted": len(bins),
            "entrypoints_matched": len(matches),
            "zero_reasons": ";".join(collect_zero_reasons_for_container(c, rr)),
            "notes": str(c.get("notes") or ""),
        })
    return rows


def analyze_repo(
    repo_dir: Path,
    source_csv: str,
    repo_url: str,
    out_dir: Path,
    namespace: str = "default",
    no_docker: bool = False,
    skip_private_registry: bool = True,
    pull_timeout: int = 300,
    helm_template_timeout: int = 240,
    helm_dependency_timeout: int = 90,
    helm_skip_dependency_build: bool = False,
    max_entry_extract: int = 24,
) -> RepoResult:
    charts = find_chart_dirs(repo_dir)
    log_debug(f"Found {len(charts)} charts")
    rendered_paths: List[str] = []
    container_results: List[Dict[str, Any]] = []
    errors: List[str] = []
    total_obj_count = 0
    total_podspec_count = 0
    candidate_mains = scan_repo_go_entrypoints(repo_dir)
    go_modules = discover_go_modules(repo_dir)
    log_debug(f"candidate_mains={len(candidate_mains)} go_modules={len(go_modules)}")

    def process_container(c: Dict[str, Any], wname: str, wkind: str, section: str, source_tag: str) -> None:
        cname = c.get("name", "")
        image = c.get("image", "")
        startup = resolve_startup_chain(c, image, skip_image_config=True)
        extracted_bins: List[str] = []
        matched_entrypoints: List[str] = []
        binary_build_infos: List[Dict[str, Any]] = []
        binary_languages: List[Dict[str, Any]] = []
        match_evidence_lines: List[str] = []
        sample_hits: List[str] = []
        best_match_status = "not_attempted"
        best_match_score = 0
        notes: List[str] = [f"source:{source_tag}"]

        if image and not no_docker:
            if skip_private_registry and is_private_registry_image(image):
                notes.append("private_registry_skipped")
                log_info(f"skip private image: {image}")
                if startup.final_cmdline:
                    notes.append("manifest_only_startup")
            else:
                if docker_image_exists_locally(image):
                    log_debug(f"image already exists locally, skip pull: {image}")
                    pull_ok, pull_msg = True, "already_present_local"
                else:
                    log_debug(f"pulling image: {image}")
                    pull_ok, pull_msg = docker_pull(image, timeout=pull_timeout)

                if not pull_ok:
                    notes.append(f"docker_pull_failed:{pull_msg}")
                    startup = resolve_startup_chain(c, image, skip_image_config=True)
                    if startup.final_cmdline:
                        notes.append("manifest_only_startup")
                else:
                    cid, cmsg = docker_create(image)
                    if not cid:
                        notes.append(f"docker_create_failed:{cmsg}")
                        startup = resolve_startup_chain(c, image, skip_image_config=True)
                        if startup.final_cmdline:
                            notes.append("manifest_only_due_to_create_fail")
                    else:
                        try:
                            startup = resolve_startup_chain(c, image)
                            startup = enrich_entries_with_scripts(
                                cid,
                                image,
                                startup,
                                out_dir / "scripts" / safe_repo_dir_name(repo_url) / wkind / wname / section / cname,
                            )
                            notes.extend(startup.notes)
                            extract_entries = select_entries_for_extraction(startup.entries, max_items=max_entry_extract)
                            if len(startup.entries) > len(extract_entries):
                                notes.append(f"entry_extract_limited:{len(extract_entries)}/{len(startup.entries)}")

                            exe_map = docker_resolve_executables_batch(
                                image,
                                [e.exe for e in extract_entries],
                                timeout=120,
                            )

                            for e in extract_entries:
                                exe_path = exe_map.get((e.exe or "").strip())
                                if not exe_path:
                                    notes.append(f"resolve_failed:{e.exe}")
                                    continue
                                safe_bin_name = exe_path.lstrip('/').replace('/', '__') or Path(exe_path).name
                                local_bin = out_dir / "binaries" / safe_repo_dir_name(repo_url) / wkind / wname / section / cname / safe_bin_name
                                okcp, _ = docker_cp_from_container(cid, exe_path, local_bin)
                                if not okcp:
                                    notes.append(f"copy_failed:{exe_path}")
                                    continue
                                if not local_bin.exists() or not local_bin.is_file():
                                    notes.append(f"copied_non_file:{exe_path}")
                                    continue
                                extracted_bins.append(str(local_bin))
                                lang_info = detect_binary_language(local_bin)
                                binary_languages.append({
                                    'binary': str(local_bin),
                                    **lang_info,
                                })
                                if not lang_info.get('is_go'):
                                    reason = f"{Path(local_bin).name}:non_go_or_unknown:{lang_info.get('language','unknown')}"
                                    match_evidence_lines.append(reason)
                                    if lang_info.get('detail'):
                                        sample_hits.append(f"{Path(local_bin).name}:{lang_info.get('detail')}")

                                mev = match_binary_to_entrypoints(local_bin, repo_dir, candidate_mains, go_modules)
                                binary_build_infos.append({
                                    'binary': str(local_bin),
                                    **mev.build_info,
                                })
                                if mev.matched_entrypoints:
                                    matched_entrypoints.extend(mev.matched_entrypoints)
                                if mev.reasons:
                                    match_evidence_lines.extend([f"{Path(local_bin).name}:{r}" for r in mev.reasons])
                                if mev.sample_hits:
                                    sample_hits.extend([f"{Path(local_bin).name}:{s}" for s in mev.sample_hits])
                                if mev.score > best_match_score:
                                    best_match_score = mev.score
                                    best_match_status = mev.status
                                elif best_match_status == "not_attempted" or best_match_status == 'skipped_non_go':
                                    best_match_status = mev.status
                        finally:
                            docker_rm(cid)
        elif no_docker:
            notes.append("docker_skipped")
            if startup.final_cmdline:
                notes.append("manifest_only_startup")

        if best_match_status == "not_attempted":
            if not extracted_bins:
                best_match_status = "no_extracted_binaries"
            elif candidate_mains == []:
                best_match_status = "no_candidate_mains"

        container_results.append(asdict(ContainerResult(
            workload=wname,
            kind=wkind,
            container_name=f"{section}:{cname}",
            image=image,
            manifest_cmdline=" ".join(manifest_cmdline_only(c)),
            final_cmdline=" ".join(startup.final_cmdline),
            scripts_considered=startup.scripts_considered,
            env_keys_used=startup.env_keys_used,
            derived_entries=[asdict(e) for e in startup.entries],
            extracted_binaries=sorted(set(extracted_bins)),
            matched_code_entrypoints=sorted(set(matched_entrypoints)),
            candidate_mains_count=len(candidate_mains),
            repo_go_modules=go_modules[:20],
            match_status=best_match_status,
            match_score=best_match_score,
            binary_build_info=binary_build_infos,
            binary_languages=binary_languages,
            match_evidence=sorted(set(match_evidence_lines))[:50],
            sample_hits=sorted(set(sample_hits))[:20],
            notes=";".join(notes),
        )))
        log_debug(
            f"{section}:{cname} | image={image} | final_cmd={len(startup.final_cmdline)} | "
            f"scripts={len(startup.scripts_considered)} | entries={len(startup.entries)} {format_entries_for_log(startup.entries)} | "
            f"bins={len(set(extracted_bins))} | matches={len(set(matched_entrypoints))} | status={best_match_status} | score={best_match_score}"
        )

    for chart_dir in charts:
        chart_out = out_dir / "rendered" / chart_dir.relative_to(repo_dir)
        ok, rendered_yaml, msg = helm_template(
            chart_dir,
            chart_out,
            release="test",
            namespace=namespace,
            values_file=None,
            template_timeout=helm_template_timeout,
            dependency_timeout=helm_dependency_timeout,
            skip_dependency_build=helm_skip_dependency_build,
        )
        if not ok:
            if "missing in charts/ directory" in msg:
                subcharts_dir = chart_dir / "charts"
                has_local_subcharts = subcharts_dir.exists() and any(subcharts_dir.rglob("Chart.yaml"))
                if has_local_subcharts:
                    log_info(f"skip parent chart render due to missing dependencies: {chart_dir}")
                    continue
            render_err = f"helm_template_failed: {chart_dir}: {msg}"
            log_warn(render_err)
            errors.append(render_err)
            continue
        rendered_paths.append(str(rendered_yaml))
        log_debug(f"Rendered: {rendered_yaml}")

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

            for section in ["initContainers", "containers"]:
                for c in (podspec.get(section, []) or []):
                    process_container(c, wname, wkind, section, source_tag="helm")

        total_obj_count += obj_count
        total_podspec_count += podspec_count

        if obj_count == 0:
            log_warn("No K8s objects found in rendered manifest")
        else:
            log_debug(f"Parsed {obj_count} K8s objects, {podspec_count} with podspec")

    if not charts:
        raw_yaml_files = find_repo_manifest_yaml_files(repo_dir)
        log_info(f"no chart found, fallback scanning yaml manifests: {len(raw_yaml_files)} files")
        for yf in raw_yaml_files:
            obj_count = 0
            podspec_count = 0
            for obj in iter_yaml_docs(yf):
                obj_count += 1
                podspec = get_podspec(obj) or get_podspec_loose(obj)
                if not podspec:
                    continue
                podspec_count += 1
                wname = obj_meta_name(obj) or yf.stem
                wkind = obj.get("kind", "") or "UnknownKind"
                rel_file = str(yf.relative_to(repo_dir))
                for section in ["initContainers", "containers"]:
                    for c in (podspec.get(section, []) or []):
                        process_container(c, wname, wkind, section, source_tag=f"yaml:{rel_file}")

            total_obj_count += obj_count
            total_podspec_count += podspec_count

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
        errors=errors,
        parsed_objects=total_obj_count,
        podspec_objects=total_podspec_count,
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
    ap.add_argument("--helm-template-timeout", type=int, default=240, help="Timeout (seconds) for each helm template attempt")
    ap.add_argument("--helm-dependency-timeout", type=int, default=90, help="Timeout (seconds) for helm dependency build")
    ap.add_argument("--helm-skip-dependency-build", action="store_true", help="Skip helm dependency build to avoid network stalls")
    ap.add_argument("--max-entry-extract", type=int, default=24, help="Max startup executables extracted per container to avoid slow script noise")
    ap.add_argument("--include-private-registry", action="store_true", help="Include private/custom registry images (default: skip to avoid long waits)")
    ap.add_argument("--verbose", action="store_true", help="Print detailed [DEBUG]/[INFO]/[WARN] logs")
    args = ap.parse_args()

    global VERBOSE
    VERBOSE = args.verbose

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

    workdir = Path(args.workdir)
    outdir = Path(args.outdir)
    workdir.mkdir(parents=True, exist_ok=True)
    outdir.mkdir(parents=True, exist_ok=True)

    repos = read_repo_urls(csv_paths, limit=args.max or 0)
    print(f"[*] Loaded {len(repos)} unique repo URLs")

    jsonl_path = outdir / "step1_4_report.jsonl"
    csv_summary_path = outdir / "step1_4_summary.csv"
    csv_zero_diag_path = outdir / "step1_4_zero_diagnostics.csv"
    summary_rows: List[Dict[str, Any]] = []
    zero_diag_rows: List[Dict[str, Any]] = []

    with jsonl_path.open("w", encoding="utf-8") as jf:
        for idx, (src, url) in enumerate(repos, start=1):
            repo_name = safe_repo_dir_name(url)
            repo_dir = workdir / repo_name
            print(f"[{idx}/{len(repos)}] repo={repo_name} url={url}")
            ok, action, detail = ensure_repo(url, repo_dir, skip_if_healthy=args.skip_existing)
            if not ok:
                rr = RepoResult(
                    source_csv=src,
                    repo_url=url,
                    local_dir=str(repo_dir),
                    repo_ok=False,
                    repo_action=action,
                    repo_detail=detail,
                    charts_found=[],
                    rendered_manifests=[],
                    containers=[],
                    errors=[detail],
                )
                jf.write(json.dumps(asdict(rr), ensure_ascii=False) + "\n")
                zero_diag_rows.extend(build_zero_diagnostic_rows(rr))
                summary_rows.append({
                    "repo_url": url,
                    "repo_ok": False,
                    "repo_action": action,
                    "repo_detail": detail,
                    "charts_found": 0,
                    "containers_parsed": 0,
                    "binaries_extracted": 0,
                    "entrypoints_matched": 0,
                    "errors": detail,
                })
                print("[SUMMARY] charts=0 containers=0 bins=0 matches=0")
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
                helm_template_timeout=args.helm_template_timeout,
                helm_dependency_timeout=args.helm_dependency_timeout,
                helm_skip_dependency_build=args.helm_skip_dependency_build,
                max_entry_extract=args.max_entry_extract,
            )
            rr.repo_ok = True
            rr.repo_action = action
            rr.repo_detail = detail
            jf.write(json.dumps(asdict(rr), ensure_ascii=False) + "\n")
            zero_diag_rows.extend(build_zero_diagnostic_rows(rr))

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
                "errors": ";".join(rr.errors),
            })
            print(f"[SUMMARY] charts={len(rr.charts_found)} containers={len(rr.containers)} bins={binaries_extracted} matches={entrypoints_matched}")
            for c in rr.containers:
                for line in summarize_binary_language_records(c.get("binary_languages", []) or []):
                    print(f"[BIN] {line}")
                for line in summarize_repo_mismatch(c):
                    print(f"[REPO] {line}")
                for line in summarize_entry_matches(c):
                    print(f"[MATCH] {line}")

    if summary_rows:
        with csv_summary_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
            w.writeheader()
            for r in summary_rows:
                w.writerow(r)

    if zero_diag_rows:
        with csv_zero_diag_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(zero_diag_rows[0].keys()))
            w.writeheader()
            for r in zero_diag_rows:
                w.writerow(r)

    print(f"[*] Wrote JSONL: {jsonl_path}")
    print(f"[*] Wrote CSV : {csv_summary_path}")
    print(f"[*] Wrote ZeroDiag CSV : {csv_zero_diag_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
