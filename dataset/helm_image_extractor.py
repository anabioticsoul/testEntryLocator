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

try:
    import yaml
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


def helm_template(chart_dir: Path, out_dir: Path, release: str = "test", namespace: str = "default", values_file: Optional[Path] = None) -> Tuple[bool, Path, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_yaml = out_dir / "rendered.yaml"

    if (chart_dir / "Chart.yaml").exists():
        run(["helm", "dependency", "build", str(chart_dir)], timeout=600)

    strategies = [
        ["helm", "template", release, str(chart_dir), "--namespace", namespace] + (["-f", str(values_file)] if values_file and values_file.exists() else []),
        ["helm", "template", release, str(chart_dir), "--namespace", namespace],
        [
            "helm", "template", release, str(chart_dir), "--namespace", namespace,
            "--set", "global.restapi.jvm.maxheapmemory=''",
            "--set", "global.restapi.jvm.minheapmemory=''",
            "--set", "Values={}"
        ],
    ]
    last_err = ""
    for attempt, cmd in enumerate(strategies, 1):
        code, stdout, stderr = run(cmd, timeout=900)
        if code == 0 and stdout.strip():
            out_yaml.write_text(stdout, encoding="utf-8")
            return True, out_yaml, f"ok_strategy_{attempt}"
        last_err = stderr.strip()[:300]
    return False, out_yaml, f"helm_template_all_strategies_failed: {last_err}"


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
        return obj.get("spec")
    spec = obj.get("spec", {}) or {}
    if kind == "CronJob":
        return spec.get("jobTemplate", {}).get("spec", {}).get("template", {}).get("spec")
    return spec.get("template", {}).get("spec")


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

def docker_pull(image: str, timeout: int = 300) -> Tuple[bool, str]:
    code, _, err = run(["docker", "pull", image], timeout=timeout)
    if code != 0:
        return False, err.strip()[:500]
    return True, "ok"


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


def resolve_startup_chain(container: Dict[str, Any], image: str) -> StartupResolution:
    image_entry, image_cmd, image_env, _ = docker_image_config(image) if image else (None, None, {}, "")
    env = parse_container_env(container, image_env)
    final_cmdline = normalize_full_command(container.get("command"), container.get("args"), image_entry, image_cmd)
    notes: List[str] = []
    if not final_cmdline:
        return StartupResolution([], [], [], [], ["no_final_cmdline"])
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
# Step4: map binary -> code entrypoints (EPScan-like)
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


def strings_match_source_paths(binary_path: Path, repo_dir: Path, candidate_main_files: List[Path]) -> List[str]:
    if not candidate_main_files:
        return []
    rels = [str(p.relative_to(repo_dir)) for p in candidate_main_files if p.exists()]
    if not rels:
        return []
    matched: List[str] = []
    try:
        code, out, _ = run(["strings", "-a", "-n", "6", str(binary_path)], timeout=120)
        if code != 0:
            return []
        for line in out.splitlines():
            for r in rels:
                if r in line or Path(r).name in line:
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
    final_cmdline: str
    scripts_considered: List[str]
    env_keys_used: List[str]
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

def manifest_cmdline_only(container: Dict[str, Any]) -> List[str]:
    cmd = [str(x) for x in (container.get("command") or [])]
    args = [str(x) for x in (container.get("args") or [])]
    return cmd + args


def analyze_repo(repo_dir: Path, source_csv: str, repo_url: str, out_dir: Path, namespace: str = "default", no_docker: bool = False, skip_private_registry: bool = True, pull_timeout: int = 300) -> RepoResult:
    charts = find_chart_dirs(repo_dir)
    print(f" [DEBUG] Found {len(charts)} charts")
    rendered_paths: List[str] = []
    container_results: List[Dict[str, Any]] = []
    errors: List[str] = []
    candidate_mains = scan_repo_go_entrypoints(repo_dir)

    for chart_dir in charts:
        chart_out = out_dir / "rendered" / chart_dir.relative_to(repo_dir)
        ok, rendered_yaml, msg = helm_template(chart_dir, chart_out, release="test", namespace=namespace, values_file=None)
        if not ok:
            render_err = f"helm_template_failed: {chart_dir}: {msg}"
            print(f" [WARN] {render_err}")
            errors.append(render_err)
            continue
        rendered_paths.append(str(rendered_yaml))
        print(f" [DEBUG] Rendered: {rendered_yaml}")

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
                    cname = c.get("name", "")
                    image = c.get("image", "")
                    startup = StartupResolution([], [], [], [], ["docker_not_run"])
                    extracted_bins: List[str] = []
                    matched_entrypoints: List[str] = []
                    notes: List[str] = []

                    if image and not no_docker:
                        if skip_private_registry and is_private_registry_image(image):
                            notes.append("private_registry_skipped")
                            print(f" [INFO] skip private image: {image}")
                        else:
                            print(f" [DEBUG] pulling image: {image}")
                            pull_ok, pull_msg = docker_pull(image, timeout=pull_timeout)
                            if not pull_ok:
                                notes.append(f"docker_pull_failed:{pull_msg}")
                            else:
                                cid, cmsg = docker_create(image)
                                if not cid:
                                    notes.append(f"docker_create_failed:{cmsg}")
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
                                        for e in startup.entries:
                                            exe_path = docker_resolve_executable(image, e.exe)
                                            if not exe_path:
                                                notes.append(f"resolve_failed:{e.exe}")
                                                continue
                                            local_bin = out_dir / "binaries" / safe_repo_dir_name(repo_url) / wkind / wname / section / cname / Path(exe_path).name
                                            okcp, _ = docker_cp_from_container(cid, exe_path, local_bin)
                                            if not okcp:
                                                notes.append(f"copy_failed:{exe_path}")
                                                continue
                                            extracted_bins.append(str(local_bin))
                                            matches = strings_match_source_paths(local_bin, repo_dir, candidate_mains)
                                            if matches:
                                                matched_entrypoints.extend(matches)
                                    finally:
                                        docker_rm(cid)
                    elif no_docker:
                        notes.append("docker_skipped")

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
                        notes=";".join(notes),
                    )))
                    print(
                        f" [DEBUG] {section}:{cname} | image={image} | final_cmd={len(startup.final_cmdline)} | "
                        f"scripts={len(startup.scripts_considered)} | entries={len(startup.entries)} {format_entries_for_log(startup.entries)} | "
                        f"bins={len(set(extracted_bins))} | matches={len(set(matched_entrypoints))}"
                    )

        if obj_count == 0:
            print(" [WARN] No K8s objects found in rendered manifest")
        else:
            print(f" [DEBUG] Parsed {obj_count} K8s objects, {podspec_count} with podspec")

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
    ap.add_argument("--include-private-registry", action="store_true", help="Include private/custom registry images (default: skip to avoid long waits)")
    args = ap.parse_args()

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
    summary_rows: List[Dict[str, Any]] = []

    with jsonl_path.open("w", encoding="utf-8") as jf:
        for idx, (src, url) in enumerate(repos, start=1):
            repo_name = safe_repo_dir_name(url)
            repo_dir = workdir / repo_name
            print(f"[{idx}/{len(repos)}] {url}")
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
                "errors": ";".join(rr.errors),
            })
            print(f" [SUMMARY] charts={len(rr.charts_found)} containers={len(rr.containers)} bins={binaries_extracted} matches={entrypoints_matched}")

    if summary_rows:
        with csv_summary_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
            w.writeheader()
            for r in summary_rows:
                w.writerow(r)

    print(f"[*] Wrote JSONL: {jsonl_path}")
    print(f"[*] Wrote CSV : {csv_summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
