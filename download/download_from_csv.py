import csv
import os
import re
import sys
import requests
from urllib.parse import urlparse
from pathlib import Path

CSV_PATH = "GITLAB-URLS.csv"
OUT_DIR = "GITLAB"
TIMEOUT = 30
RETRIES = 3
CHUNK_SIZE = 8192


def github_blob_to_raw(url: str) -> str:
    """
    https://github.com/<owner>/<repo>/blob/<ref>/<path>
    -> https://raw.githubusercontent.com/<owner>/<repo>/<ref>/<path>
    """
    p = urlparse(url)
    if p.netloc != "github.com":
        return url
    parts = p.path.strip("/").split("/")
    if len(parts) >= 5 and parts[2] == "blob":
        owner, repo, _, ref = parts[:4]
        rest = parts[4:]
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}/" + "/".join(rest)
    return url


def gitlab_blob_to_raw(url: str) -> str:
    """
    GitLab raw rule (gitlab.com):
    https://gitlab.com/<namespace...>/<project>/blob/<ref>/<path>
    -> https://gitlab.com/<namespace...>/<project>/-/raw/<ref>/<path>

    Notes:
    - <namespace...> can be multi-level groups/subgroups.
    - This assumes standard GitLab URL structure.
    """
    p = urlparse(url)
    if p.netloc != "gitlab.com":
        return url

    parts = p.path.strip("/").split("/")
    # Need to locate "blob" position because group can be multi-level
    if "blob" not in parts:
        return url

    blob_i = parts.index("blob")
    # Must have at least: <proj...>/blob/<ref>/<file...>
    if blob_i < 1 or blob_i + 1 >= len(parts):
        return url

    ref = parts[blob_i + 1]
    before = parts[:blob_i]        # namespace.../project
    after = parts[blob_i + 2:]     # file path

    if not after:
        return url

    return "https://gitlab.com/" + "/".join(before) + "/-/raw/" + ref + "/" + "/".join(after)


def normalize_raw_url(url: str) -> str:
    """
    Convert known web UI URLs (GitHub/GitLab blob) to raw content URLs.
    """
    u = url.strip()
    u2 = github_blob_to_raw(u)
    u3 = gitlab_blob_to_raw(u2)
    return u3


def looks_like_html(head_bytes: bytes) -> bool:
    s = head_bytes.lstrip().lower()
    return (
        s.startswith(b"<!doctype html")
        or s.startswith(b"<html")
        or b"<title" in s[:2048]
    )


def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = name.strip().strip(".")
    return name or "file"


def filename_from_url(url: str, index: int) -> str:
    parsed = urlparse(url)
    base = os.path.basename(parsed.path)
    if not base:
        base = f"file_{index}"
    return sanitize_filename(base)


def uniquify_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    k = 2
    while True:
        cand = parent / f"{stem}__{k}{suffix}"
        if not cand.exists():
            return cand
        k += 1


def download(url: str, out_path: Path):
    headers = {
        "User-Agent": "bulk-downloader/1.0",
        "Accept": "*/*",
    }
    with requests.get(url, stream=True, timeout=TIMEOUT, headers=headers, allow_redirects=True) as r:
        r.raise_for_status()

        it = r.iter_content(chunk_size=CHUNK_SIZE)
        first = next(it, b"")
        if first and looks_like_html(first):
            raise ValueError("Response looks like HTML (likely web UI page, not raw file).")

        with open(out_path, "wb") as f:
            if first:
                f.write(first)
            for chunk in it:
                if chunk:
                    f.write(chunk)


def main():
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

    with open(CSV_PATH, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        for idx, row in enumerate(reader, start=1):
            if not row:
                continue
            url = row[0].strip()
            if not url or url.startswith("#"):
                continue

            fixed_url = normalize_raw_url(url)

            base_name = filename_from_url(fixed_url, idx)
            out_path = uniquify_path(Path(OUT_DIR) / base_name)

            last_err = None
            for attempt in range(1, RETRIES + 1):
                try:
                    print(f"[DOWN] ({attempt}/{RETRIES}) {fixed_url}")
                    download(fixed_url, out_path)
                    print(f"[OK]   Saved to {out_path}")
                    break
                except Exception as e:
                    last_err = e
                    print(f"[WARN] Attempt {attempt} failed: {e}")
                    if attempt == RETRIES:
                        print(f"[FAIL] {url} -> {last_err}", file=sys.stderr)


if __name__ == "__main__":
    main()
