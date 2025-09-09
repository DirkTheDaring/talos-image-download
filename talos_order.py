#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
talos_order.py (v1.7)
- Separate --config (defaults + manifest) and --orders (orders list)
- Per-order manifests (manifest-{orderid}.yaml or template/dir)
- Features:
    * Talos Image Factory schematic creation from 'customization'
    * Platforms: nocloud (iso / raw.xz) + SecureBoot for BOTH iso and raw.xz
    * raw.xz -> .raw decompression
    * rsync push to Proxmox nodes
    * cache retention (keep latest N per artifact family)
    * optional full cache purge before run
- Improvements vs 1.6:
    * build_asset_url supports secureboot for nocloud raw.xz
    * robust semver handling for 'latest-in-minor' (GA preferred)
    * clearer error messages (orderid/position context)

Usage:
  ./talos_order.py --config config.yaml --orders orders.yaml --dry-run
  sudo ./talos_order.py --config config.yaml --orders orders.yaml
  sudo ./talos_order.py --config config.yaml --orders orders.yaml --purge-cache
"""

import sys, os, re, json, hashlib, shutil, subprocess, time
import urllib.request
from urllib.error import HTTPError, URLError

# --- YAML dependency (PyYAML) ---
try:
    import yaml
except ImportError:
    sys.stderr.write("ERROR: PyYAML is required. Install it with:  pip install pyyaml\n")
    sys.exit(1)

# Optional lzma (for raw.xz decompression)
try:
    import lzma
    HAVE_LZMA = True
except Exception:
    HAVE_LZMA = False

GITHUB_LATEST = "https://api.github.com/repos/siderolabs/talos/releases/latest"
GITHUB_TAGS   = "https://api.github.com/repos/siderolabs/talos/tags"
FACTORY_SCHEMATICS = "https://factory.talos.dev/schematics"

USER_AGENT = "talos-order/1.7"

# ---------------- HTTP helpers ----------------

def http_json(url, headers=None):
    req = urllib.request.Request(url, headers=(headers or {"User-Agent": USER_AGENT}))
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode("utf-8"))

def http_download(url, dest_path, user_agent=USER_AGENT):
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    tmp = dest_path + ".part"
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req) as r, open(tmp, "wb") as f:
        shutil.copyfileobj(r, f)
    os.replace(tmp, dest_path)

def http_post_yaml_get_json(url: str, yaml_body: str):
    req = urllib.request.Request(
        url,
        data=yaml_body.encode("utf-8"),
        headers={"Content-Type": "application/x-yaml", "User-Agent": USER_AGENT},
        method="POST",
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode("utf-8"))

# ---------------- utilities ----------------

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def run(cmd):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.returncode, p.stdout.strip(), p.stderr.strip()

def shlex_quote(s):
    return "'" + s.replace("'", "'\"'\"'") + "'"

# ---------------- version resolution ----------------

_SEMVER_CORE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")

def _semver_key(tag: str):
    """
    Return a sortable key from a tag like 'v1.11.0-beta.1' or 'v1.11.0'.
    GA > pre-release. Reverse sort (key desc) yields newest GA first.
    """
    t = tag.lstrip("v")
    parts = t.split("-", 1)
    core = parts[0]
    pre  = parts[1] if len(parts) > 1 else None
    m = _SEMVER_CORE.match(core)
    if not m:
        return (0, 0, 0, -1 if pre else 0, pre or "")
    major, minor, patch = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    preflag = 0 if pre is None else -1
    return (major, minor, patch, preflag, pre or "")

def resolve_version(spec):
    """
    spec: {type: latest|exact|latest-in-minor, value?, minor?}
    returns (version_tag, source)
    """
    t = (spec or {}).get("type", "latest")
    if t == "exact":
        v = spec.get("value")
        if not v:
            raise ValueError("version.type=exact requires version.value")
        return v, "exact"

    if t == "latest":
        data = http_json(GITHUB_LATEST)
        tag = data.get("tag_name")
        if not tag:
            raise RuntimeError("Failed to resolve latest version from GitHub")
        return tag, "latest"

    if t == "latest-in-minor":
        minor = spec.get("minor")
        if not minor:
            raise ValueError("version.type=latest-in-minor requires version.minor (e.g., v1.11)")
        tags = http_json(GITHUB_TAGS)
        matching = [t["name"] for t in tags if t.get("name","").startswith(minor + ".")]
        if not matching:
            raise RuntimeError(f"No tags found for minor {minor}")
        best = sorted(matching, key=_semver_key, reverse=True)[0]
        return best, "latest-in-minor"

    raise ValueError(f"Unknown version.type: {t}")

# ---------------- schematics + asset URL ----------------

def post_schematic_and_get_id(customization: dict) -> str:
    """
    Sends a minimal schematic with 'customization' section to the Factory.
    Returns the schematic ID.
    """
    body = yaml.safe_dump({"customization": customization}, sort_keys=False)
    data = http_post_yaml_get_json(FACTORY_SCHEMATICS, body)
    if "id" not in data:
        raise RuntimeError(f"Factory did not return an ID: {data}")
    return data["id"]

def build_asset_url(schematic_id, version, platform, image_format, arch, secureboot=False):
    """
    Build a download URL for the requested artifact.
    Supported:
      platform=nocloud image_format=iso|raw.xz  (both support secureboot flag)
      platform=metal   image_format=iso
    """
    base = f"https://factory.talos.dev/image/{schematic_id}/{version}"

    if platform == "nocloud":
        if image_format == "iso":
            name = f"nocloud-{arch}{'-secureboot' if secureboot else ''}.iso"
        elif image_format in ("raw", "raw.xz", "rawxz"):
            # SecureBoot supported for raw.xz too.
            name = f"nocloud-{arch}{'-secureboot' if secureboot else ''}.raw.xz"
        else:
            raise ValueError("unsupported image_format for nocloud (use iso or raw.xz)")
        return f"{base}/{name}"

    if platform == "metal" and image_format == "iso":
        return f"{base}/metal-{arch}.iso"

    raise ValueError("unsupported platform/image_format combination")

# ---------------- decompression ----------------

def decompress_raw_xz(src_path: str, dst_path: str):
    """
    Decompress XZ to RAW using Python's lzma if available; otherwise
    try external 'xz -dkc'. Streamed to avoid RAM blowups.
    """
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    tmp = dst_path + ".part"

    if HAVE_LZMA:
        with lzma.open(src_path, "rb") as src, open(tmp, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        os.replace(tmp, dst_path)
        return

    rc, _, _ = run(["bash", "-lc", "xz --version >/dev/null 2>&1"])
    if rc != 0:
        raise RuntimeError("Cannot decompress: python lzma unavailable and 'xz' not installed")

    with open(tmp, "wb") as out:
        p = subprocess.Popen(["xz", "-dkc", src_path], stdout=out, stderr=subprocess.PIPE, text=True)
        _, stderr = p.communicate()
        if p.returncode != 0:
            try: os.remove(tmp)
            except FileNotFoundError: pass
            raise RuntimeError(f"xz failed: {stderr}")
    os.replace(tmp, dst_path)

# ---------------- push helpers ----------------

def ensure_tools_for_push():
    for tool in ("rsync", "ssh"):
        rc, _, _ = run(["bash", "-lc", f"command -v {tool}"])
        if rc != 0:
            return False, f"Missing dependency: {tool}"
    return True, ""

def push_file_to_hosts(local_file, hosts, default_iso_dir, rsync_opts, ssh_opts):
    results = []
    for h in hosts:
        host = h["host"]
        dest_dir = h.get("iso_dir", default_iso_dir)
        rc, out, err = run(["bash", "-lc", f"ssh {ssh_opts} {host} 'mkdir -p {shlex_quote(dest_dir)}'"])
        if rc != 0:
            results.append({"host": host, "status": "mkdir-failed", "stderr": err})
            continue
        rc, out, err = run(["bash", "-lc", f"rsync {rsync_opts} {shlex_quote(local_file)} {host}:{shlex_quote(dest_dir)}/"])
        results.append({"host": host, "status": ("ok" if rc == 0 else "rsync-failed"), "stderr": err})
    return results

# ---------------- cache management ----------------

_FILE_RE   = re.compile(r"^talos-(v[^-]+)-(.*)$")  # captures version, then artifact tail

def _parse_semver_for_sort(tag: str):
    t = tag.lstrip("v")
    parts = t.split("-", 1)
    core = parts[0]
    pre  = parts[1] if len(parts) > 1 else None
    m = _SEMVER_CORE.match(core)
    if not m:
        return (0, 0, 0, -1 if pre else 0, pre or "")
    major, minor, patch = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    preflag = 0 if pre is None else -1
    return (major, minor, patch, preflag, pre or "")

def plan_cache_cleanup(cache_dir: str, keep_versions: int):
    if keep_versions <= 0:
        return []
    try:
        entries = os.listdir(cache_dir)
    except FileNotFoundError:
        return []
    groups = {}
    for name in entries:
        if name.endswith(".sha256"):
            continue
        m = _FILE_RE.match(name)
        if not m:
            continue
        version_tag, tail = m.group(1), m.group(2)
        full = os.path.join(cache_dir, name)
        groups.setdefault(tail, []).append((version_tag, full))
    to_delete = []
    for tail, items in groups.items():
        items_sorted = sorted(items, key=lambda x: _parse_semver_for_sort(x[0]), reverse=True)
        victims = items_sorted[keep_versions:]
        for ver, victim in victims:
            to_delete.append(victim)
            sha = victim + ".sha256"
            if os.path.exists(sha):
                to_delete.append(sha)
    return to_delete

def execute_cache_cleanup(paths_to_delete):
    removed = []
    for p in paths_to_delete:
        try:
            if os.path.isdir(p) and not os.path.islink(p):
                shutil.rmtree(p, ignore_errors=True)
            else:
                os.remove(p)
            removed.append(p)
        except FileNotFoundError:
            pass
        except Exception as e:
            removed.append(f"ERROR:{p}:{e}")
    return removed

def plan_full_purge(cache_dir: str):
    try:
        entries = [os.path.join(cache_dir, e) for e in os.listdir(cache_dir)]
    except FileNotFoundError:
        return []
    return entries

# ---------------- manifest path resolution ----------------

def manifest_path_for_order(orderid: str, manifest_cfg: dict):
    tmpl = manifest_cfg.get("path_template")
    if tmpl:
        return tmpl.format(orderid=orderid)
    mdir = manifest_cfg.get("dir")
    if mdir:
        os.makedirs(mdir, exist_ok=True)
        return os.path.join(mdir, f"manifest-{orderid}.yaml")
    p = manifest_cfg.get("path")
    if p:
        if "{orderid}" in p:
            return p.replace("{orderid}", orderid)
        root, ext = os.path.splitext(p)
        return f"{root}-{orderid}{ext or '.yaml'}"
    return os.path.abspath(f"./manifest-{orderid}.yaml")

# ---------------- core processing ----------------

def process_positions(orderid, positions, defaults, dry_run):
    """
    Process a list of position items (Talos artifacts) for a given orderid.
    Returns list of entries for manifest.
    """
    cache_dir = defaults.get("cache_dir", "/var/cache/talos-sync")
    proxmox_default_iso_dir = defaults.get("proxmox_default_iso_dir", "/var/lib/vz/template/iso")
    os.makedirs(cache_dir, exist_ok=True)

    # push defaults
    push_defaults = defaults.get("push", {}) or {}
    push_enabled_default = bool(push_defaults.get("enabled", False))
    rsync_opts = push_defaults.get("rsync_opts", "-av --progress --inplace")
    ssh_opts   = push_defaults.get("ssh_opts", "-o BatchMode=yes")
    prefer_decompressed_default = bool(push_defaults.get("prefer_decompressed", False))

    # need push tools?
    need_push = any((it.get("push", {}).get("hosts") and (it.get("push", {}).get("enabled", push_enabled_default))) for it in positions)
    if need_push and not dry_run:
        ok, msg = ensure_tools_for_push()
        if not ok:
            raise RuntimeError(msg)

    entries = []

    for it in positions:
        pos_name = it.get("name") or "<unnamed>"
        try:
            if (it.get("product") or "").lower() != "talos":
                continue

            arch = it.get("arch", defaults.get("arch", "amd64"))
            version, version_source = resolve_version(it.get("version"))

            # schematic
            schematic_id = it.get("schematic_id")
            customization = it.get("customization")
            if not schematic_id:
                if not customization:
                    raise ValueError("missing schematic_id or customization")
                if dry_run:
                    schematic_id = "<to-be-created>"
                else:
                    schematic_id = post_schematic_and_get_id(customization)

            platform = it.get("platform", "nocloud")
            secureboot = bool(it.get("secureboot", False))
            image_format = it.get("image_format", "iso")
            url = build_asset_url(schematic_id, version, platform, image_format, arch, secureboot)

            basename = url.rstrip("/").split("/")[-1]
            local_path = os.path.join(cache_dir, f"talos-{version}-{basename}")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            entry = {
                "name": pos_name,
                "product": "talos",
                "platform": platform,
                "arch": arch,
                "version": version,
                "version_source": version_source,
                "secureboot": secureboot,
                "image_format": image_format,
                "schematic_id": schematic_id,
                "url": url,
                "cache_dir": cache_dir,
                "download": {"path": local_path, "done": False, "sha256": None, "size_bytes": None},
                "decompressed": {},
                "pushed": [],
                "features": it.get("customization", {}).get("systemExtensions", {}),
                "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }

            if dry_run:
                entries.append(entry); continue

            # download if needed
            if not (os.path.exists(local_path) and os.path.getsize(local_path) > 0):
                print(f"[{orderid}] Downloading {url} -> {local_path}")
                http_download(url, local_path)
                entry["download"]["done"] = True
            else:
                print(f"[{orderid}] Cached: {local_path}")

            # checksum + size
            entry["download"]["sha256"] = sha256_file(local_path)
            entry["download"]["size_bytes"] = os.path.getsize(local_path)
            with open(local_path + ".sha256", "w") as sf:
                sf.write(entry["download"]["sha256"] + "\n")

            # decompress if requested
            if (image_format in ("raw.xz", "rawxz")) and it.get("decompress_raw", False):
                decompressed_path = re.sub(r"\.xz$", "", local_path)  # preserve '-secureboot' in name
                print(f"[{orderid}] Decompressing {local_path} -> {decompressed_path}")
                decompress_raw_xz(local_path, decompressed_path)
                entry["decompressed"] = {
                    "path": decompressed_path,
                    "sha256": sha256_file(decompressed_path),
                    "size_bytes": os.path.getsize(decompressed_path),
                }
                with open(decompressed_path + ".sha256", "w") as sf:
                    sf.write(entry["decompressed"]["sha256"] + "\n")

            # push?
            push_cfg = it.get("push", {}) or {}
            push_enabled = push_cfg.get("enabled", push_enabled_default)
            hosts = push_cfg.get("hosts", [])
            prefer_decompressed = push_cfg.get("prefer_decompressed", prefer_decompressed_default)
            if push_enabled and hosts:
                decomp_path = (entry.get("decompressed") or {}).get("path")
                artifact_to_push = decomp_path if (prefer_decompressed and decomp_path) else entry["download"]["path"]
                res = push_file_to_hosts(artifact_to_push, hosts, proxmox_default_iso_dir, rsync_opts, ssh_opts)
                entry["pushed"] = res

            entries.append(entry)

        except Exception as e:
            # contextual error entry
            err_entry = {
                "name": pos_name,
                "status": "error",
                "error": f"position failed: {e.__class__.__name__}: {e}",
            }
            entries.append(err_entry)
            print(f"[{orderid}] ERROR in position '{pos_name}': {e}", file=sys.stderr)

    return entries

# ---------------- main ----------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Process Talos orders (per-order manifests).")
    parser.add_argument("--config", required=True, help="Path to config.yaml (defaults + manifest)")
    parser.add_argument("--orders", required=True, help="Path to orders.yaml (list of orders)")
    parser.add_argument("--dry-run", action="store_true", help="Plan only (no download/push/decompress)")
    parser.add_argument("--purge-cache", action="store_true", help="Purge entire cache before processing")
    args = parser.parse_args()

    # Load config
    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f) or {}
    defaults = cfg.get("defaults", {}) or {}
    manifest_cfg = cfg.get("manifest", {}) or {}

    # Load orders: list of {orderid, customer, positions: [...]}
    with open(args.orders, "r") as f:
        orders_doc = yaml.safe_load(f)
    if isinstance(orders_doc, dict) and "orders" in orders_doc:
        orders_list = orders_doc["orders"]
    elif isinstance(orders_doc, list):
        orders_list = orders_doc
    else:
        raise ValueError("orders.yaml must be a list of orders or a dict with key 'orders'")

    cache_dir = defaults.get("cache_dir", "/var/cache/talos-sync")
    os.makedirs(cache_dir, exist_ok=True)

    # Cache policy (global)
    cache_policy = defaults.get("cache_policy", {}) or {}
    cache_enabled = bool(cache_policy.get("enabled", True))
    keep_versions = int(cache_policy.get("keep_versions", 3))
    purge_before  = bool(cache_policy.get("purge_before", False))
    if args.purge_cache:
        purge_before = True

    # Optional full purge before processing
    if purge_before:
        purge_list = plan_full_purge(cache_dir)
        if args.dry_run:
            if purge_list:
                print("[DRY-RUN] Full cache purge plan (would remove):")
                for p in purge_list: print("  -", p)
        else:
            if purge_list:
                print("Purging cache directory contents...")
                removed = execute_cache_cleanup(purge_list)
                for r in removed: print("  removed:", r)

    # Process each order and write a per-order manifest
    for order in orders_list:
        orderid = str(order.get("orderid", "")).strip()
        customer = order.get("customer", "")
        positions = order.get("positions", []) or []

        if not orderid:
            raise ValueError("Each order must have a non-empty 'orderid'")

        try:
            entries = process_positions(orderid, positions, defaults, args.dry_run)
        except Exception as e:
            # Ensure we still emit a manifest for traceability
            entries = [{"status": "error", "error": f"order failed: {e.__class__.__name__}: {e}"}]
            print(f"[{orderid}] ORDER ERROR: {e}", file=sys.stderr)

        manifest_path = manifest_path_for_order(orderid, manifest_cfg)
        manifest_doc = {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "order": {
                "orderid": orderid,
                "customer": customer,
                "positions_count": len(positions),
            },
            "source_config": os.path.abspath(args.config),
            "source_orders": os.path.abspath(args.orders),
            "cache_policy": {
                "enabled": cache_enabled,
                "keep_versions": keep_versions,
                "purge_before": purge_before,
            },
            "entries": entries,
        }

        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
        if args.dry_run:
            print(f"\n[DRY-RUN] Manifest for order {orderid} -> {manifest_path}")
            yaml.safe_dump(manifest_doc, sys.stdout, sort_keys=False)
        else:
            with open(manifest_path, "w") as mf:
                yaml.safe_dump(manifest_doc, mf, sort_keys=False)
            print(f"\nManifest written for order {orderid}: {manifest_path}")

    # Apply cache retention once at the end (global)
    if cache_enabled:
        cleanup_plan = plan_cache_cleanup(cache_dir, keep_versions)
        if args.dry_run:
            if cleanup_plan:
                print("\n[DRY-RUN] Cache cleanup plan (would remove):")
                for p in cleanup_plan: print("  -", p)
        else:
            if cleanup_plan:
                print("\nCache cleanup: removing old artifacts...")
                removed = execute_cache_cleanup(cleanup_plan)
                for r in removed: print("  removed:", r)

if __name__ == "__main__":
    main()
