# talos_order.py

A utility to automate **Talos Linux image creation, caching, and distribution** for Proxmox and other platforms.  
It fetches artifacts from the [Talos Image Factory](https://factory.talos.dev/), manages local cache retention, decompresses raw images, and optionally pushes them to Proxmox nodes.

## ✨ Features

- **Configurable per-order manifests**  
  - `--config` → defaults and manifest settings  
  - `--orders` → list of orders  
  - per-order manifests saved to disk  

- **Artifact management**  
  - Talos Image Factory schematic creation from `customization`  
  - Platform support:
    - `nocloud` (`iso` / `raw.xz`)  
    - `metal` (`iso`)  
  - SecureBoot supported for both `iso` and `raw.xz`  
  - Automatic `raw.xz → .raw` decompression (Python `lzma` or external `xz`)  

- **Cache management**  
  - Retains the latest *N* artifacts per family  
  - Optional full purge before processing  
  - `.sha256` checksums and size metadata  

- **Proxmox integration**  
  - `rsync` push to Proxmox nodes  
  - Configurable default ISO directory and per-host overrides  

- **Version resolution**  
  - `exact` → pin to a version  
  - `latest` → latest release from GitHub  
  - `latest-in-minor` → latest GA in a given minor (prefers GA over prerelease)  

- **Dry-run mode**  
  - Plan-only execution without downloading/pushing/decompressing  
  - Shows manifests and cache cleanup plan  

---

## 🚀 Usage

```bash
# Dry-run (plan only)
./talos_order.py --config config.yaml --orders orders.yaml --dry-run

# Normal run (download, cache, optional push)
sudo ./talos_order.py --config config.yaml --orders orders.yaml

# Full cache purge before run
sudo ./talos_order.py --config config.yaml --orders orders.yaml --purge-cache
```

---

## ⚙️ Configuration

### `config.yaml`
Holds defaults and manifest settings. Example:

```yaml
defaults:
  cache_dir: /var/cache/talos-sync
  proxmox_default_iso_dir: /var/lib/vz/template/iso
  arch: amd64
  push:
    enabled: true
    rsync_opts: "-av --progress --inplace"
    ssh_opts: "-o BatchMode=yes"
    prefer_decompressed: true
  cache_policy:
    enabled: true
    keep_versions: 3
    purge_before: false

manifest:
  dir: ./manifests
```

### `orders.yaml`
List of orders to process. Example:

```yaml
orders:
  - orderid: "order123"
    customer: "ExampleCorp"
    positions:
      - name: "Talos ISO SecureBoot"
        product: "talos"
        platform: "nocloud"
        image_format: "iso"
        arch: amd64
        secureboot: true
        version:
          type: latest-in-minor
          minor: v1.7
        customization:
          systemExtensions:
            - siderolabs/hello-world
        push:
          enabled: true
          hosts:
            - host: proxmox1.example.com
            - host: proxmox2.example.com
```

---

## 📂 Outputs

For each order, a manifest is written to the configured path (e.g. `manifests/manifest-order123.yaml`).  
The manifest includes:

- Resolved versions and sources (`exact`, `latest`, `latest-in-minor`)  
- Download status, size, checksum  
- Decompressed artifacts (if enabled)  
- Push results per host  
- Cache policy in effect  

---

## 🔧 Dependencies

- Python 3.7+  
- [PyYAML](https://pypi.org/project/PyYAML/) → `pip install pyyaml`  
- Optional:
  - `xz` (if Python’s `lzma` module is unavailable)  
  - `ssh` and `rsync` (for pushing artifacts to Proxmox nodes)  

---

## 🛡️ Example Workflow

1. Define defaults and manifest settings in `config.yaml`.  
2. Describe orders and positions in `orders.yaml`.  
3. Run in dry-run mode to preview what will happen:  

   ```bash
   ./talos_order.py --config config.yaml --orders orders.yaml --dry-run
   ```

4. Run normally to download artifacts, update cache, and push to hosts.  
5. Inspect generated per-order manifests under `./manifests/`.  

---

## 📝 Notes

- Per-order manifests are always written (even on error) for traceability.  
- Cache cleanup runs globally after all orders are processed.  
- Designed for reproducible, automated image handling in Proxmox + Talos setups.  

---

## 📜 License

MIT License – see [LICENSE](LICENSE) for details.  
