"""Content packaging and safety scanning.

Upload flow:
  1. User uploads a *folder* (sent as a zip of the tree, or multiple files).
  2. We reject anything that looks like a binary/executable BEFORE storing.
  3. We re-pack the tree with 7z at max compression and split into shards
     of <= MAX_BLOB_SHARD_BYTES so the network only ever moves small pieces.
  4. Each shard is content-addressed by sha256.

The executable scan is deliberately defense-in-depth: it runs at upload AND
can run later as a randomized network sweep (see scheduler.py), because a
malicious node could inject content that bypassed someone else's upload path.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import tempfile

# Magic-number signatures of common executable / loadable binary formats.
_EXEC_MAGIC = {
    b"\x7fELF": "ELF",
    b"MZ": "PE/DOS",
    b"\xca\xfe\xba\xbe": "Mach-O fat",
    b"\xcf\xfa\xed\xfe": "Mach-O 64",
    b"\xfe\xed\xfa\xce": "Mach-O",
    b"\xfe\xed\xfa\xcf": "Mach-O 64 be",
    b"dex\n": "Android DEX",
    b"\x50\x4b\x03\x04PK": "nested-archive-suspect",  # heuristic only
}

# Extensions we refuse outright regardless of contents.
_BANNED_EXT = {
    ".exe", ".dll", ".so", ".dylib", ".bin", ".bat", ".cmd", ".sh",
    ".msi", ".apk", ".jar", ".com", ".scr", ".elf", ".o", ".a",
}


def scan_file_for_executable(path: str) -> str | None:
    """Return a reason string if the file appears executable, else None."""
    ext = os.path.splitext(path)[1].lower()
    if ext in _BANNED_EXT:
        return f"banned_extension:{ext}"
    try:
        with open(path, "rb") as fh:
            head = fh.read(16)
    except OSError:
        return None
    for magic, name in _EXEC_MAGIC.items():
        if head.startswith(magic):
            return f"executable_magic:{name}"
    # POSIX exec bit set on a regular file is also suspicious for "data only".
    if os.path.isfile(path) and os.access(path, os.X_OK):
        return "exec_bit_set"
    return None


def scan_tree(root: str) -> list[str]:
    """Walk a directory, returning a list of violations (empty == clean)."""
    violations = []
    for dirpath, _dirs, files in os.walk(root):
        for fn in files:
            full = os.path.join(dirpath, fn)
            reason = scan_file_for_executable(full)
            if reason:
                rel = os.path.relpath(full, root)
                violations.append(f"{rel}: {reason}")
    return violations


def _have_7z() -> str | None:
    for cand in ("7z", "7za", "7zr"):
        if shutil.which(cand):
            return cand
    return None


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def pack_folder(src_dir: str, out_dir: str, shard_bytes: int) -> list[dict]:
    """Compress src_dir with 7z (max) split into shards. Returns shard meta.

    Falls back to a pure-Python tar+split if the 7z binary is unavailable,
    so the node still functions in minimal environments (less compression).
    """
    os.makedirs(out_dir, exist_ok=True)
    archive_base = os.path.join(out_dir, "content.7z")
    sevenzip = _have_7z()

    if sevenzip:
        # -mx=9 max compression, -v split volume size.
        cmd = [
            sevenzip, "a", "-mx=9", f"-v{shard_bytes}b",
            "-bso0", "-bsp0", archive_base, ".",
        ]
        subprocess.run(cmd, cwd=src_dir, check=True)
        produced = sorted(
            os.path.join(out_dir, f)
            for f in os.listdir(out_dir)
            if f.startswith("content.7z.")
        )
    else:
        produced = _tar_split_fallback(src_dir, out_dir, shard_bytes)

    shards = []
    for seq, path in enumerate(produced):
        digest = sha256_file(path)
        final = os.path.join(out_dir, digest)
        os.rename(path, final)
        shards.append({"id": digest, "seq": seq, "size_bytes": os.path.getsize(final)})
    return shards


def _tar_split_fallback(src_dir: str, out_dir: str, shard_bytes: int) -> list[str]:
    import tarfile

    tmp = tempfile.NamedTemporaryFile(delete=False, dir=out_dir, suffix=".tar.gz")
    tmp.close()
    with tarfile.open(tmp.name, "w:gz") as tf:
        tf.add(src_dir, arcname=".")
    parts = []
    with open(tmp.name, "rb") as fh:
        idx = 0
        while True:
            chunk = fh.read(shard_bytes)
            if not chunk:
                break
            p = os.path.join(out_dir, f"part.{idx:04d}")
            with open(p, "wb") as out:
                out.write(chunk)
            parts.append(p)
            idx += 1
    os.unlink(tmp.name)
    return parts
