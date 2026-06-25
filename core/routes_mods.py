"""Mod lifecycle endpoints.

Authoring rules:
  - Only the author_pubkey may edit a mod's content/metadata. Enforced by a
    signed challenge (see routes_identity.consume_auth).
  - A small set of "pointers" (category, deprecated flag) are governable —
    here left as author-only by default, but the hook is where community
    governance / federated flags would plug in.
"""
from __future__ import annotations

import io
import os
import time
import json
import shutil
import secrets
import tempfile
import zipfile

from flask import Blueprint, request, jsonify, current_app, send_file

from .db import get_db
from .identity_helpers import require_author
from .packaging import scan_tree, pack_folder

bp = Blueprint("mods", __name__, url_prefix="/api/mods")


@bp.route("", methods=["GET"])
def list_mods():
    db = get_db()
    rows = db.execute(
        "SELECT id,name,author_pubkey,description,version,category,deprecated,updated_at "
        "FROM mods ORDER BY updated_at DESC LIMIT 200"
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@bp.route("/<mod_id>", methods=["GET"])
def get_mod(mod_id):
    db = get_db()
    mod = db.execute("SELECT * FROM mods WHERE id=?", (mod_id,)).fetchone()
    if not mod:
        return jsonify(error="not found"), 404
    shards = db.execute(
        "SELECT id,seq,size_bytes,have_local FROM shards WHERE mod_id=? ORDER BY seq",
        (mod_id,),
    ).fetchall()
    return jsonify(mod=dict(mod), shards=[dict(s) for s in shards])


@bp.route("/publish", methods=["POST"])
def publish():
    """Publish a new mod.

    Multipart form:
      pubkey, sig (signed challenge), machine_fingerprint
      meta (json: name, description, version, category)
      bundle (a .zip of the mod folder)
    """
    pubkey = request.form["pubkey"]
    ok, warn = require_author(pubkey, request.form["sig"],
                             request.form.get("machine_fingerprint", ""))
    if not ok:
        return jsonify(error="auth_failed", reason=warn), 403

    meta = json.loads(request.form["meta"])
    bundle = request.files["bundle"]

    workdir = tempfile.mkdtemp(prefix="distmod_pub_")
    try:
        extract = os.path.join(workdir, "tree")
        os.makedirs(extract)
        with zipfile.ZipFile(io.BytesIO(bundle.read())) as zf:
            _safe_extract(zf, extract)

        violations = scan_tree(extract)
        if violations:
            return jsonify(error="executable_content_rejected",
                           violations=violations[:50]), 422

        mod_id = secrets.token_hex(16)
        blob_out = os.path.join(current_app.config["BLOB_DIR"], mod_id)
        shards = pack_folder(extract, blob_out,
                             current_app.config["MAX_BLOB_SHARD_BYTES"])

        db = get_db()
        now = int(time.time())
        manifest = {"mod_id": mod_id, "shards": [s["id"] for s in shards],
                    "name": meta["name"], "version": meta.get("version", "1")}
        db.execute(
            "INSERT INTO mods(id,name,author_pubkey,description,version,created_at,"
            "updated_at,category,deprecated,manifest_sig) VALUES(?,?,?,?,?,?,?,?,0,?)",
            (mod_id, meta["name"][:128], pubkey, meta.get("description", "")[:2000],
             meta.get("version", "1"), now, now, meta.get("category", "misc"),
             request.form.get("manifest_sig", "")),
        )
        for s in shards:
            db.execute(
                "INSERT INTO shards(id,mod_id,seq,size_bytes,have_local,created_at) "
                "VALUES(?,?,?,?,1,?)",
                (s["id"], mod_id, s["seq"], s["size_bytes"], now),
            )
            db.execute(
                "INSERT OR REPLACE INTO shard_locations(shard_id,node_id,last_seen) "
                "VALUES(?,?,?)", (s["id"], current_app.config["NODE_ID"], now),
            )
        db.commit()
        return jsonify(ok=True, mod_id=mod_id, shards=len(shards),
                       manifest=manifest, warning=warn)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


@bp.route("/<mod_id>/edit", methods=["POST"])
def edit_mod(mod_id):
    db = get_db()
    mod = db.execute("SELECT author_pubkey FROM mods WHERE id=?", (mod_id,)).fetchone()
    if not mod:
        return jsonify(error="not found"), 404
    pubkey = request.form["pubkey"]
    if pubkey != mod["author_pubkey"]:
        return jsonify(error="only_author_may_edit"), 403
    ok, warn = require_author(pubkey, request.form["sig"],
                             request.form.get("machine_fingerprint", ""))
    if not ok:
        return jsonify(error="auth_failed", reason=warn), 403

    meta = json.loads(request.form.get("meta", "{}"))
    fields, vals = [], []
    for k in ("name", "description", "version", "category", "deprecated"):
        if k in meta:
            fields.append(f"{k}=?")
            vals.append(meta[k])
    if fields:
        vals.extend([int(time.time()), mod_id])
        db.execute(f"UPDATE mods SET {','.join(fields)}, updated_at=? WHERE id=?", vals)
        db.commit()
    return jsonify(ok=True, warning=warn)


@bp.route("/shard/<shard_id>", methods=["GET"])
def get_shard(shard_id):
    """Serve raw shard bytes to a peer or client (content-addressed)."""
    db = get_db()
    row = db.execute(
        "SELECT mod_id FROM shards WHERE id=? AND have_local=1", (shard_id,)
    ).fetchone()
    if not row:
        return jsonify(error="not_held_here"), 404
    path = os.path.join(current_app.config["BLOB_DIR"], row["mod_id"], shard_id)
    if not os.path.exists(path):
        return jsonify(error="missing_bytes"), 410
    return send_file(path, mimetype="application/octet-stream",
                     as_attachment=True, download_name=shard_id)


def _safe_extract(zf: zipfile.ZipFile, dest: str) -> None:
    """Prevent zip-slip path traversal."""
    dest = os.path.realpath(dest)
    for member in zf.namelist():
        target = os.path.realpath(os.path.join(dest, member))
        if not target.startswith(dest + os.sep) and target != dest:
            raise ValueError(f"unsafe path in archive: {member}")
    zf.extractall(dest)
