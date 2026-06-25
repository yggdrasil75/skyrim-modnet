"""User identity.

A "user key" is an Ed25519 keypair the user generates client-side. The
network only ever sees the public key. To act as a mod's author you sign a
challenge with your private key. Because the key is portable, an author can
move to a new machine and keep updating their mods.

Anti-theft / "key used on another system" protection:
  When a key authenticates from a machine fingerprint different from the
  recorded `current_holder`, the node flags it. The legitimate owner can
  rotate to a fresh key (publishing a signed "successor" record) which is
  the recommended response if they suspect compromise.
"""
from __future__ import annotations

import time
import secrets

from flask import Blueprint, request, jsonify, current_app
from nacl import signing, encoding, exceptions as nacl_exc

from .db import get_db

bp = Blueprint("identity", __name__, url_prefix="/api/identity")

# Short-lived challenges keyed by pubkey, kept in-memory per node.
_CHALLENGES: dict[str, tuple[str, float]] = {}
_CHALLENGE_TTL = 120


def verify_signature(pubkey_hex: str, message: bytes, sig_hex: str) -> bool:
    try:
        vk = signing.VerifyKey(pubkey_hex, encoder=encoding.HexEncoder)
        vk.verify(message, bytes.fromhex(sig_hex))
        return True
    except (nacl_exc.BadSignatureError, ValueError):
        return False


@bp.route("/register", methods=["POST"])
def register():
    """Record a pubkey. Idempotent; proves control by signing the pubkey."""
    data = request.get_json(force=True)
    pubkey = data["pubkey"]
    sig = data["sig"]                # signature over the pubkey string
    display = (data.get("display_name") or "").strip()[:64]
    holder = data.get("machine_fingerprint", "")[:128]

    if not verify_signature(pubkey, pubkey.encode(), sig):
        return jsonify(error="bad signature"), 400

    db = get_db()
    now = int(time.time())
    row = db.execute("SELECT pubkey FROM users WHERE pubkey=?", (pubkey,)).fetchone()
    if row is None:
        db.execute(
            "INSERT INTO users(pubkey, display_name, created_at, current_holder, holder_claimed_at) "
            "VALUES(?,?,?,?,?)",
            (pubkey, display, now, holder, now),
        )
    db.commit()
    return jsonify(ok=True, pubkey=pubkey)


@bp.route("/challenge", methods=["POST"])
def challenge():
    """Issue a nonce the user must sign to perform an authoring action."""
    pubkey = request.get_json(force=True)["pubkey"]
    nonce = secrets.token_hex(24)
    _CHALLENGES[pubkey] = (nonce, time.time())
    return jsonify(nonce=nonce, ttl=_CHALLENGE_TTL)


def consume_auth(pubkey: str, sig_hex: str, machine_fp: str = "") -> tuple[bool, str]:
    """Verify a signed challenge. Returns (ok, warning)."""
    entry = _CHALLENGES.pop(pubkey, None)
    if not entry:
        return False, "no_challenge"
    nonce, issued = entry
    if time.time() - issued > _CHALLENGE_TTL:
        return False, "expired"
    if not verify_signature(pubkey, nonce.encode(), sig_hex):
        return False, "bad_sig"

    warning = ""
    db = get_db()
    row = db.execute(
        "SELECT current_holder FROM users WHERE pubkey=?", (pubkey,)
    ).fetchone()
    if row and machine_fp and row["current_holder"] and row["current_holder"] != machine_fp:
        # Key authenticated from a different machine than last claimed.
        warning = "key_used_on_new_system_consider_rotating"
        db.execute(
            "UPDATE users SET current_holder=?, holder_claimed_at=? WHERE pubkey=?",
            (machine_fp, int(time.time()), pubkey),
        )
        db.commit()
    return True, warning
