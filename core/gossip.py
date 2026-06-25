"""Signed gossip.

Every meaningful inter-node assertion (peer announcements, shard
attestations, votes, moderation flags) is signed by the asserting node's
Ed25519 key. Receivers verify before trusting. This is what lets the
network reason about *who* said something — without it, voting and
trust are meaningless because any peer could forge any claim.

Canonicalization: we sign over a deterministic JSON encoding (sorted keys,
no whitespace) of the payload plus a type tag and timestamp, so the same
logical message always produces the same bytes on every node.
"""
from __future__ import annotations

import json
import time

from nacl import signing, encoding, exceptions as nacl_exc


def canonical_bytes(msg_type: str, payload: dict, ts: int) -> bytes:
    envelope = {"t": msg_type, "ts": ts, "p": payload}
    return json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()


def sign_message(signing_key_hex: str, msg_type: str, payload: dict,
                 ts: int | None = None) -> dict:
    """Produce a signed envelope ready to send to a peer."""
    ts = ts if ts is not None else int(time.time())
    sk = signing.SigningKey(signing_key_hex, encoder=encoding.HexEncoder)
    body = canonical_bytes(msg_type, payload, ts)
    sig = sk.sign(body).signature.hex()
    return {
        "t": msg_type,
        "ts": ts,
        "p": payload,
        "node_id": None,        # filled in by caller (config.NODE_ID)
        "sig": sig,
        "verify_key": sk.verify_key.encode(encoder=encoding.HexEncoder).decode(),
    }


def verify_message(envelope: dict, expected_verify_key: str | None = None,
                   max_age_s: int = 3600) -> tuple[bool, str]:
    """Verify a signed envelope.

    If expected_verify_key is given (because we already know this node's key),
    we require it to match — this prevents a peer swapping in a fresh key to
    impersonate an established identity. Returns (ok, reason).
    """
    try:
        msg_type = envelope["t"]
        ts = int(envelope["ts"])
        payload = envelope["p"]
        sig = bytes.fromhex(envelope["sig"])
        vk_hex = envelope["verify_key"]
    except (KeyError, ValueError, TypeError):
        return False, "malformed"

    if expected_verify_key and vk_hex != expected_verify_key:
        return False, "verify_key_mismatch"

    age = abs(int(time.time()) - ts)
    if age > max_age_s:
        return False, "stale_or_future"

    try:
        vk = signing.VerifyKey(vk_hex, encoder=encoding.HexEncoder)
        vk.verify(canonical_bytes(msg_type, payload, ts), sig)
    except (nacl_exc.BadSignatureError, ValueError):
        return False, "bad_signature"
    return True, "ok"


def derive_node_id(verify_key_hex: str) -> str:
    """Node identity is bound to its key: node_id = first 16 bytes of the
    verify key hex. This means a node cannot change keys without becoming a
    new identity (and losing its accrued trust/age) — exactly what we want."""
    return verify_key_hex[:32]
