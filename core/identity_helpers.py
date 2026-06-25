"""Thin bridge so route modules don't import each other circularly."""
from __future__ import annotations

from .routes_identity import consume_auth


def require_author(pubkey: str, sig: str, machine_fp: str = "") -> tuple[bool, str]:
    return consume_auth(pubkey, sig, machine_fp)
