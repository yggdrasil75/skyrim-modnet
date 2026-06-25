"""Reputation — data, never enforcement.

Design principle (explicit, by request): NO accumulation of bad votes ever
auto-bans a node. Not at 100, not at 10^7. Bad votes are *reputation data*
that is replicated (via gateways + trusted nodes) and surfaced to users, who
alone decide whom to mark `enemy`. This is the anti-influence guarantee: the
network never takes the ban action, so no faction can weaponize it.

Two things make raw bad-vote counts misleading, so we never expose them alone:

  1. Scale. A node storing 100 TB across millions of shards will rack up far
     more failed-transfer events than a node holding 20 files, purely by
     volume — even if it's perfectly honest. We therefore compute a
     storage-normalized failure RATE and a Wilson lower bound so small
     samples don't over-condemn.

  2. Honest flakiness. A trusted, long-lived node can have a genuinely failed
     transfer. Good votes (successful replications of known-good files) are
     tracked alongside bad ones so its ratio stays healthy.

The file-security path is separate: if >= SECURITY_FRACTION of the *weighted
security nodes that actually scanned a file* call it malicious, the file gets
a user-facing WARNING — never an automatic removal.
"""
from __future__ import annotations

import math

from .trust import vote_weight, STATE_TRUSTED, STATE_GATEWAY

SECURITY_FRACTION = 0.05      # 5% of weighted scanning security nodes


def wilson_lower_bound(good: int, bad: int, z: float = 1.96) -> float:
    """Lower bound of the 'good' proportion at ~95% confidence.

    With few samples this stays conservative (won't crown a node 'great' off
    one success, won't condemn off one failure). Returns 0..1 where higher is
    better-behaved.
    """
    n = good + bad
    if n == 0:
        return 0.0
    phat = good / n
    denom = 1 + z * z / n
    centre = phat + z * z / (2 * n)
    margin = z * math.sqrt((phat * (1 - phat) + z * z / (4 * n)) / n)
    return max(0.0, (centre - margin) / denom)


def storage_expectation_factor(stored_files: int, stored_bytes: float) -> float:
    """How many failures we should *expect* from sheer volume, as a divisor.

    A node holding more data is allowed proportionally more raw failures before
    its rate looks bad. We use a gentle log scale so it doesn't fully cancel
    out genuine misbehavior. Returns >= 1.0.
    """
    files = max(stored_files, 0)
    # ~1.0 for tiny nodes, growing slowly: 100 files ~2x tolerance,
    # 1e6 files ~7x. Never lets a node hide unlimited failures.
    return 1.0 + math.log10(1 + files) * 1.0


def node_reputation(peer: dict) -> dict:
    """Compute a surfaced reputation summary for one node. No ban decision.

    `peer` row needs: good_votes, bad_votes, stored_files, stored_bytes,
    node_state, first_seen, failures.
    """
    good = peer.get("good_votes", 0) or 0
    bad = peer.get("bad_votes", 0) or 0
    tol = storage_expectation_factor(peer.get("stored_files", 0) or 0,
                                     peer.get("stored_bytes", 0) or 0)
    # storage-adjusted bad count: forgive failures proportional to volume
    adj_bad = bad / tol
    health = wilson_lower_bound(good, int(round(adj_bad)))
    total = good + bad
    raw_rate = (bad / total) if total else 0.0
    return {
        "node_id": peer.get("node_id"),
        "good_votes": good,
        "bad_votes": bad,
        "stored_files": peer.get("stored_files", 0) or 0,
        "raw_bad_rate": round(raw_rate, 4),
        "storage_adjusted_bad": round(adj_bad, 2),
        "health_score": round(health, 4),   # 0..1, higher = better behaved
        "node_state": peer.get("node_state"),
        # advisory only — the UI uses this to *suggest*, the user decides.
        "suggest_enemy": health < 0.5 and total >= 20,
    }


def list_suspect_nodes(db, min_samples: int = 20,
                       health_ceiling: float = 0.85) -> list[dict]:
    """Build the advisory 'bad node list' users can browse / subscribe to.

    Ranked worst-first by health score. Only nodes that are BOTH past
    `min_samples` (enough history to judge) AND below `health_ceiling` (look
    genuinely troubled) are included — a long-lived node with one flaky
    transfer shouldn't appear on a 'bad node' list. Still purely informational:
    each consumer chooses which of these to actually mark `enemy`.
    """
    rows = db.execute(
        "SELECT node_id, good_votes, bad_votes, stored_files, stored_bytes, "
        "node_state, first_seen, failures, relation FROM peers"
    ).fetchall()
    out = []
    for r in rows:
        rep = node_reputation(dict(r))
        samples = rep["good_votes"] + rep["bad_votes"]
        if samples >= min_samples and rep["health_score"] < health_ceiling:
            rep["already_enemy"] = (r["relation"] == "enemy")
            out.append(rep)
    out.sort(key=lambda x: x["health_score"])
    return out


def file_security_status(db, mod_id: str) -> dict:
    """Apply the 5% weighted-security-node warning threshold for a file.

    We count only nodes that actually submitted a security verdict for this
    mod, weighted by trust. If the malicious weight is >= SECURITY_FRACTION of
    the total verdict weight, we surface a WARNING. Never a removal.
    """
    rows = db.execute(
        "SELECT v.reporter_id, v.verdict, p.node_state, p.first_seen, "
        "p.relation FROM file_security_votes v "
        "LEFT JOIN peers p ON p.node_id = v.reporter_id WHERE v.mod_id=?",
        (mod_id,),
    ).fetchall()
    if not rows:
        return {"mod_id": mod_id, "warning": False, "malicious_fraction": 0.0,
                "scanners": 0}

    mal_w = 0.0
    total_w = 0.0
    scanners = 0
    for r in rows:
        peer = {"node_state": r["node_state"] or "newcomer",
                "first_seen": r["first_seen"], "relation": r["relation"] or "neutral"}
        w = vote_weight(peer)
        # security scanning is a content judgement; gateways (weight 0) don't
        # scan, so they naturally drop out here.
        if w <= 0:
            continue
        total_w += w
        scanners += 1
        if r["verdict"] == "malicious":
            mal_w += w

    frac = (mal_w / total_w) if total_w else 0.0
    return {
        "mod_id": mod_id,
        "warning": frac >= SECURITY_FRACTION,
        "malicious_fraction": round(frac, 4),
        "threshold": SECURITY_FRACTION,
        "scanners": scanners,
    }
